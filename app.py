from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse
import os
import hashlib
import africastalking
from groq import Groq

app = FastAPI()

# ---------- Initialize Africa's Talking SDK ----------
AT_USERNAME = os.getenv("AT_USERNAME")   # "sandbox" or your live username
AT_API_KEY  = os.getenv("AT_API_KEY")

africastalking.initialize(username=AT_USERNAME, api_key=AT_API_KEY)
sms_service = africastalking.SMS

# ---------- Initialize Groq Client ----------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client  = Groq(api_key=GROQ_API_KEY)

# Shortcode — change to your live shortcode when going live
SENDER_ID = os.getenv("AT_SENDER_ID", "98449")

# ---------- ROOT ROUTE FOR HEALTH CHECK ----------
@app.get("/")
def root():
    return {"status": "EduTena SMS API is running"}

# ---------- DATABASE CONNECTION ----------
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    url = urlparse(DATABASE_URL)
    return psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )

def init_db():
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            phone_hash  TEXT PRIMARY KEY,
            level       TEXT,
            math        INTEGER,
            science     INTEGER,
            social      INTEGER,
            creative    INTEGER,
            technical   INTEGER,
            pathway     TEXT,
            risk        TEXT,
            state       TEXT,
            lang        TEXT DEFAULT 'en'
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

# ---------- PRIVACY: Hash phone number ----------
def hash_phone(phone: str) -> str:
    """SHA-256 hash — raw phone number is never stored in the database."""
    return hashlib.sha256(phone.strip().encode()).hexdigest()

# ---------- DB HELPERS ----------
ALLOWED_FIELDS = {
    "level", "math", "science", "social", "creative",
    "technical", "pathway", "risk", "state", "lang"
}

def save_student(phone_hash: str, field: str, value):
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO students(phone_hash) VALUES(%s) ON CONFLICT DO NOTHING",
        (phone_hash,)
    )
    cur.execute(
        f"UPDATE students SET {field}=%s WHERE phone_hash=%s",
        (value, phone_hash)
    )
    conn.commit()
    cur.close()
    conn.close()

def get_student(phone_hash: str):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM students WHERE phone_hash=%s", (phone_hash,))
    student = cur.fetchone()
    cur.close()
    conn.close()
    return student
    # columns index:
    # 0:phone_hash 1:level 2:math 3:science 4:social
    # 5:creative 6:technical 7:pathway 8:risk 9:state 10:lang

# ---------- PATHWAY CALCULATION ----------
def calculate_pathway(phone_hash: str) -> str:
    student = get_student(phone_hash)
    if not student:
        return None
    _, _, math, science, social, creative, technical, _, _, _, _ = student

    stem         = (math or 0) + (science or 0) + (technical or 0)
    social_score = (social or 0) * 2
    arts_score   = (creative or 0) * 2

    if stem >= social_score and stem >= arts_score:
        pathway = "STEM"
    elif social_score >= stem and social_score >= arts_score:
        pathway = "Social Sciences"
    else:
        pathway = "Arts & Sports Science"

    save_student(phone_hash, "pathway", pathway)
    return pathway

# ---------- RISK CALCULATION ----------
def calculate_risk(phone_hash: str) -> str:
    student = get_student(phone_hash)
    if not student:
        return "Unknown"
    _, _, math, science, social, creative, technical, _, _, _, _ = student

    scores = [math or 0, science or 0, social or 0, creative or 0, technical or 0]
    avg    = sum(scores) / len(scores)

    if avg >= 2.5:
        risk = "Low"
    elif avg >= 1.5:
        risk = "Medium"
    else:
        risk = "High"

    save_student(phone_hash, "risk", risk)
    return risk

# ---------- SMS FORMATTER ----------
def sms_format(text: str, limit: int = 459) -> str:
    """Trim AI response to fit within 3 SMS parts (459 chars max)."""
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit - 3] + "..."

# ---------- GROQ AI FUNCTIONS ----------

def ai_career_guidance(pathway: str, level: str, lang: str = "en") -> str:
    lang_note = "Respond in Kiswahili." if lang == "sw" else "Respond in English."
    prompt = (
        f"A Kenyan {level} student is placed in the {pathway} pathway under CBC. "
        f"Suggest 3 specific career options with a one-line description each. "
        f"Be encouraging, practical, and relevant to Kenya's job market. "
        f"Keep total response under 400 characters. {lang_note}"
    )
    try:
        res = groq_client.chat.completions.create(
            model="mistral-saba-24b",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.7
        )
        return sms_format(res.choices[0].message.content)
    except Exception as e:
        print(f"Groq career error: {e}")
        return "Career guidance unavailable. Please try again later."

def ai_cbe_summary(topic: str, level: str, lang: str = "en") -> str:
    lang_note = "Respond in Kiswahili." if lang == "sw" else "Respond in English."
    prompt = (
        f"Summarize the CBC Kenya topic '{topic}' for a {level} student. "
        f"Provide 3 key learning points only. "
        f"Keep total response under 400 characters. {lang_note}"
    )
    try:
        res = groq_client.chat.completions.create(
            model="mistral-saba-24b",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.5
        )
        return sms_format(res.choices[0].message.content)
    except Exception as e:
        print(f"Groq CBE error: {e}")
        return "Summary unavailable. Please try again later."

def ai_risk_advice(risk: str, pathway: str, lang: str = "en") -> str:
    lang_note = "Respond in Kiswahili." if lang == "sw" else "Respond in English."
    prompt = (
        f"A Kenyan student has a {risk} transition risk for the {pathway} pathway under CBC. "
        f"Give 2 short, practical improvement tips. "
        f"Be direct, supportive, and specific. Under 350 characters. {lang_note}"
    )
    try:
        res = groq_client.chat.completions.create(
            model="mistral-saba-24b",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.6
        )
        return sms_format(res.choices[0].message.content)
    except Exception as e:
        print(f"Groq risk error: {e}")
        return "Risk advice unavailable. Please try again later."

# ---------- OUTBOUND SMS ----------
async def send_reply(to_phone: str, message: str):
    try:
        response = sms_service.send(
            message=message,
            recipients=[to_phone],
            sender_id=SENDER_ID
        )
        print(f"Sent to {to_phone[:7]}****: {message[:80]}")
        print("AT:", response)
    except Exception as e:
        print(f"SMS send failed: {e}")

# ---------- LANGUAGE DETECTION ----------
def detect_lang(text: str) -> str:
    sw_keywords = ["habari", "ndiyo", "hapana", "asante", "tafadhali", "sawa"]
    if any(kw in text.lower() for kw in sw_keywords):
        return "sw"
    return "en"

# ---------- MENU STRINGS ----------
MENU = {
    "en": {
        "welcome":     "Welcome to EduTena CBE.\nSelect Level:\n1. JSS\n2. Senior\n\nTip: Reply SW for Kiswahili",
        "level_err":   "Invalid. Select:\n1. JSS\n2. Senior",
        "math":        "Rate Math:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "science":     "Rate Science:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "social":      "Rate Social Studies:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "creative":    "Rate Creative Arts:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "tech":        "Rate Technical Skills:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "invalid":     "Invalid. Reply 1, 2, or 3.",
        "done":        "Commands:\nCAREERS - AI career guide\nLEARN [topic] - CBE summary\nRISK - transition risk\nSTART - restart",
        "no_pathway":  "Complete assessment first. Reply START.",
        "learn_usage": "Usage: LEARN [topic]\nExample: LEARN photosynthesis",
    },
    "sw": {
        "welcome":     "Karibu EduTena CBE.\nChagua Kiwango:\n1. JSS\n2. Sekondari\n\nKidokezo: Jibu EN kwa Kiingereza",
        "level_err":   "Batili. Chagua:\n1. JSS\n2. Sekondari",
        "math":        "Kadiria Hisabati:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "science":     "Kadiria Sayansi:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "social":      "Kadiria Sayansi Jamii:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "creative":    "Kadiria Sanaa:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "tech":        "Kadiria Ujuzi wa Kiufundi:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "invalid":     "Batili. Jibu 1, 2, au 3.",
        "done":        "Amri:\nCAREERS - mwongozo wa kazi\nLEARN [mada] - muhtasari CBE\nRISK - hatari ya mpito\nSTART - anza upya",
        "no_pathway":  "Maliza tathmini kwanza. Jibu START.",
        "learn_usage": "Matumizi: LEARN [mada]\nMfano: LEARN usanisinuru",
    }
}

RATING_MAP = {"1": 3, "2": 2, "3": 1}

# ---------- SMS WEBHOOK ----------
@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(
    from_: str = Form(..., alias="from"),
    text:  str = Form(...)
):
    phone      = from_
    phone_hash = hash_phone(phone)  # privacy: raw phone never stored
    text_clean = text.strip()
    text_upper = text_clean.upper()
    print(f"Incoming from {phone[:7]}****: {text_clean}")

    student = get_student(phone_hash)

    # ── Language switch ──────────────────────────────────────────
    if text_upper == "SW":
        save_student(phone_hash, "lang", "sw")
        await send_reply(phone, "Lugha: Kiswahili ✅\nJibu START kuanza.")
        return ""
    if text_upper == "EN":
        save_student(phone_hash, "lang", "en")
        await send_reply(phone, "Language: English ✅\nReply START to begin.")
        return ""

    # Determine language
    lang = "en"
    if student and student[10]:
        lang = student[10]
    else:
        lang = detect_lang(text_clean)
    M = MENU[lang]

    # ── START / first contact / reset ────────────────────────────
    if text_upper == "START" or not student:
        save_student(phone_hash, "state", "LEVEL")
        save_student(phone_hash, "lang", lang)
        await send_reply(phone, M["welcome"])
        return ""

    # ── Read current state ───────────────────────────────────────
    state   = student[9]
    pathway = student[7]
    level   = student[1] or "JSS"

    # ── Global commands ──────────────────────────────────────────

    # CAREERS — AI career guidance
    if text_upper == "CAREERS":
        if not pathway:
            pathway = calculate_pathway(phone_hash)
        if not pathway:
            await send_reply(phone, M["no_pathway"])
            return ""
        reply = ai_career_guidance(pathway, level, lang)
        await send_reply(phone, reply)
        return ""

    # LEARN [topic] — AI CBE summarizer
    if text_upper.startswith("LEARN"):
        parts = text_clean.split(None, 1)
        if len(parts) < 2:
            await send_reply(phone, M["learn_usage"])
            return ""
        topic = parts[1]
        reply = ai_cbe_summary(topic, level, lang)
        await send_reply(phone, reply)
        return ""

    # RISK — AI transition risk assessment
    if text_upper == "RISK":
        if not pathway:
            pathway = calculate_pathway(phone_hash)
        if not pathway:
            await send_reply(phone, M["no_pathway"])
            return ""
        risk        = calculate_risk(phone_hash)
        advice      = ai_risk_advice(risk, pathway, lang)
        risk_label  = {"Low": "✅ Low", "Medium": "⚠️ Medium", "High": "🔴 High"}.get(risk, risk)
        await send_reply(phone, f"Transition Risk: {risk_label}\n\n{advice}")
        return ""

    # ── Assessment state machine ─────────────────────────────────
    try:
        if state == "LEVEL":
            if text_clean == "1":
                lvl = "JSS"
            elif text_clean == "2":
                lvl = "Senior"
            else:
                await send_reply(phone, M["level_err"])
                return ""
            save_student(phone_hash, "level", lvl)
            save_student(phone_hash, "state", "MATH")
            await send_reply(phone, M["math"])
            return ""

        elif state == "MATH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['math']}")
                return ""
            save_student(phone_hash, "math", score)
            save_student(phone_hash, "state", "SCIENCE")
            await send_reply(phone, M["science"])
            return ""

        elif state == "SCIENCE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['science']}")
                return ""
            save_student(phone_hash, "science", score)
            save_student(phone_hash, "state", "SOCIAL")
            await send_reply(phone, M["social"])
            return ""

        elif state == "SOCIAL":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['social']}")
                return ""
            save_student(phone_hash, "social", score)
            save_student(phone_hash, "state", "CREATIVE")
            await send_reply(phone, M["creative"])
            return ""

        elif state == "CREATIVE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['creative']}")
                return ""
            save_student(phone_hash, "creative", score)
            save_student(phone_hash, "state", "TECH")
            await send_reply(phone, M["tech"])
            return ""

        elif state == "TECH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['tech']}")
                return ""
            save_student(phone_hash, "technical", score)

            # Calculate pathway + risk
            pathway    = calculate_pathway(phone_hash)
            risk       = calculate_risk(phone_hash)
            save_student(phone_hash, "state", "DONE")

            risk_label = {"Low": "✅ Low", "Medium": "⚠️ Medium", "High": "🔴 High"}.get(risk, risk)

            if lang == "sw":
                result = (
                    f"Njia: {pathway}\n"
                    f"Hatari ya Mpito: {risk_label}\n\n"
                    f"{M['done']}"
                )
            else:
                result = (
                    f"Pathway: {pathway}\n"
                    f"Transition Risk: {risk_label}\n\n"
                    f"{M['done']}"
                )
            await send_reply(phone, result)
            return ""

        else:
            # DONE state — show command menu
            await send_reply(phone, M["done"])
            return ""

    except Exception as e:
        print(f"Error: {e}")
        await send_reply(phone, "Something went wrong. Reply START to try again.")
        return ""

    await send_reply(phone, "Reply START to begin.")
    return ""
