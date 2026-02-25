from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse
import os
import africastalking

app = FastAPI()

# ---------- Initialize Africa's Talking SDK ----------
AT_USERNAME = os.getenv("AT_USERNAME")
AT_API_KEY  = os.getenv("AT_API_KEY")

africastalking.initialize(username=AT_USERNAME, api_key=AT_API_KEY)
sms_service = africastalking.SMS

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
    # Create table if not exists (fresh installs)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            phone      TEXT PRIMARY KEY,
            level      TEXT,
            math       INTEGER,
            science    INTEGER,
            social     INTEGER,
            creative   INTEGER,
            technical  INTEGER,
            pathway    TEXT,
            state      TEXT,
            lang       TEXT DEFAULT 'en'
        )
    """)
    # Safe migration: add lang column if upgrading from old schema
    cur.execute("""
        ALTER TABLE students ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'en'
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

# ---------- HELPERS ----------
ALLOWED_FIELDS = {
    "level", "math", "science", "social", "creative",
    "technical", "pathway", "state", "lang"
}

def save_student(phone, field, value):
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("INSERT INTO students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit()
    cur.close()
    conn.close()

def get_student(phone):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM students WHERE phone=%s", (phone,))
    student = cur.fetchone()
    cur.close()
    conn.close()
    return student
    # columns: phone(0), level(1), math(2), science(3), social(4),
    #          creative(5), technical(6), pathway(7), state(8), lang(9)

def calculate_pathway(phone):
    student = get_student(phone)
    if not student:
        return None
    _, _, math, science, social, creative, technical, _, _, _ = student

    stem         = (math or 0) + (science or 0) + (technical or 0)
    social_score = (social or 0) * 2
    arts         = (creative or 0) * 2

    if stem >= social_score and stem >= arts:
        pathway = "STEM"
    elif social_score >= stem and social_score >= arts:
        pathway = "Social Sciences"
    else:
        pathway = "Arts & Sports Science"

    save_student(phone, "pathway", pathway)
    return pathway

# ---------- OUTBOUND SMS ----------
async def send_reply(to_phone: str, message: str):
    try:
        response = sms_service.send(
            message=message,
            recipients=[to_phone],
            sender_id=SENDER_ID
        )
        print(f"Reply to {to_phone[:7]}****: {message[:80]}")
        print("AT:", response)
    except Exception as e:
        print(f"SMS failed: {str(e)}")

# ---------- LANGUAGE MENUS ----------
# Supported languages: en, sw, lh (Luhya), ki (Kikuyu)

MENU = {
    "en": {
        "lang_confirm":  "Language: English ✅\nReply START to begin.",
        "welcome":       "Welcome to EduTena CBE.\nSelect Level:\n1. JSS\n2. Senior\n\nChange language:\nSW=Swahili LH=Luhya KI=Kikuyu",
        "level_err":     "Invalid. Select:\n1. JSS\n2. Senior",
        "math":          "Rate Math:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "science":       "Rate Science:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "social":        "Rate Social Studies:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "creative":      "Rate Creative Arts:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "tech":          "Rate Technical Skills:\n1. Exceeding\n2. Meeting\n3. Approaching",
        "invalid":       "Invalid. Reply 1, 2, or 3.",
        "pathway_msg":   "Recommended Pathway:\n{pathway}\nReply CAREERS to see careers.",
        "careers_stem":  "STEM Careers:\n1. Engineering\n2. Data Science\n3. Medicine",
        "careers_soc":   "Social Sciences Careers:\n1. Law\n2. Psychology\n3. Economics",
        "careers_arts":  "Arts & Sports Careers:\n1. Design\n2. Music\n3. Sports",
        "no_pathway":    "Complete assessment first. Reply START.",
        "done":          "Assessment complete.\nReply CAREERS for options or START to restart.",
    },
    "sw": {
        "lang_confirm":  "Lugha: Kiswahili ✅\nJibu START kuanza.",
        "welcome":       "Karibu EduTena CBE.\nChagua Kiwango:\n1. JSS\n2. Sekondari\n\nBadilisha lugha:\nEN=Kiingereza LH=Luhya KI=Kikuyu",
        "level_err":     "Batili. Chagua:\n1. JSS\n2. Sekondari",
        "math":          "Kadiria Hisabati:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "science":       "Kadiria Sayansi:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "social":        "Kadiria Sayansi Jamii:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "creative":      "Kadiria Sanaa:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia",
        "invalid":       "Batili. Jibu 1, 2, au 3.",
        "pathway_msg":   "Njia Inayopendekezwa:\n{pathway}\nJibu CAREERS kuona kazi.",
        "careers_stem":  "Kazi za STEM:\n1. Uhandisi\n2. Sayansi ya Data\n3. Dawa",
        "careers_soc":   "Kazi za Sayansi Jamii:\n1. Sheria\n2. Saikolojia\n3. Uchumi",
        "careers_arts":  "Kazi za Sanaa & Michezo:\n1. Usanifu\n2. Muziki\n3. Michezo",
        "no_pathway":    "Maliza tathmini kwanza. Jibu START.",
        "done":          "Tathmini imekamilika.\nJibu CAREERS kwa kazi au START kuanza upya.",
    },
    "lh": {
        "lang_confirm":  "Olulimi: Luhya ✅\nJibu START okhuandaa.",
        "welcome":       "Wafwelwa e EduTena CBE.\nSena Engufu:\n1. JSS\n2. Sekondari\n\nSena olulimi:\nEN=Kingereza SW=Kiswahili KI=Kikuyu",
        "level_err":     "Busia. Sena:\n1. JSS\n2. Sekondari",
        "math":          "Kadiria Hesabu:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela",
        "science":       "Kadiria Sayansi:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela",
        "social":        "Kadiria Elimu ya Jamii:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela",
        "creative":      "Kadiria Sanaa:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela",
        "invalid":       "Busia. Jibu 1, 2, kamba 3.",
        "pathway_msg":   "Njia Enyiseniwe:\n{pathway}\nJibu CAREERS okhuona emilimo.",
        "careers_stem":  "Emilimo ya STEM:\n1. Uhandisi\n2. Sayansi ya Data\n3. Dawa",
        "careers_soc":   "Emilimo ya Jamii:\n1. Sheria\n2. Saikolojia\n3. Uchumi",
        "careers_arts":  "Emilimo ya Sanaa:\n1. Usanifu\n2. Muziki\n3. Michezo",
        "no_pathway":    "Maliza tathmini kwanza. Jibu START.",
        "done":          "Tathmini yakhwira.\nJibu CAREERS kwa emilimo kamba START okhuanza.",
    },
    "ki": {
        "lang_confirm":  "Rurimi: Kikuyu ✅\nCookia START guthomia.",
        "welcome":       "Ndumiria EduTena CBE.\nThura Kiwango:\n1. JSS\n2. Sekondari\n\nThura rurimi:\nEN=Kingereza SW=Kiswahili LH=Luhya",
        "level_err":     "Ti wegwaru. Thura:\n1. JSS\n2. Sekondari",
        "math":          "Kadiria Hesabu:\n1. Gucokia\n2. Gufika\n3. Guserekania",
        "science":       "Kadiria Sayansi:\n1. Gucokia\n2. Gufika\n3. Guserekania",
        "social":        "Kadiria Maarifa ya Jamii:\n1. Gucokia\n2. Gufika\n3. Guserekania",
        "creative":      "Kadiria Sanaa:\n1. Gucokia\n2. Gufika\n3. Guserekania",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Gucokia\n2. Gufika\n3. Guserekania",
        "invalid":       "Ti wegwaru. Cookia 1, 2, kana 3.",
        "pathway_msg":   "Njia Yoneneirwo:\n{pathway}\nCookia CAREERS kuona mirimo.",
        "careers_stem":  "Mirimo ya STEM:\n1. Uhandisi\n2. Sayansi ya Data\n3. Dawa",
        "careers_soc":   "Mirimo ya Jamii:\n1. Sheria\n2. Saikolojia\n3. Uchumi",
        "careers_arts":  "Mirimo ya Sanaa:\n1. Usanifu\n2. Muziki\n3. Michezo",
        "no_pathway":    "Ithoma mbere. Cookia START.",
        "done":          "Ithomo niikuura.\nCookia CAREERS mirimo kana START gutomia.",
    },
}

# Commands to switch language (work anytime, even mid-assessment)
LANG_TRIGGERS = {
    "EN": "en",
    "SW": "sw",
    "LH": "lh",
    "KI": "ki",
}

RATING_MAP = {"1": 3, "2": 2, "3": 1}

# ---------- SMS WEBHOOK ----------
@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(
    from_: str = Form(..., alias="from"),
    text:  str = Form(...)
):
    phone      = from_
    text_clean = text.strip()
    text_upper = text_clean.upper()
    print(f"Incoming from {phone[:7]}****: {text_clean}")

    student = get_student(phone)

    # ── Language switch (works anytime, even mid-assessment) ─────
    if text_upper in LANG_TRIGGERS:
        new_lang = LANG_TRIGGERS[text_upper]
        save_student(phone, "lang", new_lang)
        await send_reply(phone, MENU[new_lang]["lang_confirm"])
        return ""

    # Determine current language (default English)
    lang = "en"
    if student and len(student) >= 10 and student[9]:
        lang = student[9]
    M = MENU[lang]

    # ── START / first contact / reset ────────────────────────────
    if text_upper == "START" or not student:
        save_student(phone, "state", "LEVEL")
        save_student(phone, "lang", lang)
        await send_reply(phone, M["welcome"])
        return ""

    if not student:
        await send_reply(phone, M["welcome"])
        return ""

    # ── Read current state ───────────────────────────────────────
    state   = student[8]
    pathway = student[7]

    # ── CAREERS command ──────────────────────────────────────────
    if text_upper == "CAREERS":
        if not pathway:
            pathway = calculate_pathway(phone)
        if not pathway:
            await send_reply(phone, M["no_pathway"])
            return ""
        if pathway == "STEM":
            await send_reply(phone, M["careers_stem"])
        elif pathway == "Social Sciences":
            await send_reply(phone, M["careers_soc"])
        else:
            await send_reply(phone, M["careers_arts"])
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
            save_student(phone, "level", lvl)
            save_student(phone, "state", "MATH")
            await send_reply(phone, M["math"])
            return ""

        elif state == "MATH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['math']}")
                return ""
            save_student(phone, "math", score)
            save_student(phone, "state", "SCIENCE")
            await send_reply(phone, M["science"])
            return ""

        elif state == "SCIENCE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['science']}")
                return ""
            save_student(phone, "science", score)
            save_student(phone, "state", "SOCIAL")
            await send_reply(phone, M["social"])
            return ""

        elif state == "SOCIAL":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['social']}")
                return ""
            save_student(phone, "social", score)
            save_student(phone, "state", "CREATIVE")
            await send_reply(phone, M["creative"])
            return ""

        elif state == "CREATIVE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['creative']}")
                return ""
            save_student(phone, "creative", score)
            save_student(phone, "state", "TECH")
            await send_reply(phone, M["tech"])
            return ""

        elif state == "TECH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['tech']}")
                return ""
            save_student(phone, "technical", score)
            save_student(phone, "state", "DONE")
            pathway = calculate_pathway(phone)
            await send_reply(phone, M["pathway_msg"].format(pathway=pathway))
            return ""

        else:
            # DONE state — remind of available commands
            await send_reply(phone, M["done"])
            return ""

    except Exception as e:
        print("Error:", str(e))
        await send_reply(phone, "Error. Reply START to try again.")
        return ""

    await send_reply(phone, M["welcome"])
    return ""
