from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse
import os
import africastalking

app = FastAPI()

# ---------- Initialize Africa's Talking SDK ----------
AT_USERNAME = os.getenv("AT_USERNAME")      # should be "sandbox"
AT_API_KEY = os.getenv("AT_API_KEY")

africastalking.initialize(username=AT_USERNAME, api_key=AT_API_KEY)
sms_service = africastalking.SMS

# ---------- ROOT ROUTE FOR HEALTH CHECK ----------
@app.get("/")
def root():
    return {"status": "FastAPI is running"}

# ---------- DATABASE CONNECTION ----------
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    url = urlparse(DATABASE_URL)
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    return conn

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            phone TEXT PRIMARY KEY,
            level TEXT,
            math INTEGER,
            science INTEGER,
            social INTEGER,
            creative INTEGER,
            technical INTEGER,
            pathway TEXT,
            state TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

# ---------- HELPERS ----------
ALLOWED_FIELDS = {"level", "math", "science", "social", "creative", "technical", "pathway", "state"}

def save_student(phone, field, value):
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit()
    cur.close()
    conn.close()

def get_student(phone):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM students WHERE phone=%s", (phone,))
    student = cur.fetchone()
    cur.close()
    conn.close()
    return student

def calculate_pathway(phone):
    student = get_student(phone)
    if not student:
        return None
    # phone, level, math, science, social, creative, technical, pathway, state
    _, _, math, science, social, creative, technical, _, _ = student

    stem = (math or 0) + (science or 0) + (technical or 0)
    social_score = (social or 0) * 2
    arts = (creative or 0) * 2

    if stem >= social_score and stem >= arts:
        pathway = "STEM"
    elif social_score >= stem and social_score >= arts:
        pathway = "Social Sciences"
    else:
        pathway = "Arts & Sports Science"

    save_student(phone, "pathway", pathway)
    return pathway

# ---------- Outbound SMS Helper ----------
async def send_reply(to_phone: str, message: str):
    try:
        response = sms_service.send(
            message=message,
            recipients=[to_phone],
            sender_id="98449"   # ← Required so replies appear in the simulator thread
        )
        print(f"Reply sent to {to_phone}: {message}")
        print("Full Africa's Talking response:", response)
    except Exception as e:
        print(f"Failed to send reply to {to_phone}: {str(e)}")

# ---------- SMS WEBHOOK ----------
RATING_MAP = {"1": 3, "2": 2, "3": 1}

@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(
    from_: str = Form(..., alias="from"),
    text: str = Form(...)
):
    phone = from_
    text_clean = text.strip()
    text_upper = text_clean.upper()
    print(f"Incoming SMS from {phone}: {text_clean} (upper: {text_upper})")

    student = get_student(phone)

    # Handle START / first contact / reset
    if text_upper == "START" or not student:
        save_student(phone, "state", "LEVEL")
        await send_reply(phone, "Welcome to Edutena CBE.\nSelect Level:\n1. JSS\n2. Senior")
        return ""

    if not student:
        await send_reply(phone, "Reply START to begin.")
        return ""

    # Get current state & pathway
    state = student[-1]   # last column = state
    pathway = student[-2] # second last = pathway

    # Global CAREERS command (anytime)
    if text_upper == "CAREERS":
        if not pathway:
            pathway = calculate_pathway(phone)
        if pathway == "STEM":
            await send_reply(phone, "STEM Careers:\n1. Engineering\n2. Data Science\n3. Medicine")
        elif pathway == "Social Sciences":
            await send_reply(phone, "Social Sciences Careers:\n1. Law\n2. Psychology\n3. Economics")
        elif pathway == "Arts & Sports Science":
            await send_reply(phone, "Arts & Sports Careers:\n1. Design\n2. Music\n3. Sports")
        else:
            await send_reply(phone, "No pathway calculated yet. Complete ratings first.")
        return ""

    try:
        if state == "LEVEL":
            if text_clean == "1":
                level = "JSS"
            elif text_clean == "2":
                level = "Senior"
            else:
                await send_reply(phone, "Invalid choice. Select Level:\n1. JSS\n2. Senior")
                return ""
            save_student(phone, "level", level)
            save_student(phone, "state", "MATH")
            await send_reply(phone, "Rate Math:\n1. Exceeding\n2. Meeting\n3. Approaching")
            return ""

        elif state == "MATH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, "Invalid input. Rate Math:\n1. Exceeding\n2. Meeting\n3. Approaching")
                return ""
            save_student(phone, "math", score)
            save_student(phone, "state", "SCIENCE")
            await send_reply(phone, "Rate Science:\n1. Exceeding\n2. Meeting\n3. Approaching")
            return ""

        elif state == "SCIENCE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, "Invalid input. Rate Science:\n1. Exceeding\n2. Meeting\n3. Approaching")
                return ""
            save_student(phone, "science", score)
            save_student(phone, "state", "SOCIAL")
            await send_reply(phone, "Rate Social Studies:\n1. Exceeding\n2. Meeting\n3. Approaching")
            return ""

        elif state == "SOCIAL":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, "Invalid input. Rate Social Studies:\n1. Exceeding\n2. Meeting\n3. Approaching")
                return ""
            save_student(phone, "social", score)
            save_student(phone, "state", "CREATIVE")
            await send_reply(phone, "Rate Creative Arts:\n1. Exceeding\n2. Meeting\n3. Approaching")
            return ""

        elif state == "CREATIVE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, "Invalid input. Rate Creative Arts:\n1. Exceeding\n2. Meeting\n3. Approaching")
                return ""
            save_student(phone, "creative", score)
            save_student(phone, "state", "TECH")
            await send_reply(phone, "Rate Technical Skills:\n1. Exceeding\n2. Meeting\n3. Approaching")
            return ""

        elif state == "TECH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, "Invalid input. Rate Technical Skills:\n1. Exceeding\n2. Meeting\n3. Approaching")
                return ""
            save_student(phone, "technical", score)
            save_student(phone, "state", "DONE")
            pathway = calculate_pathway(phone)
            await send_reply(phone, f"Recommended Pathway:\n{pathway}\nReply CAREERS to see careers")
            return ""

        else:
            # Unknown / DONE state
            await send_reply(phone, "You've completed the assessment.\nReply CAREERS for career suggestions or START to reset.")
            return ""

    except Exception as e:
        print("Error processing SMS:", str(e))
        await send_reply(phone, "Sorry, something went wrong. Please try again or reply START.")
        return ""

    # Fallback
    await send_reply(phone, "Reply START to begin.")
    return ""
