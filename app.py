from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse
import os

app = FastAPI()

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
ALLOWED_FIELDS = {"level","math","science","social","creative","technical","pathway","state"}

def save_student(phone, field, value):
    if field not in ALLOWED_FIELDS:
        raise ValueError("Invalid field")
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

# ---------- SMS WEBHOOK ----------
RATING_MAP = {"1": 3, "2": 2, "3": 1}

@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(
    from_: str = Form(..., alias="from"),  # Africa's Talking sends 'from'
    text: str = Form(...)
):
    phone = from_
    text = text.strip()
    print(f"Incoming SMS from {phone}: {text}")

    student = get_student(phone)
    if not student:
        save_student(phone, "state", "LEVEL")
        return "Welcome to Edutena CBE.\nSelect Level:\n1. JSS\n2. Senior"

    state = student[-1]
    pathway = student[-2]

    # Global CAREERS check
    if text.upper() == "CAREERS":
        if not pathway:
            pathway = calculate_pathway(phone)
        if pathway == "STEM":
            return "1. Engineering\n2. Data Science\n3. Medicine"
        if pathway == "Social Sciences":
            return "1. Law\n2. Psychology\n3. Economics"
        if pathway == "Arts & Sports Science":
            return "1. Design\n2. Music\n3. Sports"

    try:
        if state == "LEVEL":
            level = "JSS" if text == "1" else "Senior"
            save_student(phone, "level", level)
            save_student(phone, "state", "MATH")
            return "Rate Math:\n1. Exceeding\n2. Meeting\n3. Approaching"

        if state == "MATH":
            score = RATING_MAP.get(text)
            if not score:
                return "Invalid input. Rate Math:\n1. Exceeding\n2. Meeting\n3. Approaching"
            save_student(phone, "math", score)
            save_student(phone, "state", "SCIENCE")
            return "Rate Science:\n1. Exceeding\n2. Meeting\n3. Approaching"

        if state == "SCIENCE":
            score = RATING_MAP.get(text)
            if not score:
                return "Invalid input. Rate Science:\n1. Exceeding\n2. Meeting\n3. Approaching"
            save_student(phone, "science", score)
            save_student(phone, "state", "SOCIAL")
            return "Rate Social Studies:\n1. Exceeding\n2. Meeting\n3. Approaching"

        if state == "SOCIAL":
            score = RATING_MAP.get(text)
            if not score:
                return "Invalid input. Rate Social Studies:\n1. Exceeding\n2. Meeting\n3. Approaching"
            save_student(phone, "social", score)
            save_student(phone, "state", "CREATIVE")
            return "Rate Creative Arts:\n1. Exceeding\n2. Meeting\n3. Approaching"

        if state == "CREATIVE":
            score = RATING_MAP.get(text)
            if not score:
                return "Invalid input. Rate Creative Arts:\n1. Exceeding\n2. Meeting\n3. Approaching"
            save_student(phone, "creative", score)
            save_student(phone, "state", "TECH")
            return "Rate Technical Skills:\n1. Exceeding\n2. Meeting\n3. Approaching"

        if state == "TECH":
            score = RATING_MAP.get(text)
            if not score:
                return "Invalid input. Rate Technical Skills:\n1. Exceeding\n2. Meeting\n3. Approaching"
            save_student(phone, "technical", score)
            save_student(phone, "state", "DONE")
            pathway = calculate_pathway(phone)
            return f"Recommended Pathway:\n{pathway}\nReply CAREERS to see careers"

        return "Reply START to begin."
    except Exception as e:
        print("Error processing SMS:", e)
        return "Sorry, something went wrong. Please try again."