import os
from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse

app = FastAPI()

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

# ---------- STARTUP ----------

@app.on_event("startup")
def startup():
    init_db()

# ---------- HELPERS ----------

def save_student(phone, field, value):
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

@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(from_: str = Form(...), text: str = Form(...)):

    phone = from_
    text = text.strip()

    student = get_student(phone)

    if not student:
        save_student(phone, "state", "LEVEL")
        return "Welcome to Edutena CBE.\nSelect Level:\n1.JSS\n2.Senior"

    state = student[-1]

    if state == "LEVEL":
        level = "JSS" if text == "1" else "Senior"
        save_student(phone, "level", level)
        save_student(phone, "state", "MATH")
        return "Rate Math:\n1.Exceeding\n2.Meeting\n3.Approaching"

    if state == "MATH":
        save_student(phone, "math", 4-int(text))
        save_student(phone, "state", "SCIENCE")
        return "Rate Science:\n1.Exceeding\n2.Meeting\n3.Approaching"

    if state == "SCIENCE":
        save_student(phone, "science", 4-int(text))
        save_student(phone, "state", "SOCIAL")
        return "Rate Social Studies:\n1.Exceeding\n2.Meeting\n3.Approaching"

    if state == "SOCIAL":
        save_student(phone, "social", 4-int(text))
        save_student(phone, "state", "CREATIVE")
        return "Rate Creative Arts:\n1.Exceeding\n2.Meeting\n3.Approaching"

    if state == "CREATIVE":
        save_student(phone, "creative", 4-int(text))
        save_student(phone, "state", "TECH")
        return "Rate Technical Skills:\n1.Exceeding\n2.Meeting\n3.Approaching"

    if state == "TECH":
        save_student(phone, "technical", 4-int(text))
        save_student(phone, "state", "DONE")
        pathway = calculate_pathway(phone)
        return f"Recommended Pathway:\n{pathway}\nReply CAREERS"

    if text.upper() == "CAREERS":
        pathway = student[-2]
        if pathway == "STEM":
            return "1.Engineering\n2.Data Science\n3.Medicine"
        if pathway == "Social Sciences":
            return "1.Law\n2.Psychology\n3.Economics"
        if pathway == "Arts & Sports Science":
            return "1.Design\n2.Music\n3.Sports"

    return "Reply START to begin."