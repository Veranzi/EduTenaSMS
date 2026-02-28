from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse
import os
import httpx
import asyncio
import africastalking

app = FastAPI()

AT_USERNAME = os.getenv("AT_USERNAME")
AT_API_KEY  = os.getenv("AT_API_KEY")
africastalking.initialize(username=AT_USERNAME, api_key=AT_API_KEY)
sms_service  = africastalking.SMS
SENDER_ID    = os.getenv("AT_SENDER_ID", "98449")
GEMINI_KEY   = os.getenv("GEMINI_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL")

@app.get("/")
def root():
    return {"status": "EduTena API is running", "endpoints": {"sms": "/sms", "ussd": "/ussd"}}

# =============================================================
#  DATABASE
# =============================================================

def get_connection():
    url = urlparse(DATABASE_URL)
    return psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password, host=url.hostname, port=url.port)

def init_db():
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            phone TEXT PRIMARY KEY, lang TEXT DEFAULT 'en',
            level TEXT, grade TEXT, term TEXT, pathway TEXT,
            math INTEGER, science INTEGER, social INTEGER,
            creative INTEGER, technical INTEGER,
            career_interest TEXT, state TEXT
        )
    """)
    for col in ["lang","grade","term","pathway","career_interest"]:
        cur.execute(f"ALTER TABLE students ADD COLUMN IF NOT EXISTS {col} TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ussd_students (
            phone TEXT PRIMARY KEY, lang TEXT DEFAULT 'en',
            level TEXT, grade TEXT, term TEXT, pathway TEXT,
            math INTEGER, science INTEGER, social INTEGER,
            creative INTEGER, technical INTEGER,
            career_interest TEXT, state TEXT
        )
    """)
    for col in ["lang","grade","term","pathway","career_interest"]:
        cur.execute(f"ALTER TABLE ussd_students ADD COLUMN IF NOT EXISTS {col} TEXT")

    # SMS chat history table for Gemini RAG
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            id SERIAL PRIMARY KEY,
            phone TEXT,
            role TEXT,
            message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Clean corrupted rows from old schema
    for table in ["students", "ussd_students"]:
        cur.execute(f"""
            UPDATE {table} SET lang='en', state='LANG',
                level=NULL, grade=NULL, term=NULL, pathway=NULL,
                math=NULL, science=NULL, social=NULL,
                creative=NULL, technical=NULL, career_interest=NULL
            WHERE lang NOT IN ('en','sw','lh','ki') OR lang IS NULL
        """)

    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

# =============================================================
#  SHARED CONSTANTS
# =============================================================

# CBE Structure:
# JSS    → Grade 7, 8, 9   (Junior Secondary)
# Senior → Grade 10, 11, 12 (Senior Secondary)
#
# Grade 7 & 8  → Track performance + improvement suggestions
# Grade 9      → Predict pathway (STEM / Social Sciences / Arts)
# Grade 10-12  → Pathway-specific subjects + careers + market data

RATING_MAP = {"1": 4, "2": 3, "3": 2, "4": 1}
# 4=Exceeding Expectation, 3=Meeting, 2=Approaching, 1=Below

RATING_OPTIONS_SMS = (
    "1. Exceeding Expectation\n"
    "2. Meeting Expectation\n"
    "3. Approaching Expectation\n"
    "4. Below Expectation"
)
RATING_OPTIONS_USSD = (
    "1. Exceeding\n"
    "2. Meeting\n"
    "3. Approaching\n"
    "4. Below"
)

JSS_GRADES    = {"1": "Grade 7", "2": "Grade 8", "3": "Grade 9"}
SENIOR_GRADES = {"1": "Grade 10", "2": "Grade 11", "3": "Grade 12"}
TERMS         = {"1": "Term 1", "2": "Term 2", "3": "Term 3"}
LANG_MAP      = {"1": "en", "2": "sw", "3": "lh", "4": "ki"}
PATHWAYS      = {"1": "STEM", "2": "Social Sciences", "3": "Arts & Sports Science"}

# =============================================================
#  PATHWAY-SPECIFIC SUBJECTS FOR SENIOR (Grade 10-12)
# =============================================================
# Each pathway has its own 5 subjects that are rated

SENIOR_SUBJECTS = {
    "STEM": [
        ("math",       "Mathematics"),
        ("science",    "Physics/Chemistry"),
        ("technical",  "Computer Science"),
        ("social",     "Biology"),
        ("creative",   "Agriculture/Tech"),
    ],
    "Social Sciences": [
        ("math",       "Mathematics"),
        ("social",     "History & Government"),
        ("creative",   "Business Studies"),
        ("science",    "Geography"),
        ("technical",  "CRE/IRE"),
    ],
    "Arts & Sports Science": [
        ("creative",   "Visual Arts/Music"),
        ("social",     "Drama & Theatre"),
        ("technical",  "Physical Education"),
        ("math",       "Mathematics"),
        ("science",    "Home Science/Tech"),
    ],
}

def get_senior_subjects(pathway: str) -> list:
    return SENIOR_SUBJECTS.get(pathway, SENIOR_SUBJECTS["STEM"])

# State machine for Senior subject ratings
SENIOR_STATES = ["S_SUBJ1", "S_SUBJ2", "S_SUBJ3", "S_SUBJ4", "S_SUBJ5"]
SENIOR_DB_FIELDS = ["math", "science", "technical", "social", "creative"]

# =============================================================
#  KENYA LABOUR MARKET 2025 — CAREER DATA
# =============================================================

SENIOR_CAREERS = {
    "STEM": [
        # (name, demand★, salary KES/mo, trend, university_options, focus_subjects)
        ("Software Engineer",        "5★", "80K-200K", "↑ Silicon Savannah boom",
         "UoN, Strathmore, JKUAT, KU",
         "Math, Computer Science, Physics"),
        ("Data Scientist",           "5★", "70K-180K", "↑ Highest demand 2025",
         "Strathmore, UoN, JKUAT",
         "Math, Statistics, Computer Science"),
        ("Cybersecurity Specialist", "5★", "90K-250K", "↑ Critical shortage",
         "Strathmore, KU, JKUAT",
         "Computer Science, Math, Physics"),
        ("Renewable Energy Eng.",    "4★", "60K-150K", "↑ Green energy boom",
         "UoN, JKUAT, Moi University",
         "Physics, Chemistry, Math"),
        ("Medical Doctor",           "4★", "80K-300K", "↑ Healthcare growing",
         "UoN, Moi, KMTC",
         "Biology, Chemistry, Physics"),
        ("Pharmacist",               "4★", "50K-120K", "↑ Pharma sector rising",
         "UoN, KU, Pharmacy Board Kenya",
         "Chemistry, Biology, Math"),
        ("Civil Engineer",           "4★", "55K-130K", "→ Steady, housing demand",
         "UoN, JKUAT, Technical Univ. Kenya",
         "Math, Physics, Technical Drawing"),
        ("Lab Technician",           "3★", "25K-55K",  "→ Public sector mostly",
         "KMTC, Kenya Polytechnic, KU",
         "Biology, Chemistry, Physics"),
        ("Architect",                "3★", "50K-120K", "→ Urban projects",
         "UoN, TUK, JKUAT",
         "Math, Physics, Art & Design"),
        ("ICT Support",              "3★", "30K-70K",  "→ Steady countrywide",
         "Kenya Polytechnic, KCA, Zetech",
         "Computer Science, Math"),
    ],
    "Social Sciences": [
        ("Accountant/Auditor",       "5★", "50K-150K", "↑ Most advertised 2025",
         "Strathmore, UoN, KCA, ACCA",
         "Math, Business Studies, Economics"),
        ("Digital Marketer",         "4★", "35K-120K", "↑ 17% of job postings",
         "Strathmore, USIU, KCA",
         "Business, ICT, Communications"),
        ("Finance Manager",          "5★", "80K-200K", "↑ Fintech driving demand",
         "Strathmore, UoN, CFA Institute",
         "Math, Business Studies, Economics"),
        ("Lawyer/Advocate",          "4★", "60K-250K", "↑ Legal services growing",
         "UoN, Moi, KU, Strathmore",
         "History, CRE, English/Kiswahili"),
        ("Sales Executive",          "4★", "30K-100K", "↑ Top 3 most hired role",
         "Any university, KISM",
         "Business, Communication, Economics"),
        ("Human Resource Mgr",       "3★", "45K-120K", "→ Steady all sectors",
         "UoN, KU, Moi, IHRM Kenya",
         "Business, Sociology, Psychology"),
        ("Economist",                "3★", "50K-130K", "→ Government & research",
         "UoN, Moi, USIU",
         "Math, Economics, Geography"),
        ("Teacher/Educator",         "3★", "25K-60K",  "→ High need, CBC era",
         "KU, Moi, Maseno, TTC Colleges",
         "Any subject specialization"),
        ("Psychologist",             "3★", "30K-80K",  "↑ Mental health growing",
         "UoN, USIU, KU",
         "Biology, CRE, Sociology"),
        ("Journalist/Media",         "2★", "20K-60K",  "↓ Print down, digital up",
         "USIU, Daystar, KU",
         "English/Kiswahili, History, ICT"),
    ],
    "Arts & Sports Science": [
        ("Graphic Designer/UI-UX",   "4★", "35K-120K", "↑ Digital economy boom",
         "ADMI, Kenyatta, Limkokwing",
         "Visual Arts, Computer Science, Math"),
        ("Film & Content Creator",   "4★", "20K-150K", "↑ Social media economy",
         "AFDA, ADMI, Daystar",
         "Drama, Visual Arts, ICT"),
        ("Physiotherapist",          "3★", "35K-90K",  "↑ Sports & healthcare",
         "UoN, KU, KMTC",
         "Physical Education, Biology, Chemistry"),
        ("Sports Coach/Manager",     "3★", "25K-80K",  "→ Growing, academies",
         "KU, Moi, Sports Kenya",
         "Physical Education, Biology"),
        ("Interior Designer",        "3★", "30K-100K", "↑ Urban housing boom",
         "ADMI, TUK, Kenyatta",
         "Visual Arts, Math, Technical Drawing"),
        ("Tourism/Hospitality Mgr",  "3★", "30K-100K", "↑ Post-COVID recovery",
         "Utalii College, KU, USIU",
         "Geography, Business, Home Science"),
        ("Fashion Designer",         "2★", "15K-80K",  "→ Niche but growing",
         "Kenya Fashion Institute, ADMI",
         "Visual Arts, Home Science, Business"),
        ("Beauty & Wellness",        "3★", "20K-70K",  "↑ TVET boom",
         "TVET Colleges, Kenya Beauty School",
         "Home Science, Biology, Chemistry"),
        ("Musician/Performer",       "2★", "Variable", "→ Competitive",
         "Kenya Conservatoire, Daystar",
         "Music, Drama, Visual Arts"),
        ("Community Development",    "2★", "25K-60K",  "→ NGO sector",
         "UoN, Moi, Catholic University",
         "History, CRE, Sociology"),
    ],
}

def get_career_list_sms(pathway: str, lang: str, grade: str) -> str:
    """Top 5 careers for pathway with demand + salary. Student picks 1-5."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    hdr = {
        "en": f"{pathway} Careers | {grade}\nKenya Market 2025\nSelect your interest:\n",
        "sw": f"Kazi za {pathway} | {grade}\nSoko Kenya 2025\nChagua hamu yako:\n",
        "lh": f"Emilimo ya {pathway} | {grade}\nSoko Kenya 2025\nSena hamu yako:\n",
        "ki": f"Mirimo ya {pathway} | {grade}\nSoko Kenya 2025\nThura hamu yako:\n",
    }
    msg = hdr.get(lang, hdr["en"])
    for i, (name, stars, salary, trend, unis, subjects) in enumerate(careers[:5], 1):
        msg += f"{i}. {name} {stars}\n   KES {salary} | {trend}\n"
    footer = {
        "en": "\nReply 1-5 to select career\nReply MORE to see all 10",
        "sw": "\nJibu 1-5 kuchagua kazi\nJibu MORE kuona zote 10",
        "lh": "\nJibu 1-5 okhuсena emilimo\nJibu MORE okhuona yote 10",
        "ki": "\nCookia 1-5 guthura mirimo\nCookia MORE kuona yothe 10",
    }
    msg += footer.get(lang, footer["en"])
    return msg

def get_career_detail_sms(pathway: str, career_idx: int, lang: str) -> str:
    """Full detail for selected career — universities + subjects to focus on."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    if career_idx < 0 or career_idx >= len(careers):
        return "Invalid selection."
    name, stars, salary, trend, unis, subjects = careers[career_idx]
    msgs = {
        "en": (
            f"Career: {name}\n"
            f"Market Demand: {stars}\n"
            f"Salary: KES {salary}/mo\n"
            f"Trend: {trend}\n\n"
            f"Universities/Colleges:\n{unis}\n\n"
            f"Focus Subjects:\n{subjects}\n\n"
            f"Saved to your profile!\nReply START to reassess."
        ),
        "sw": (
            f"Kazi: {name}\n"
            f"Mahitaji: {stars}\n"
            f"Mshahara: KES {salary}/mwezi\n"
            f"Mwelekeo: {trend}\n\n"
            f"Vyuo:\n{unis}\n\n"
            f"Masomo ya Kuzingatia:\n{subjects}\n\n"
            f"Imehifadhiwa!\nJibu START kuanza upya."
        ),
        "lh": (
            f"Emilimo: {name}\n"
            f"Haja: {stars}\n"
            f"Mishahara: KES {salary}/mwezi\n\n"
            f"Vyuo:\n{unis}\n\n"
            f"Masomo:\n{subjects}\n\n"
            f"Imehifadhiwa!\nJibu START okhuanza."
        ),
        "ki": (
            f"Murimo: {name}\n"
            f"Hitaji: {stars}\n"
            f"Mshahara: KES {salary}/mwezi\n\n"
            f"Vyuo:\n{unis}\n\n"
            f"Masomo:\n{subjects}\n\n"
            f"Niikuura!\nCookia START gutomia."
        ),
    }
    return msgs.get(lang, msgs["en"])

def get_all_careers_sms(pathway: str, lang: str) -> str:
    """All 10 careers — sent when student replies MORE."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    hdr = {
        "en": f"All {pathway} Careers\nKenya Market 2025\nSelect your interest:\n",
        "sw": f"Kazi Zote za {pathway}\nSoko Kenya 2025\nChagua:\n",
        "lh": f"Emilimo Yote ya {pathway}\nSoko Kenya 2025\nSena:\n",
        "ki": f"Mirimo Yothe ya {pathway}\nSoko Kenya 2025\nThura:\n",
    }
    msg = hdr.get(lang, hdr["en"])
    for i, (name, stars, salary, trend, unis, subjects) in enumerate(careers, 1):
        msg += f"{i}. {name} {stars} KES {salary}\n"
    footer = {
        "en": "\nReply 1-10 to select your career interest.",
        "sw": "\nJibu 1-10 kuchagua kazi yako.",
        "lh": "\nJibu 1-10 okhuсena emilimo yako.",
        "ki": "\nCookia 1-10 guthura murimo waku.",
    }
    msg += footer.get(lang, footer["en"])
    return msg

def get_career_ussd_list(pathway: str) -> str:
    """Compact USSD career list — name + stars only."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    lines = f"{pathway}\nSelect Career Interest:\n"
    for i, (name, stars, salary, trend, unis, subjects) in enumerate(careers[:6], 1):
        short = name[:16] if len(name) > 16 else name
        lines += f"{i}. {short} {stars}\n"
    lines += "7. More careers"
    return lines

# =============================================================
#  GEMINI RAG — CBE KNOWLEDGE BASE
# =============================================================

CBE_KNOWLEDGE = """
You are EduTena, a Kenya CBE (Competency Based Education) assistant.
You ONLY answer questions about:
- Kenya CBC/CBE curriculum structure
- JSS (Junior Secondary School) Grade 7, 8, 9
- Senior Secondary School Grade 10, 11, 12
- CBE pathways: STEM, Social Sciences, Arts & Sports Science
- The 4 CBE performance levels: Exceeding Expectation (4), Meeting Expectation (3),
  Approaching Expectation (2), Below Expectation (1)
- Subject areas: Math, Science, Social Studies, Creative Arts, Technical Skills
- Career guidance aligned to CBC pathways
- Kenyan universities and colleges for each pathway
- Kenya labour market and job demand by pathway
- How parents and students can navigate the CBC system
- Term assessments and how pathway selection works in Grade 9

RULES:
- Keep answers SHORT — max 3 sentences (this is SMS, character limit matters)
- Be warm, encouraging, speak like a Kenyan educator
- If asked something NOT about CBE/CBC Kenya education, say:
  "I can only help with CBE/CBC education questions. Reply START to continue assessment."
- Never give medical, legal, or financial investment advice
- Always end with an encouraging phrase for the student
"""

async def ask_gemini(phone: str, question: str) -> str:
    """
    Send question to Gemini with CBE RAG context.
    Includes last 3 exchanges for conversation memory.
    """
    if not GEMINI_KEY:
        return "AI assistant not configured. Reply START to continue your assessment."

    # Get last 3 exchanges from chat history
    history = get_chat_history(phone, limit=6)
    history_text = ""
    for role, msg in history:
        history_text += f"{role.upper()}: {msg}\n"

    prompt = (
        f"{CBE_KNOWLEDGE}\n\n"
        f"CONVERSATION HISTORY:\n{history_text}\n"
        f"STUDENT QUESTION: {question}\n\n"
        f"Answer in max 3 short sentences suitable for SMS:"
    )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "maxOutputTokens": 150,
                        "temperature": 0.4,
                    }
                }
            )
            data   = response.json()
            answer = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            # Save to history
            save_chat(phone, "user", question)
            save_chat(phone, "assistant", answer)
            return answer
    except Exception as e:
        print(f"[GEMINI] Error: {e}")
        return "Sorry, I could not answer that right now. Reply START to continue your assessment."

def get_chat_history(phone: str, limit: int = 6):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT role, message FROM chat_history
        WHERE phone=%s ORDER BY created_at DESC LIMIT %s
    """, (phone, limit))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return list(reversed(rows))

def save_chat(phone: str, role: str, message: str):
    conn = get_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO chat_history(phone, role, message) VALUES(%s,%s,%s)",
        (phone, role, message)
    )
    conn.commit(); cur.close(); conn.close()

def is_cbe_question(text: str) -> bool:
    """Detect if student is asking a question (not a menu digit or command)."""
    commands = {"START","CAREERS","MORE","1","2","3","4","5","6","7","8","9","10",
                "EN","SW","LH","KI","HELP"}
    text_upper = text.strip().upper()
    if text_upper in commands:
        return False
    # If it's longer than 3 chars and not a single digit — treat as a question
    if len(text.strip()) > 3 and not text.strip().isdigit():
        return True
    return False

# =============================================================
#  PATHWAY CALCULATOR (JSS Grade 9)
# =============================================================

def calculate_pathway_from_scores(math, science, social, creative, technical):
    stem         = (math or 0) + (science or 0) + (technical or 0)
    social_score = (social or 0) * 2
    arts         = (creative or 0) * 2
    if stem >= social_score and stem >= arts:
        return "STEM"
    elif social_score >= stem and social_score >= arts:
        return "Social Sciences"
    else:
        return "Arts & Sports Science"

# =============================================================
#  IMPROVEMENT SUGGESTIONS (Grade 7, 8)
# =============================================================

def get_improvement_suggestions(math, science, social, creative, technical, lang="en"):
    weak = []
    subjects    = {"Math": math, "Science": science, "Social Studies": social,
                   "Creative Arts": creative, "Technical Skills": technical}
    subjects_sw = {"Math": "Hisabati", "Science": "Sayansi",
                   "Social Studies": "Sayansi Jamii",
                   "Creative Arts": "Sanaa", "Technical Skills": "Ujuzi wa Kiufundi"}
    for subj, score in subjects.items():
        if (score or 0) <= 2:
            weak.append(subjects_sw[subj] if lang in ("sw","lh","ki") else subj)

    if not weak:
        return {"en": "Excellent! You are on track in all subjects. Keep it up!",
                "sw": "Vizuri sana! Uko vizuri katika masomo yote. Endelea!",
                "lh": "Wewe omulahi! Uko sawa kwa masomo yote. Endelea!",
                "ki": "Uria mwega! Uri mwega kwa masomo mothe. Endelea!"}.get(lang,"")
    weak_str = ", ".join(weak)
    return {
        "en": f"Work harder on: {weak_str}. Ask your teacher for extra help and practice daily.",
        "sw": f"Jaribu zaidi: {weak_str}. Omba mwalimu msaada na fanya mazoezi kila siku.",
        "lh": f"Jaribu khale: {weak_str}. Omba mwalimu msaada na fanya mazoezi.",
        "ki": f"Thiini guthoma: {weak_str}. Uiguithia mwarimu na ithima mara nyingi.",
    }.get(lang, "")

# =============================================================
#  LANGUAGE MENUS
# =============================================================

LANG_SELECT_MSG = (
    "Welcome to EduTena CBE.\n"
    "Select Language:\n"
    "1. English\n"
    "2. Swahili\n"
    "3. Luhya\n"
    "4. Kikuyu"
)

SMS_MENU = {
    "en": {
        "lang_confirm":  "Language: English\nReply START to begin.",
        "welcome":       "EduTena CBE\nSelect Level:\n1. JSS (Grade 7-9)\n2. Senior (Grade 10-12)",
        "level_err":     "Invalid. Reply 1 for JSS or 2 for Senior.",
        "jss_grade":     "Select JSS Grade:\n1. Grade 7\n2. Grade 8\n3. Grade 9",
        "senior_grade":  "Select Senior Grade:\n1. Grade 10\n2. Grade 11\n3. Grade 12",
        "grade_err":     "Invalid. Select 1, 2, or 3.",
        "term":          "Select Term:\n1. Term 1\n2. Term 2\n3. Term 3",
        "term_err":      "Invalid. Select term 1, 2, or 3.",
        "senior_pathway":"Select your Senior Pathway:\n1. STEM\n2. Social Sciences\n3. Arts & Sports Science",
        "pathway_err":   "Invalid. Select 1, 2, or 3.",
        "invalid":       "Invalid. Reply 1, 2, 3, or 4.",
        "pathway_msg":   "Predicted Pathway: {pathway}\nBased on your Grade 9 scores.\nReply CAREERS to see options.",
        "tracking_hdr":  "Performance: {grade} | {term}\n",
        "suggestion":    "{suggestions}\nYou can also ask any CBE question by texting it!",
        "senior_perf":   "Performance: {grade} | {term}\n{suggestions}",
        "no_pathway":    "Complete assessment first. Reply START.",
        "done":          "Assessment saved. Reply CAREERS or ask any CBE question!",
        "career_saved":  "Career interest saved!",
        "invalid_career":"Invalid. Reply a number from the career list.",
    },
    "sw": {
        "lang_confirm":  "Lugha: Kiswahili\nJibu START kuanza.",
        "welcome":       "EduTena CBE\nChagua Kiwango:\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
        "level_err":     "Batili. Jibu 1 kwa JSS au 2 kwa Sekondari.",
        "jss_grade":     "Chagua Darasa la JSS:\n1. Darasa 7\n2. Darasa 8\n3. Darasa 9",
        "senior_grade":  "Chagua Darasa la Sekondari:\n1. Darasa 10\n2. Darasa 11\n3. Darasa 12",
        "grade_err":     "Batili. Chagua 1, 2, au 3.",
        "term":          "Chagua Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":      "Batili. Chagua muhula 1, 2, au 3.",
        "senior_pathway":"Chagua Njia yako ya Sekondari:\n1. STEM\n2. Sayansi Jamii\n3. Sanaa & Michezo",
        "pathway_err":   "Batili. Chagua 1, 2, au 3.",
        "invalid":       "Batili. Jibu 1, 2, 3, au 4.",
        "pathway_msg":   "Njia Inayotabirika: {pathway}\nJibu CAREERS kuona kazi.",
        "tracking_hdr":  "Utendaji: {grade} | {term}\n",
        "suggestion":    "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "senior_perf":   "Utendaji: {grade} | {term}\n{suggestions}",
        "no_pathway":    "Maliza tathmini kwanza. Jibu START.",
        "done":          "Imehifadhiwa. Jibu CAREERS au uliza swali lolote la CBE!",
        "career_saved":  "Kazi yako imehifadhiwa!",
        "invalid_career":"Batili. Jibu nambari kutoka kwenye orodha ya kazi.",
    },
    "lh": {
        "lang_confirm":  "Olulimi: Luhya\nJibu START okhuandaa.",
        "welcome":       "EduTena CBE\nSena Engufu:\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
        "level_err":     "Busia. Jibu 1 kwa JSS kamba 2 kwa Sekondari.",
        "jss_grade":     "Sena Okhufunda lwa JSS:\n1. Okhufunda 7\n2. Okhufunda 8\n3. Okhufunda 9",
        "senior_grade":  "Sena Okhufunda lwa Sekondari:\n1. Okhufunda 10\n2. Okhufunda 11\n3. Okhufunda 12",
        "grade_err":     "Busia. Sena 1, 2, kamba 3.",
        "term":          "Sena Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":      "Busia. Sena muhula 1, 2, kamba 3.",
        "senior_pathway":"Sena Njia yako ya Sekondari:\n1. STEM\n2. Sayansi Jamii\n3. Sanaa & Michezo",
        "pathway_err":   "Busia. Sena 1, 2, kamba 3.",
        "invalid":       "Busia. Jibu 1, 2, 3, kamba 4.",
        "pathway_msg":   "Njia Enyiseniwe: {pathway}\nJibu CAREERS okhuona emilimo.",
        "tracking_hdr":  "Okusema: {grade} | {term}\n",
        "suggestion":    "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "senior_perf":   "Okusema: {grade} | {term}\n{suggestions}",
        "no_pathway":    "Maliza tathmini kwanza. Jibu START.",
        "done":          "Yakhwira. Jibu CAREERS kamba uliza swali la CBE!",
        "career_saved":  "Emilimo yako imehifadhiwa!",
        "invalid_career":"Busia. Jibu nambari kutoka orodha ya emilimo.",
    },
    "ki": {
        "lang_confirm":  "Rurimi: Kikuyu\nCookia START guthomia.",
        "welcome":       "EduTena CBE\nThura Kiwango:\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)",
        "level_err":     "Ti wegwaru. Cookia 1 JSS kana 2 Sekondari.",
        "jss_grade":     "Thura Kiwango kia JSS:\n1. Kiwango 7\n2. Kiwango 8\n3. Kiwango 9",
        "senior_grade":  "Thura Kiwango kia Sekondari:\n1. Kiwango 10\n2. Kiwango 11\n3. Kiwango 12",
        "grade_err":     "Ti wegwaru. Thura 1, 2, kana 3.",
        "term":          "Thura Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":      "Ti wegwaru. Thura 1, 2, kana 3.",
        "senior_pathway":"Thura Njia yaku ya Sekondari:\n1. STEM\n2. Sayansi Jamii\n3. Sanaa & Michezo",
        "pathway_err":   "Ti wegwaru. Thura 1, 2, kana 3.",
        "invalid":       "Ti wegwaru. Cookia 1, 2, 3, kana 4.",
        "pathway_msg":   "Njia Yoneneirwo: {pathway}\nCookia CAREERS kuona mirimo.",
        "tracking_hdr":  "Mahitio: {grade} | {term}\n",
        "suggestion":    "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "senior_perf":   "Mahitio: {grade} | {term}\n{suggestions}",
        "no_pathway":    "Ithoma mbere. Cookia START.",
        "done":          "Niikuura. Cookia CAREERS kana uiguithia swali la CBE!",
        "career_saved":  "Murimo waku niikuura!",
        "invalid_career":"Ti wegwaru. Cookia nambari kutoka orodha ya mirimo.",
    },
}

# =============================================================
#  SMS DB HELPERS
# =============================================================
# Columns: phone(0) lang(1) level(2) grade(3) term(4) pathway(5)
#          math(6) science(7) social(8) creative(9) technical(10)
#          career_interest(11) state(12)

SMS_ALLOWED = {
    "lang","level","grade","term","pathway",
    "math","science","social","creative","technical",
    "career_interest","state"
}

def sms_save(phone, field, value):
    if field not in SMS_ALLOWED:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit(); cur.close(); conn.close()

def sms_get(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT phone, lang, level, grade, term, pathway,
               math, science, social, creative, technical,
               career_interest, state
        FROM students WHERE phone=%s
    """, (phone,))
    s = cur.fetchone(); cur.close(); conn.close()
    return s

def sms_calculate_pathway(phone):
    s = sms_get(phone)
    if not s: return None
    pathway = calculate_pathway_from_scores(s[6], s[7], s[8], s[9], s[10])
    sms_save(phone, "pathway", pathway)
    return pathway

async def send_reply(to_phone, message):
    try:
        sms_service.send(message=message, recipients=[to_phone], sender_id=SENDER_ID)
        print(f"[SMS] → {to_phone[:7]}****: {message[:80]}")
    except Exception as e:
        print(f"[SMS] failed: {e}")

# =============================================================
#  SMS WEBHOOK
# =============================================================

@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(from_: str = Form(..., alias="from"), text: str = Form(...)):
    phone      = from_
    text_clean = text.strip()
    text_upper = text_clean.upper()
    print(f"[SMS] from {phone[:7]}****: {text_clean}")

    student = sms_get(phone)

    # ── Always restart fresh on START ────────────────────────────
    if text_upper == "START" or not student:
        sms_save(phone, "state", "LANG")
        await send_reply(phone, LANG_SELECT_MSG)
        return ""

    lang  = student[1] if student[1] in SMS_MENU else "en"
    state = student[12]
    M     = SMS_MENU[lang]

    # ── AI Question — anything that looks like a question ────────
    if is_cbe_question(text_clean) and state not in ("LANG","LEVEL","JSS_GRADE",
       "SENIOR_GRADE","TERM","SENIOR_PATHWAY","S_SUBJ1","S_SUBJ2","S_SUBJ3",
       "S_SUBJ4","S_SUBJ5","MATH","SCIENCE","SOCIAL","CREATIVE","TECH"):
        answer = await ask_gemini(phone, text_clean)
        await send_reply(phone, answer)
        return ""

    # ── MORE command — full career list ──────────────────────────
    if text_upper == "MORE":
        pathway = student[5]
        grade   = student[3] or ""
        if not pathway: await send_reply(phone, M["no_pathway"]); return ""
        await send_reply(phone, get_all_careers_sms(pathway, lang))
        sms_save(phone, "state", "CAREER_SELECT_ALL")
        return ""

    # ── CAREERS command ───────────────────────────────────────────
    if text_upper == "CAREERS":
        pathway = student[5]
        grade   = student[3] or ""
        if not pathway: await send_reply(phone, M["no_pathway"]); return ""
        await send_reply(phone, get_career_list_sms(pathway, lang, grade))
        sms_save(phone, "state", "CAREER_SELECT")
        return ""

    try:
        # ── Language selection ────────────────────────────────────
        if state == "LANG":
            chosen = LANG_MAP.get(text_clean)
            if not chosen:
                await send_reply(phone, LANG_SELECT_MSG); return ""
            sms_save(phone, "lang", chosen)
            sms_save(phone, "state", "LEVEL")
            await send_reply(phone, SMS_MENU[chosen]["welcome"])

        # ── Level ─────────────────────────────────────────────────
        elif state == "LEVEL":
            if text_clean == "1":
                sms_save(phone, "level", "JSS")
                sms_save(phone, "state", "JSS_GRADE")
                await send_reply(phone, M["jss_grade"])
            elif text_clean == "2":
                sms_save(phone, "level", "Senior")
                sms_save(phone, "state", "SENIOR_GRADE")
                await send_reply(phone, M["senior_grade"])
            else:
                await send_reply(phone, M["level_err"])

        # ── JSS Grade ─────────────────────────────────────────────
        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(text_clean)
            if not g:
                await send_reply(phone, M["grade_err"]+"\n"+M["jss_grade"]); return ""
            sms_save(phone, "grade", g)
            sms_save(phone, "state", "TERM")
            await send_reply(phone, M["term"])

        # ── Senior Grade ──────────────────────────────────────────
        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(text_clean)
            if not g:
                await send_reply(phone, M["grade_err"]+"\n"+M["senior_grade"]); return ""
            sms_save(phone, "grade", g)
            sms_save(phone, "state", "TERM")
            await send_reply(phone, M["term"])

        # ── Term ──────────────────────────────────────────────────
        elif state == "TERM":
            t = TERMS.get(text_clean)
            if not t:
                await send_reply(phone, M["term_err"]+"\n"+M["term"]); return ""
            sms_save(phone, "term", t)
            # Route: Senior needs pathway first; JSS goes straight to subjects
            if student[2] == "Senior":
                sms_save(phone, "state", "SENIOR_PATHWAY")
                await send_reply(phone, M["senior_pathway"])
            else:
                sms_save(phone, "state", "MATH")
                await send_reply(phone, M["math"] if hasattr(M, "get") else
                    f"Rate Math:\n{RATING_OPTIONS_SMS}")

        # ── Senior Pathway Selection ──────────────────────────────
        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(text_clean)
            if not chosen:
                await send_reply(phone, M["pathway_err"]+"\n"+M["senior_pathway"]); return ""
            sms_save(phone, "pathway", chosen)
            subjects = get_senior_subjects(chosen)
            sms_save(phone, "state", "S_SUBJ1")
            field, label = subjects[0]
            await send_reply(phone, f"Rate {label}:\n{RATING_OPTIONS_SMS}")

        # ── Senior Subject Ratings (S_SUBJ1 to S_SUBJ5) ──────────
        elif state in SENIOR_STATES:
            idx      = SENIOR_STATES.index(state)
            pathway  = student[5]
            subjects = get_senior_subjects(pathway)
            score    = RATING_MAP.get(text_clean)
            if not score:
                field, label = subjects[idx]
                await send_reply(phone, f"Invalid.\nRate {label}:\n{RATING_OPTIONS_SMS}"); return ""
            field, label = subjects[idx]
            sms_save(phone, field, score)

            if idx < 4:
                # Move to next subject
                next_field, next_label = subjects[idx + 1]
                sms_save(phone, "state", SENIOR_STATES[idx + 1])
                await send_reply(phone, f"Rate {next_label}:\n{RATING_OPTIONS_SMS}")
            else:
                # All 5 subjects done — show performance + careers
                sms_save(phone, "state", "CAREER_SELECT")
                s = sms_get(phone)
                grade   = s[3] or ""
                term    = s[4] or ""
                suggestions = get_improvement_suggestions(s[6],s[7],s[8],s[9],s[10],lang)
                # Message 1: performance
                await send_reply(phone, M["senior_perf"].format(
                    grade=grade, term=term, suggestions=suggestions))
                # Message 2: career list with market ratings
                await send_reply(phone, get_career_list_sms(pathway, lang, grade))

        # ── JSS Subject Ratings (MATH→SCIENCE→SOCIAL→CREATIVE→TECH)
        elif state == "MATH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"Invalid.\nRate Math:\n{RATING_OPTIONS_SMS}"); return ""
            sms_save(phone, "math", score)
            sms_save(phone, "state", "SCIENCE")
            await send_reply(phone, f"Rate Science:\n{RATING_OPTIONS_SMS}")

        elif state == "SCIENCE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"Invalid.\nRate Science:\n{RATING_OPTIONS_SMS}"); return ""
            sms_save(phone, "science", score)
            sms_save(phone, "state", "SOCIAL")
            await send_reply(phone, f"Rate Social Studies:\n{RATING_OPTIONS_SMS}")

        elif state == "SOCIAL":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"Invalid.\nRate Social Studies:\n{RATING_OPTIONS_SMS}"); return ""
            sms_save(phone, "social", score)
            sms_save(phone, "state", "CREATIVE")
            await send_reply(phone, f"Rate Creative Arts:\n{RATING_OPTIONS_SMS}")

        elif state == "CREATIVE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"Invalid.\nRate Creative Arts:\n{RATING_OPTIONS_SMS}"); return ""
            sms_save(phone, "creative", score)
            sms_save(phone, "state", "TECH")
            await send_reply(phone, f"Rate Technical Skills:\n{RATING_OPTIONS_SMS}")

        elif state == "TECH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"Invalid.\nRate Technical Skills:\n{RATING_OPTIONS_SMS}"); return ""
            sms_save(phone, "technical", score)
            s     = sms_get(phone)
            grade = s[3] or ""
            term  = s[4] or ""

            if grade == "Grade 9":
                pathway = calculate_pathway_from_scores(s[6],s[7],s[8],s[9],s[10])
                sms_save(phone, "pathway", pathway)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, M["pathway_msg"].format(pathway=pathway))
            else:
                suggestions = get_improvement_suggestions(s[6],s[7],s[8],s[9],s[10],lang)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, M["tracking_hdr"].format(grade=grade,term=term)
                                 + M["suggestion"].format(suggestions=suggestions))

        # ── Career Selection (top 5 list) ─────────────────────────
        elif state == "CAREER_SELECT":
            pathway = student[5]
            if not pathway: await send_reply(phone, M["no_pathway"]); return ""
            if text_clean.isdigit() and 1 <= int(text_clean) <= 5:
                idx    = int(text_clean) - 1
                career = SENIOR_CAREERS[pathway][idx][0]
                sms_save(phone, "career_interest", career)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, M["career_saved"])
                await send_reply(phone, get_career_detail_sms(pathway, idx, lang))
            elif text_upper == "MORE":
                await send_reply(phone, get_all_careers_sms(pathway, lang))
                sms_save(phone, "state", "CAREER_SELECT_ALL")
            else:
                await send_reply(phone, M["invalid_career"])

        # ── Career Selection (all 10 list) ────────────────────────
        elif state == "CAREER_SELECT_ALL":
            pathway = student[5]
            if not pathway: await send_reply(phone, M["no_pathway"]); return ""
            if text_clean.isdigit() and 1 <= int(text_clean) <= 10:
                idx    = int(text_clean) - 1
                career = SENIOR_CAREERS[pathway][idx][0]
                sms_save(phone, "career_interest", career)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, M["career_saved"])
                await send_reply(phone, get_career_detail_sms(pathway, idx, lang))
            else:
                await send_reply(phone, M["invalid_career"])

        else:
            await send_reply(phone, M["done"])

    except Exception as e:
        print(f"[SMS] Error: {e}")
        await send_reply(phone, "Error. Reply START to try again.")

    return ""


# =============================================================
#  USSD DB HELPERS
# =============================================================
# Columns: phone(0) lang(1) level(2) grade(3) term(4) pathway(5)
#          math(6) science(7) social(8) creative(9) technical(10)
#          career_interest(11) state(12)

USSD_ALLOWED = {
    "lang","level","grade","term","pathway",
    "math","science","social","creative","technical",
    "career_interest","state"
}

def ussd_save(phone, field, value):
    if field not in USSD_ALLOWED:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO ussd_students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE ussd_students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit(); cur.close(); conn.close()

def ussd_get(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT phone, lang, level, grade, term, pathway,
               math, science, social, creative, technical,
               career_interest, state
        FROM ussd_students WHERE phone=%s
    """, (phone,))
    s = cur.fetchone(); cur.close(); conn.close()
    return s

def ussd_calculate_pathway(phone):
    s = ussd_get(phone)
    if not s: return None
    pathway = calculate_pathway_from_scores(s[6],s[7],s[8],s[9],s[10])
    ussd_save(phone, "pathway", pathway)
    return pathway

def ussd_reset(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE ussd_students
        SET lang=NULL, level=NULL, grade=NULL, term=NULL, pathway=NULL,
            math=NULL, science=NULL, social=NULL, creative=NULL,
            technical=NULL, career_interest=NULL, state='LANG'
        WHERE phone=%s
    """, (phone,))
    conn.commit(); cur.close(); conn.close()

def con(text):  return f"CON {text}"
def end(text):  return f"END {text}"
def rating_screen(subject): return con(f"Rate {subject}:\n{RATING_OPTIONS_USSD}")
def invalid_rating(s):      return con(f"Invalid.\nRate {s}:\n{RATING_OPTIONS_USSD}")

# =============================================================
#  USSD WEBHOOK
# =============================================================

@app.post("/ussd", response_class=PlainTextResponse)
async def ussd_callback(
    sessionId:   str = Form(...),
    serviceCode: str = Form(...),
    phoneNumber: str = Form(...),
    text:        str = Form(default="")
):
    phone = phoneNumber
    steps = [s.strip() for s in text.split("*")] if text else []
    step  = steps[-1] if steps else ""
    print(f"[USSD] session={sessionId} phone={phone[:7]}**** steps={steps}")

    student = ussd_get(phone)

    if not text or not student:
        ussd_save(phone, "state", "LANG")
        return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")

    state   = student[12]
    lang    = student[1] if student[1] in SMS_MENU else "en"
    pathway = student[5]

    try:
        if state == "LANG":
            chosen = LANG_MAP.get(step)
            if not chosen:
                return con("Invalid.\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            ussd_save(phone, "lang", chosen)
            ussd_save(phone, "state", "LEVEL")
            labels = {"en":"EduTena CBE\n1. JSS (Gr 7-9)\n2. Senior (Gr 10-12)",
                      "sw":"EduTena CBE\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
                      "lh":"EduTena CBE\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
                      "ki":"EduTena CBE\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)"}
            return con(labels.get(chosen, labels["en"]))

        elif state == "LEVEL":
            if step == "1":
                ussd_save(phone, "level", "JSS")
                ussd_save(phone, "state", "JSS_GRADE")
                return con("Select JSS Grade:\n1. Grade 7\n2. Grade 8\n3. Grade 9")
            elif step == "2":
                ussd_save(phone, "level", "Senior")
                ussd_save(phone, "state", "SENIOR_GRADE")
                return con("Select Senior Grade:\n1. Grade 10\n2. Grade 11\n3. Grade 12")
            else:
                return con("Invalid.\n1. JSS (Grade 7-9)\n2. Senior (Grade 10-12)")

        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(step)
            if not g: return con("Invalid.\n1. Grade 7\n2. Grade 8\n3. Grade 9")
            ussd_save(phone, "grade", g)
            ussd_save(phone, "state", "TERM")
            return con("Select Term:\n1. Term 1\n2. Term 2\n3. Term 3")

        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(step)
            if not g: return con("Invalid.\n1. Grade 10\n2. Grade 11\n3. Grade 12")
            ussd_save(phone, "grade", g)
            ussd_save(phone, "state", "TERM")
            return con("Select Term:\n1. Term 1\n2. Term 2\n3. Term 3")

        elif state == "TERM":
            t = TERMS.get(step)
            if not t: return con("Invalid.\n1. Term 1\n2. Term 2\n3. Term 3")
            ussd_save(phone, "term", t)
            if student[2] == "Senior":
                ussd_save(phone, "state", "SENIOR_PATHWAY")
                return con("Select Senior Pathway:\n1. STEM\n2. Social Sciences\n3. Arts & Sports")
            else:
                ussd_save(phone, "state", "MATH")
                return rating_screen("Math")

        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(step)
            if not chosen:
                return con("Invalid.\n1. STEM\n2. Social Sciences\n3. Arts & Sports")
            ussd_save(phone, "pathway", chosen)
            subjects = get_senior_subjects(chosen)
            ussd_save(phone, "state", "S_SUBJ1")
            _, label = subjects[0]
            return rating_screen(label)

        elif state in SENIOR_STATES:
            idx      = SENIOR_STATES.index(state)
            subjects = get_senior_subjects(student[5])
            score    = RATING_MAP.get(step)
            if not score:
                _, label = subjects[idx]
                return invalid_rating(label)
            field, _ = subjects[idx]
            ussd_save(phone, field, score)
            if idx < 4:
                _, next_label = subjects[idx + 1]
                ussd_save(phone, "state", SENIOR_STATES[idx + 1])
                return rating_screen(next_label)
            else:
                # Done — show careers with demand
                pathway = student[5]
                ussd_save(phone, "state", "USSD_CAREER_SELECT")
                career_lines = get_career_ussd_list(pathway)
                return con(career_lines)

        elif state == "MATH":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Math")
            ussd_save(phone, "math", score); ussd_save(phone, "state", "SCIENCE")
            return rating_screen("Science")

        elif state == "SCIENCE":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Science")
            ussd_save(phone, "science", score); ussd_save(phone, "state", "SOCIAL")
            return rating_screen("Social Studies")

        elif state == "SOCIAL":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Social Studies")
            ussd_save(phone, "social", score); ussd_save(phone, "state", "CREATIVE")
            return rating_screen("Creative Arts")

        elif state == "CREATIVE":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Creative Arts")
            ussd_save(phone, "creative", score); ussd_save(phone, "state", "TECH")
            return rating_screen("Technical Skills")

        elif state == "TECH":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Technical Skills")
            ussd_save(phone, "technical", score)
            s     = ussd_get(phone)
            grade = s[3] or ""
            term  = s[4] or ""
            if grade == "Grade 9":
                pathway = calculate_pathway_from_scores(s[6],s[7],s[8],s[9],s[10])
                ussd_save(phone, "pathway", pathway)
                ussd_save(phone, "state", "RESULT")
                return con(f"Predicted Pathway:\n{pathway}\n\n1. View Careers\n2. Restart\n3. Exit")
            else:
                suggestions = get_improvement_suggestions(s[6],s[7],s[8],s[9],s[10],lang)
                short = suggestions[:90]+"..." if len(suggestions)>90 else suggestions
                ussd_save(phone, "state", "DONE")
                return con(f"{grade}|{term}\n{short}\n\n1. Restart\n2. Exit")

        elif state == "USSD_CAREER_SELECT":
            pathway = student[5]
            if step.isdigit() and 1 <= int(step) <= 6:
                idx = int(step) - 1
                career = SENIOR_CAREERS[pathway][idx][0]
                ussd_save(phone, "career_interest", career)
                ussd_save(phone, "state", "DONE")
                _, _, salary, trend, unis, subjects = SENIOR_CAREERS[pathway][idx]
                short_unis = unis[:40]+"..." if len(unis)>40 else unis
                return end(f"Career: {career}\nColleges: {short_unis}\nFocus: {subjects[:40]}\nSaved! SMS START for full details.")
            elif step == "7":
                # Show all careers
                career_lines = get_career_ussd_list(pathway)
                return con(career_lines.replace("6.","6.").replace("7. More careers",""))
            else:
                return con(get_career_ussd_list(pathway))

        elif state == "RESULT":
            pathway = student[5] or ussd_calculate_pathway(phone)
            if step == "1":
                ussd_save(phone, "state", "USSD_CAREER_SELECT")
                return con(get_career_ussd_list(pathway))
            elif step == "2":
                ussd_reset(phone)
                return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            else:
                return end("Thank you for using EduTena CBE. Good luck!")

        elif state == "DONE":
            if step == "1":
                ussd_reset(phone)
                return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            else:
                return end("Thank you for using EduTena CBE. Good luck!")

        else:
            ussd_reset(phone)
            return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")

    except Exception as e:
        print(f"[USSD] Error: {e}")
        return end("Something went wrong. Please dial again.")
