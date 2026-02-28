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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            id SERIAL PRIMARY KEY,
            phone TEXT,
            role TEXT,
            message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

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

# CBE Structure (Competency Based Education):
# JSS    → Grade 7, 8, 9   (Junior Secondary)
# Senior → Grade 10, 11, 12 (Senior Secondary) — NO TERM, pathway-first

RATING_MAP = {"1": 4, "2": 3, "3": 2, "4": 1}

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
#  KENYA LABOUR MARKET 2025 — REDESIGNED CAREER DATA
#  Fields: (name, demand_pct, trend, focus_subjects, universities, cbe_requirements)
#
#  demand_pct  = share of job postings / employer demand in Kenya 2025
#  cbe_requirements = CBE Senior Secondary pathway + recommended competency levels
#  (CBE uses Exceeding/Meeting/Approaching/Below — NOT grades/points like 844)
# =============================================================

SENIOR_CAREERS = {
    "STEM": [
        (
            "Software Engineer",
            "23%",  # demand of tech job postings
            "↑ Silicon Savannah boom",
            "Mathematics, Computer Science, Physics",
            "University of Nairobi, Strathmore University, JKUAT, KU, Moi University",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies (Exceeding/Meeting):\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Computer Science — Exceeding Expectation\n"
                "• Physics — Meeting Expectation\n"
                "Entry: STEM pathway completion + KCSE equivalent competency portfolio\n"
                "Note: Strathmore & KU offer bridging programmes for CBE learners"
            ),
        ),
        (
            "Data Scientist",
            "18%",
            "↑ Highest demand 2025",
            "Mathematics, Statistics, Computer Science",
            "Strathmore University, UoN, JKUAT, African Leadership University",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Computer Science — Meeting Expectation\n"
                "• Science (Applied) — Meeting Expectation\n"
                "Entry: STEM pathway + strong numerical reasoning portfolio\n"
                "Note: ALU uses CBE-aligned competency portfolios for admission"
            ),
        ),
        (
            "Cybersecurity Specialist",
            "12%",
            "↑ Critical shortage",
            "Computer Science, Mathematics, Physics",
            "Strathmore University, KU, JKUAT, Kenya Polytechnic",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Computer Science — Exceeding Expectation\n"
                "• Mathematics — Meeting Expectation\n"
                "• Technical Skills — Exceeding Expectation\n"
                "Entry: STEM pathway + ICT project portfolio\n"
                "Note: TVET cybersecurity diplomas also available post-CBE"
            ),
        ),
        (
            "Renewable Energy Engineer",
            "9%",
            "↑ Green energy boom",
            "Physics, Chemistry, Mathematics, Technical Drawing",
            "UoN, JKUAT, Moi University, Technical University of Kenya",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Physics — Exceeding Expectation\n"
                "• Chemistry — Meeting Expectation\n"
                "• Mathematics — Meeting Expectation\n"
                "Entry: STEM pathway + science project portfolio\n"
                "TUK accepts CBE learners via competency assessment"
            ),
        ),
        (
            "Medical Doctor",
            "11%",
            "↑ Healthcare demand growing",
            "Biology, Chemistry, Physics, Mathematics",
            "University of Nairobi, Moi University, KMTC (clinical officer)",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Biology — Exceeding Expectation\n"
                "• Chemistry — Exceeding Expectation\n"
                "• Physics — Meeting Expectation\n"
                "• Mathematics — Meeting Expectation\n"
                "Entry: STEM pathway + science portfolio + HPEB assessment\n"
                "KMTC: Clinical Officer diploma available post-Grade 12 CBE"
            ),
        ),
        (
            "Civil Engineer",
            "8%",
            "→ Steady, housing demand",
            "Mathematics, Physics, Technical Drawing",
            "UoN, JKUAT, Technical University of Kenya, Moi University",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Physics — Meeting Expectation\n"
                "• Technical Skills — Meeting Expectation\n"
                "Entry: STEM pathway + design/build project portfolio"
            ),
        ),
        (
            "Pharmacist",
            "7%",
            "↑ Pharma sector rising",
            "Chemistry, Biology, Mathematics",
            "UoN School of Pharmacy, KU, Kenyatta University Teaching Hospital",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Chemistry — Exceeding Expectation\n"
                "• Biology — Exceeding Expectation\n"
                "• Mathematics — Meeting Expectation\n"
                "Entry: STEM pathway + science competency portfolio"
            ),
        ),
        (
            "Architect",
            "5%",
            "→ Urban projects growing",
            "Mathematics, Physics, Visual Arts & Design",
            "UoN, TUK, JKUAT, Kenyatta University",
            (
                "CBE Pathway: STEM or Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Physics — Meeting Expectation\n"
                "• Creative/Design — Exceeding Expectation\n"
                "Entry: STEM or Arts pathway + design portfolio"
            ),
        ),
        (
            "Lab Technician",
            "4%",
            "→ Public sector demand",
            "Biology, Chemistry, Physics",
            "KMTC, Kenya Polytechnic, KU, Moi University",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Biology — Meeting Expectation\n"
                "• Chemistry — Meeting Expectation\n"
                "Entry: STEM pathway. KMTC accepts CBE Grade 12 completers\n"
                "TVET diploma also available after CBE"
            ),
        ),
        (
            "ICT Support Specialist",
            "3%",
            "→ Steady countrywide",
            "Computer Science, Mathematics",
            "Kenya Polytechnic, KCA University, Zetech University, TVET Colleges",
            (
                "CBE Pathway: STEM\n"
                "Required Competencies:\n"
                "• Computer Science — Meeting Expectation\n"
                "• Technical Skills — Meeting Expectation\n"
                "Entry: STEM pathway OR TVET ICT diploma post-Grade 12\n"
                "Many employers accept CBE portfolio directly"
            ),
        ),
    ],

    "Social Sciences": [
        (
            "Accountant / Auditor",
            "22%",
            "↑ Most advertised role 2025",
            "Mathematics, Business Studies, Economics",
            "Strathmore University, UoN, KCA University, ACCA Kenya",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Business Studies — Exceeding Expectation\n"
                "• Economics — Meeting Expectation\n"
                "Entry: Social Sciences pathway + ACCA / CPA(K) pathway available\n"
                "KASNEB accepts CBE learners for CPA professional exams"
            ),
        ),
        (
            "Finance Manager",
            "19%",
            "↑ Fintech driving demand",
            "Mathematics, Business Studies, Economics",
            "Strathmore University, UoN, CFA Institute, KCA University",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Business Studies — Exceeding Expectation\n"
                "• Economics — Exceeding Expectation\n"
                "Entry: Social Sciences pathway + financial literacy portfolio\n"
                "CFA Institute: pathway open to CBE university graduates"
            ),
        ),
        (
            "Digital Marketer",
            "17%",
            "↑ 17% of job postings 2025",
            "Business Studies, ICT, Communication & Media",
            "Strathmore University, USIU-Africa, KCA University, Daystar",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Business Studies — Meeting Expectation\n"
                "• ICT / Technical Skills — Meeting Expectation\n"
                "• Creative Arts — Meeting Expectation\n"
                "Entry: Social Sciences pathway + digital portfolio (content, campaigns)\n"
                "Many roles hire on portfolio — university not always required"
            ),
        ),
        (
            "Lawyer / Advocate",
            "11%",
            "↑ Legal services growing",
            "History & Government, CRE/IRE, English/Kiswahili",
            "University of Nairobi, Moi University, KU, Strathmore Law",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• History & Government — Exceeding Expectation\n"
                "• English / Kiswahili — Exceeding Expectation\n"
                "• CRE/IRE — Meeting Expectation\n"
                "Entry: Social Sciences pathway + Kenya School of Law (post-degree)\n"
                "Law degree then advocate training: CBE portfolio accepted"
            ),
        ),
        (
            "Sales Executive",
            "10%",
            "↑ Top 3 most hired role",
            "Business Studies, Communication, Economics",
            "Any university, KISM (Kenya Institute of Sales & Marketing)",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Business Studies — Meeting Expectation\n"
                "• Communication — Meeting Expectation\n"
                "Entry: Grade 12 CBE completion in any pathway\n"
                "KISM offers professional sales diplomas open to CBE completers"
            ),
        ),
        (
            "Human Resource Manager",
            "8%",
            "→ Steady across all sectors",
            "Business Studies, Sociology, Psychology",
            "UoN, KU, Moi University, IHRM Kenya",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Business Studies — Meeting Expectation\n"
                "• Social Studies — Meeting Expectation\n"
                "Entry: Social Sciences pathway + IHRM professional membership\n"
                "IHRM accepts CBE Grade 12 completers for diploma programmes"
            ),
        ),
        (
            "Economist",
            "5%",
            "→ Government & research",
            "Mathematics, Economics, Geography",
            "UoN, Moi University, USIU-Africa, Egerton University",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Mathematics — Exceeding Expectation\n"
                "• Economics — Exceeding Expectation\n"
                "• Geography — Meeting Expectation\n"
                "Entry: Social Sciences pathway + quantitative project portfolio"
            ),
        ),
        (
            "Teacher / Educator",
            "4%",
            "→ High demand, CBC era",
            "Specialisation subject + Education studies",
            "KU, Moi University, Maseno University, Teacher Training Colleges",
            (
                "CBE Pathway: Any pathway\n"
                "Required Competencies:\n"
                "• Specialisation subject — Exceeding Expectation\n"
                "• Communication — Meeting Expectation\n"
                "Entry: Grade 12 CBE completion + KNUT / TSC registration\n"
                "P1 Teacher Training Colleges accept CBE Grade 12 completers"
            ),
        ),
        (
            "Psychologist",
            "3%",
            "↑ Mental health demand rising",
            "Biology, CRE/IRE, Social Studies",
            "UoN, USIU-Africa, KU, Catholic University of Eastern Africa",
            (
                "CBE Pathway: Social Sciences\n"
                "Required Competencies:\n"
                "• Biology — Meeting Expectation\n"
                "• Social Studies — Exceeding Expectation\n"
                "Entry: Social Sciences pathway + counselling volunteer portfolio"
            ),
        ),
        (
            "Journalist / Media",
            "1%",
            "↓ Print declining, digital rising",
            "English/Kiswahili, History, ICT",
            "USIU-Africa, Daystar University, KU, Kenya Institute of Mass Communication",
            (
                "CBE Pathway: Social Sciences or Arts & Sports\n"
                "Required Competencies:\n"
                "• English / Kiswahili — Exceeding Expectation\n"
                "• Creative Arts — Meeting Expectation\n"
                "Entry: Any pathway + strong writing/media portfolio\n"
                "KIMC accepts CBE completers for journalism diploma"
            ),
        ),
    ],

    "Arts & Sports Science": [
        (
            "Graphic Designer / UI-UX",
            "20%",
            "↑ Digital economy boom",
            "Visual Arts, Computer Science, Mathematics",
            "ADMI, Kenyatta University, Limkokwing University, Strathmore",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Visual Arts — Exceeding Expectation\n"
                "• Computer Science — Meeting Expectation\n"
                "• Mathematics — Meeting Expectation\n"
                "Entry: Arts & Sports pathway + design portfolio (mandatory)\n"
                "ADMI uses portfolio-based CBE admission — no points system"
            ),
        ),
        (
            "Film & Content Creator",
            "18%",
            "↑ Social media economy",
            "Drama & Theatre, Visual Arts, ICT",
            "ADMI, AFDA Kenya, Daystar University, KCA University",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Drama & Theatre — Exceeding Expectation\n"
                "• Visual Arts — Meeting Expectation\n"
                "• ICT / Technical Skills — Meeting Expectation\n"
                "Entry: Arts & Sports pathway + video/content portfolio\n"
                "Many creators work independently — portfolio is your entry point"
            ),
        ),
        (
            "Interior Designer",
            "12%",
            "↑ Urban housing boom",
            "Visual Arts, Mathematics, Technical Drawing",
            "ADMI, Technical University of Kenya, Kenyatta University, Limkokwing",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Visual Arts — Exceeding Expectation\n"
                "• Mathematics — Meeting Expectation\n"
                "• Technical Skills — Meeting Expectation\n"
                "Entry: Arts & Sports pathway + design/drawing portfolio"
            ),
        ),
        (
            "Physiotherapist",
            "10%",
            "↑ Sports & healthcare",
            "Physical Education, Biology, Chemistry",
            "UoN, KU, KMTC, Moi University",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Physical Education — Exceeding Expectation\n"
                "• Biology — Meeting Expectation\n"
                "• Chemistry — Meeting Expectation\n"
                "Entry: Arts & Sports pathway + KMTC physiotherapy diploma\n"
                "KMTC accepts CBE Grade 12 completers"
            ),
        ),
        (
            "Sports Coach / Manager",
            "9%",
            "→ Growing, sports academies",
            "Physical Education, Biology, Business Studies",
            "KU, Moi University, Sports Kenya, TVET Sports Colleges",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Physical Education — Exceeding Expectation\n"
                "• Biology — Meeting Expectation\n"
                "Entry: Arts & Sports pathway + coaching/competition portfolio\n"
                "Sports Kenya and TVET accept CBE completers for coach licensing"
            ),
        ),
        (
            "Tourism & Hospitality Manager",
            "8%",
            "↑ Post-COVID recovery",
            "Geography, Business Studies, Home Science",
            "Utalii College, KU, USIU-Africa, Mombasa Polytechnic",
            (
                "CBE Pathway: Arts & Sports Science or Social Sciences\n"
                "Required Competencies:\n"
                "• Business Studies — Meeting Expectation\n"
                "• Geography — Meeting Expectation\n"
                "• Home Science — Meeting Expectation\n"
                "Entry: Any pathway + Utalii College hospitality diploma\n"
                "Utalii accepts CBE Grade 12 completers directly"
            ),
        ),
        (
            "Fashion Designer",
            "7%",
            "→ Niche but growing",
            "Visual Arts, Home Science, Business Studies",
            "Kenya Fashion Institute, ADMI, Kenyatta University",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Visual Arts — Exceeding Expectation\n"
                "• Home Science — Meeting Expectation\n"
                "Entry: Arts & Sports pathway + garment/design portfolio\n"
                "Kenya Fashion Institute uses portfolio-based CBE admission"
            ),
        ),
        (
            "Beauty & Wellness Specialist",
            "6%",
            "↑ TVET sector growing",
            "Home Science, Biology, Chemistry",
            "TVET Colleges, Kenya Beauty School, Moi University",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Home Science — Meeting Expectation\n"
                "• Biology — Meeting Expectation\n"
                "Entry: Grade 12 CBE Arts & Sports completion\n"
                "TVET beauty & wellness diplomas open to all CBE Grade 12 completers"
            ),
        ),
        (
            "Musician / Performer",
            "3%",
            "→ Competitive but growing",
            "Music, Drama & Theatre, Visual Arts",
            "Kenya Conservatoire of Music, Daystar University, KIPPRA",
            (
                "CBE Pathway: Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Music / Drama — Exceeding Expectation\n"
                "• Creative Arts — Exceeding Expectation\n"
                "Entry: Arts & Sports pathway + performance/recording portfolio\n"
                "Kenya Conservatoire uses audition + CBE portfolio for admission"
            ),
        ),
        (
            "Community Development Officer",
            "7%",
            "→ NGO & county government",
            "History, CRE/IRE, Social Studies",
            "UoN, Moi University, Catholic University, KU",
            (
                "CBE Pathway: Social Sciences or Arts & Sports Science\n"
                "Required Competencies:\n"
                "• Social Studies — Meeting Expectation\n"
                "• Communication — Meeting Expectation\n"
                "Entry: Any pathway + community service/volunteer portfolio"
            ),
        ),
    ],
}


def get_career_list_sms(pathway: str, lang: str, grade: str) -> str:
    """Top 5 careers with demand % — student picks 1-5."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    hdr = {
        "en": f"{pathway} Careers | {grade}\nKenya Labour Market 2025\nSelect your interest:\n",
        "sw": f"Kazi za {pathway} | {grade}\nSoko la Kazi Kenya 2025\nChagua hamu yako:\n",
        "lh": f"Emilimo ya {pathway} | {grade}\nSoko Kenya 2025\nSena hamu yako:\n",
        "ki": f"Mirimo ya {pathway} | {grade}\nSoko Kenya 2025\nThura hamu yako:\n",
    }
    msg = hdr.get(lang, hdr["en"])
    for i, (name, demand, trend, subjects, unis, reqs) in enumerate(careers[:5], 1):
        msg += f"{i}. {name}\n   Demand: {demand} | {trend}\n"
    footer = {
        "en": "\nReply 1-5 to select your career\nReply MORE to see all 10",
        "sw": "\nJibu 1-5 kuchagua kazi\nJibu MORE kuona zote 10",
        "lh": "\nJibu 1-5 okhuсena emilimo\nJibu MORE okhuona yote 10",
        "ki": "\nCookia 1-5 guthura mirimo\nCookia MORE kuona yothe 10",
    }
    msg += footer.get(lang, footer["en"])
    return msg


def get_career_detail_sms(pathway: str, career_idx: int, lang: str) -> str:
    """Full career detail: demand %, focus subjects, universities, CBE requirements."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    if career_idx < 0 or career_idx >= len(careers):
        return "Invalid selection."
    name, demand, trend, subjects, unis, reqs = careers[career_idx]

    msgs = {
        "en": (
            f"Career: {name}\n"
            f"Market Demand: {demand} of job postings\n"
            f"Trend: {trend}\n\n"
            f"Focus Subjects:\n{subjects}\n\n"
            f"Universities/Colleges:\n{unis}\n\n"
            f"CBE Entry Requirements:\n{reqs}\n\n"
            f"Saved to your profile!\nReply START to reassess."
        ),
        "sw": (
            f"Kazi: {name}\n"
            f"Mahitaji Sokoni: {demand} ya nafasi za kazi\n"
            f"Mwelekeo: {trend}\n\n"
            f"Masomo ya Kuzingatia:\n{subjects}\n\n"
            f"Vyuo:\n{unis}\n\n"
            f"Mahitaji ya CBE:\n{reqs}\n\n"
            f"Imehifadhiwa!\nJibu START kuanza upya."
        ),
        "lh": (
            f"Emilimo: {name}\n"
            f"Haja Sokoni: {demand}\n"
            f"Mwelekeo: {trend}\n\n"
            f"Masomo:\n{subjects}\n\n"
            f"Vyuo:\n{unis}\n\n"
            f"Mahitaji ya CBE:\n{reqs}\n\n"
            f"Imehifadhiwa!\nJibu START okhuanza."
        ),
        "ki": (
            f"Murimo: {name}\n"
            f"Hitaji: {demand}\n"
            f"Mwelekeo: {trend}\n\n"
            f"Masomo:\n{subjects}\n\n"
            f"Vyuo:\n{unis}\n\n"
            f"Mahitaji ya CBE:\n{reqs}\n\n"
            f"Niikuura!\nCookia START gutomia."
        ),
    }
    return msgs.get(lang, msgs["en"])


def get_all_careers_sms(pathway: str, lang: str) -> str:
    """All 10 careers with demand %."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    hdr = {
        "en": f"All {pathway} Careers\nKenya Market 2025\nSelect your interest:\n",
        "sw": f"Kazi Zote za {pathway}\nSoko Kenya 2025\nChagua:\n",
        "lh": f"Emilimo Yote ya {pathway}\nSoko Kenya 2025\nSena:\n",
        "ki": f"Mirimo Yothe ya {pathway}\nSoko Kenya 2025\nThura:\n",
    }
    msg = hdr.get(lang, hdr["en"])
    for i, (name, demand, trend, subjects, unis, reqs) in enumerate(careers, 1):
        msg += f"{i}. {name} — {demand}\n"
    footer = {
        "en": "\nReply 1-10 to select your career interest.",
        "sw": "\nJibu 1-10 kuchagua kazi yako.",
        "lh": "\nJibu 1-10 okhuсena emilimo yako.",
        "ki": "\nCookia 1-10 guthura murimo waku.",
    }
    msg += footer.get(lang, footer["en"])
    return msg


def get_career_ussd_list(pathway: str) -> str:
    """Compact USSD career list — name + demand % only."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    lines = f"{pathway}\nSelect Career:\n"
    for i, (name, demand, trend, subjects, unis, reqs) in enumerate(careers[:6], 1):
        short = name[:16] if len(name) > 16 else name
        lines += f"{i}. {short} {demand}\n"
    lines += "7. More careers"
    return lines


# =============================================================
#  GEMINI — SHARED BASE CONTEXT
# =============================================================

CBE_KNOWLEDGE = """
You are EduTena, a Kenya CBE (Competency Based Education) assistant.
You help students and parents navigate the CBC/CBE curriculum.
IMPORTANT: This is CBE — NOT the old 844 system. Entry to university
is competency portfolio based, not KCSE points.

CBE Structure:
- JSS: Grade 7, 8, 9 (Junior Secondary)
- Senior Secondary: Grade 10, 11, 12 — pathways: STEM, Social Sciences, Arts & Sports Science
- Performance levels: Exceeding Expectation (4), Meeting Expectation (3),
  Approaching Expectation (2), Below Expectation (1)

RULES:
- SMS only — keep ALL responses under 160 characters per sentence
- Be warm, speak like a trusted Kenyan teacher or older sibling
- Never give medical, legal or financial investment advice
- If asked something unrelated to CBE/CBC, say:
  "I only help with CBE questions. Reply RESUME to continue your assessment."
"""

# =============================================================
#  GEMINI FUNCTION 1 — PERSONALISED CAREER NARRATIVE
#  Triggered after a Senior student picks a career.
#  Uses their grade + pathway + career choice to generate
#  a short, personalised motivational message.
# =============================================================

async def gemini_career_narrative(
    grade: str,
    pathway: str,
    career: str,
    subjects: str,
    demand: str,
    lang: str
) -> str:
    """
    Generate a 2-sentence personalised SMS message after career selection.
    Tells the student WHY this career suits their pathway and what to do next.
    Falls back to a generic message if Gemini is unavailable.
    """
    if not GEMINI_KEY:
        return f"Great choice! Focus on {subjects} and build a strong CBE portfolio to reach your goal of becoming a {career}. You've got this!"

    lang_instruction = {
        "sw": "Respond in Kiswahili.",
        "lh": "Respond in simple Luhya mixed with English.",
        "ki": "Respond in simple Kikuyu mixed with English.",
    }.get(lang, "Respond in English.")

    prompt = (
        f"{CBE_KNOWLEDGE}\n\n"
        f"TASK: Write a SHORT personalised motivational message (max 2 sentences, under 300 characters total) "
        f"for a Kenyan student who just chose their career interest.\n\n"
        f"Student profile:\n"
        f"- Grade: {grade}\n"
        f"- CBE Pathway: {pathway}\n"
        f"- Career chosen: {career}\n"
        f"- Key subjects for this career: {subjects}\n"
        f"- Market demand: {demand} of job postings in Kenya 2025\n\n"
        f"The message should:\n"
        f"1. Affirm their choice with a specific reason tied to their pathway\n"
        f"2. Give ONE concrete next step they can take in school right now\n"
        f"Do NOT repeat the career name more than once. Be warm and Kenyan.\n"
        f"{lang_instruction}\n\n"
        f"Message (2 sentences max):"
    )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"maxOutputTokens": 100, "temperature": 0.7},
                }
            )
            data   = response.json()
            answer = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            return answer
    except Exception as e:
        print(f"[GEMINI career_narrative] Error: {e}")
        return f"Excellent choice! In {grade}, focus hard on {subjects} — these are your foundation for becoming a {career}. Keep going, the Kenya job market needs you!"


# =============================================================
#  GEMINI FUNCTION 2 — SMART JSS IMPROVEMENT SUGGESTIONS
#  Replaces the hardcoded rule-based suggestion for Grade 7 & 8.
#  Generates personalised, encouraging advice based on the
#  specific combination of scores, not just a list of weak subjects.
# =============================================================

SCORE_LABEL = {4: "Exceeding Expectation", 3: "Meeting Expectation",
               2: "Approaching Expectation", 1: "Below Expectation"}

async def gemini_jss_suggestions(
    grade: str,
    term: str,
    math: int,
    science: int,
    social: int,
    creative: int,
    technical: int,
    lang: str
) -> str:
    """
    Generate personalised, actionable improvement advice for a JSS student.
    Falls back to hardcoded logic if Gemini is unavailable.
    """
    # Always compute hardcoded fallback first
    fallback = get_improvement_suggestions(math, science, social, creative, technical, lang)

    if not GEMINI_KEY:
        return fallback

    lang_instruction = {
        "sw": "Respond in Kiswahili.",
        "lh": "Respond in simple Luhya mixed with English.",
        "ki": "Respond in simple Kikuyu mixed with English.",
    }.get(lang, "Respond in English.")

    scores_text = (
        f"Math: {SCORE_LABEL.get(math,'unknown')}\n"
        f"Science: {SCORE_LABEL.get(science,'unknown')}\n"
        f"Social Studies: {SCORE_LABEL.get(social,'unknown')}\n"
        f"Creative Arts: {SCORE_LABEL.get(creative,'unknown')}\n"
        f"Technical Skills: {SCORE_LABEL.get(technical,'unknown')}"
    )

    prompt = (
        f"{CBE_KNOWLEDGE}\n\n"
        f"TASK: Write a SHORT personalised improvement message (max 3 sentences, under 400 characters total) "
        f"for a JSS student who just submitted their self-assessment.\n\n"
        f"Student profile:\n"
        f"- Grade: {grade}, {term}\n"
        f"- CBE Performance Levels:\n{scores_text}\n\n"
        f"The message should:\n"
        f"1. Acknowledge what they are doing well (their strongest subject)\n"
        f"2. Name the 1-2 subjects most needing attention and give ONE specific, actionable tip for each\n"
        f"3. End with a short encouraging phrase\n"
        f"Be specific, warm, and realistic. Do NOT just list subjects — give real advice.\n"
        f"{lang_instruction}\n\n"
        f"Message (3 sentences max):"
    )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"maxOutputTokens": 120, "temperature": 0.6},
                }
            )
            data   = response.json()
            answer = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            return answer
    except Exception as e:
        print(f"[GEMINI jss_suggestions] Error: {e}")
        return fallback


# =============================================================
#  GEMINI FUNCTION 3 — MID-FLOW Q&A WITH PAUSE / RESUME
#  Students can ask a CBE question at ANY point during assessment.
#  The current state is saved as "PAUSED_<original_state>" so the
#  flow resumes exactly where it left off when they reply RESUME.
# =============================================================

async def ask_gemini(phone: str, question: str, context_state: str = "") -> str:
    """
    Answer a free-text CBE question via Gemini with conversation memory.
    Saves the exchange to chat_history for context in follow-up questions.
    """
    if not GEMINI_KEY:
        return "AI assistant not configured. Reply RESUME to continue your assessment."

    history = get_chat_history(phone, limit=6)
    history_text = ""
    for role, msg in history:
        history_text += f"{role.upper()}: {msg}\n"

    # If student is mid-assessment, add context so Gemini can reference it
    flow_context = ""
    if context_state:
        flow_context = f"\nNote: This student is currently in their CBE assessment (step: {context_state}). They can reply RESUME to continue.\n"

    prompt = (
        f"{CBE_KNOWLEDGE}"
        f"{flow_context}\n"
        f"CONVERSATION HISTORY:\n{history_text}\n"
        f"STUDENT QUESTION: {question}\n\n"
        f"Answer in max 3 short sentences suitable for SMS. "
        f"End with: 'Reply RESUME to continue your assessment.' if they are mid-flow, "
        f"or 'Reply START to begin.' if not."
    )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"maxOutputTokens": 160, "temperature": 0.4},
                }
            )
            data   = response.json()
            answer = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            save_chat(phone, "user", question)
            save_chat(phone, "assistant", answer)
            return answer
    except Exception as e:
        print(f"[GEMINI ask] Error: {e}")
        return "Sorry, I could not answer that right now. Reply RESUME to continue your assessment."


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


# ── Mid-flow question detection ───────────────────────────────
# Commands that are NEVER treated as questions — always menu inputs
_MENU_COMMANDS = {
    "START","RESUME","CAREERS","MORE","HELP",
    "1","2","3","4","5","6","7","8","9","10",
    "EN","SW","LH","KI",
}

# States where we must treat input as menu digits, not questions
_STRICT_MENU_STATES = {
    "LANG","LEVEL","JSS_GRADE","SENIOR_GRADE","TERM",
    "SENIOR_PATHWAY","MATH","SCIENCE","SOCIAL","CREATIVE","TECH",
    "CAREER_SELECT","CAREER_SELECT_ALL",
}

def is_cbe_question(text: str, state: str = "") -> bool:
    """
    Returns True if the text looks like a natural language CBE question.
    Now works in ANY state except strict menu states — enabling mid-flow Q&A.
    """
    text_upper = text.strip().upper()
    if text_upper in _MENU_COMMANDS:
        return False
    if state in _STRICT_MENU_STATES:
        return False
    # Anything longer than 3 chars that isn't a plain digit → treat as question
    if len(text.strip()) > 3 and not text.strip().isdigit():
        return True
    return False


def pause_state(phone: str, current_state: str, save_fn):
    """
    Save current state as PAUSED_<state> so RESUME can restore it.
    save_fn is either sms_save or ussd_save.
    """
    save_fn(phone, "state", f"PAUSED_{current_state}")


def get_paused_state(state: str) -> str | None:
    """Extract the original state from a PAUSED_<state> string."""
    if state and state.startswith("PAUSED_"):
        return state[len("PAUSED_"):]
    return None


# ── Resume message per language ──────────────────────────────
RESUME_PROMPTS = {
    "LANG":            {"en": LANG_SELECT_MSG, "sw": LANG_SELECT_MSG, "lh": LANG_SELECT_MSG, "ki": LANG_SELECT_MSG},
    "LEVEL":           {l: SMS_MENU[l]["welcome"] for l in SMS_MENU},
    "JSS_GRADE":       {l: SMS_MENU[l]["jss_grade"] for l in SMS_MENU},
    "SENIOR_GRADE":    {l: SMS_MENU[l]["senior_grade"] for l in SMS_MENU},
    "TERM":            {l: SMS_MENU[l]["term"] for l in SMS_MENU},
    "SENIOR_PATHWAY":  {l: SMS_MENU[l]["senior_pathway"] for l in SMS_MENU},
    "MATH":            {l: f"Rate Math:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "SCIENCE":         {l: f"Rate Science:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "SOCIAL":          {l: f"Rate Social Studies:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "CREATIVE":        {l: f"Rate Creative Arts:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "TECH":            {l: f"Rate Technical Skills:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
}

def get_resume_prompt(original_state: str, lang: str, student) -> str:
    """Return the correct re-prompt message for a given state."""
    mapping = RESUME_PROMPTS.get(original_state)
    if mapping:
        return mapping.get(lang, mapping.get("en", "Reply START to begin."))
    # For CAREER_SELECT we need the pathway
    if original_state == "CAREER_SELECT":
        pathway = student[5] or ""
        grade   = student[3] or ""
        return get_career_list_sms(pathway, lang, grade)
    return "Reply START to begin your assessment."


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
#  IMPROVEMENT SUGGESTIONS (JSS Grade 7, 8)
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
        "lang_confirm":   "Language: English\nReply START to begin.",
        "welcome":        "EduTena CBE\nSelect Level:\n1. JSS (Grade 7-9)\n2. Senior (Grade 10-12)",
        "level_err":      "Invalid. Reply 1 for JSS or 2 for Senior.",
        "jss_grade":      "Select JSS Grade:\n1. Grade 7\n2. Grade 8\n3. Grade 9",
        "senior_grade":   "Select Senior Grade:\n1. Grade 10\n2. Grade 11\n3. Grade 12",
        "grade_err":      "Invalid. Select 1, 2, or 3.",
        "term":           "Select Term:\n1. Term 1\n2. Term 2\n3. Term 3",
        "term_err":       "Invalid. Select term 1, 2, or 3.",
        "senior_pathway": "Select your CBE Pathway:\n1. STEM\n2. Social Sciences\n3. Arts & Sports Science",
        "pathway_err":    "Invalid. Select 1, 2, or 3.",
        "invalid":        "Invalid. Reply 1, 2, 3, or 4.",
        "pathway_msg":    "Predicted Pathway: {pathway}\nBased on your Grade 9 scores.\nReply CAREERS to see options.",
        "tracking_hdr":   "Performance: {grade} | {term}\n",
        "suggestion":     "{suggestions}\nYou can also ask any CBE question by texting it!",
        "no_pathway":     "Complete assessment first. Reply START.",
        "done":           "Assessment saved. Reply CAREERS or ask any CBE question!",
        "career_saved":   "Career interest saved!",
        "invalid_career": "Invalid. Reply a number from the career list.",
    },
    "sw": {
        "lang_confirm":   "Lugha: Kiswahili\nJibu START kuanza.",
        "welcome":        "EduTena CBE\nChagua Kiwango:\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
        "level_err":      "Batili. Jibu 1 kwa JSS au 2 kwa Sekondari.",
        "jss_grade":      "Chagua Darasa la JSS:\n1. Darasa 7\n2. Darasa 8\n3. Darasa 9",
        "senior_grade":   "Chagua Darasa la Sekondari:\n1. Darasa 10\n2. Darasa 11\n3. Darasa 12",
        "grade_err":      "Batili. Chagua 1, 2, au 3.",
        "term":           "Chagua Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":       "Batili. Chagua muhula 1, 2, au 3.",
        "senior_pathway": "Chagua Njia yako ya CBE:\n1. STEM\n2. Sayansi Jamii\n3. Sanaa & Michezo",
        "pathway_err":    "Batili. Chagua 1, 2, au 3.",
        "invalid":        "Batili. Jibu 1, 2, 3, au 4.",
        "pathway_msg":    "Njia Inayotabirika: {pathway}\nJibu CAREERS kuona kazi.",
        "tracking_hdr":   "Utendaji: {grade} | {term}\n",
        "suggestion":     "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "no_pathway":     "Maliza tathmini kwanza. Jibu START.",
        "done":           "Imehifadhiwa. Jibu CAREERS au uliza swali lolote la CBE!",
        "career_saved":   "Kazi yako imehifadhiwa!",
        "invalid_career": "Batili. Jibu nambari kutoka kwenye orodha ya kazi.",
    },
    "lh": {
        "lang_confirm":   "Olulimi: Luhya\nJibu START okhuandaa.",
        "welcome":        "EduTena CBE\nSena Engufu:\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
        "level_err":      "Busia. Jibu 1 kwa JSS kamba 2 kwa Sekondari.",
        "jss_grade":      "Sena Okhufunda lwa JSS:\n1. Okhufunda 7\n2. Okhufunda 8\n3. Okhufunda 9",
        "senior_grade":   "Sena Okhufunda lwa Sekondari:\n1. Okhufunda 10\n2. Okhufunda 11\n3. Okhufunda 12",
        "grade_err":      "Busia. Sena 1, 2, kamba 3.",
        "term":           "Sena Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":       "Busia. Sena muhula 1, 2, kamba 3.",
        "senior_pathway": "Sena Njia yako ya CBE:\n1. STEM\n2. Sayansi Jamii\n3. Sanaa & Michezo",
        "pathway_err":    "Busia. Sena 1, 2, kamba 3.",
        "invalid":        "Busia. Jibu 1, 2, 3, kamba 4.",
        "pathway_msg":    "Njia Enyiseniwe: {pathway}\nJibu CAREERS okhuona emilimo.",
        "tracking_hdr":   "Okusema: {grade} | {term}\n",
        "suggestion":     "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "no_pathway":     "Maliza tathmini kwanza. Jibu START.",
        "done":           "Yakhwira. Jibu CAREERS kamba uliza swali la CBE!",
        "career_saved":   "Emilimo yako imehifadhiwa!",
        "invalid_career": "Busia. Jibu nambari kutoka orodha ya emilimo.",
    },
    "ki": {
        "lang_confirm":   "Rurimi: Kikuyu\nCookia START guthomia.",
        "welcome":        "EduTena CBE\nThura Kiwango:\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)",
        "level_err":      "Ti wegwaru. Cookia 1 JSS kana 2 Sekondari.",
        "jss_grade":      "Thura Kiwango kia JSS:\n1. Kiwango 7\n2. Kiwango 8\n3. Kiwango 9",
        "senior_grade":   "Thura Kiwango kia Sekondari:\n1. Kiwango 10\n2. Kiwango 11\n3. Kiwango 12",
        "grade_err":      "Ti wegwaru. Thura 1, 2, kana 3.",
        "term":           "Thura Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":       "Ti wegwaru. Thura 1, 2, kana 3.",
        "senior_pathway": "Thura Njia yaku ya CBE:\n1. STEM\n2. Sayansi Jamii\n3. Sanaa & Michezo",
        "pathway_err":    "Ti wegwaru. Thura 1, 2, kana 3.",
        "invalid":        "Ti wegwaru. Cookia 1, 2, 3, kana 4.",
        "pathway_msg":    "Njia Yoneneirwo: {pathway}\nCookia CAREERS kuona mirimo.",
        "tracking_hdr":   "Mahitio: {grade} | {term}\n",
        "suggestion":     "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "no_pathway":     "Ithoma mbere. Cookia START.",
        "done":           "Niikuura. Cookia CAREERS kana uiguithia swali la CBE!",
        "career_saved":   "Murimo waku niikuura!",
        "invalid_career": "Ti wegwaru. Cookia nambari kutoka orodha ya mirimo.",
    },
}


# =============================================================
#  SMS DB HELPERS
# =============================================================

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
# Senior flow (NO term):
#   LANG → LEVEL → SENIOR_GRADE → SENIOR_PATHWAY → CAREER_SELECT → (detail)
# JSS flow (with term):
#   LANG → LEVEL → JSS_GRADE → TERM → MATH/SCIENCE/SOCIAL/CREATIVE/TECH → result

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

    # ── GEMINI #3: RESUME — restore paused state ─────────────────
    # Student asked a mid-flow question → state is PAUSED_<original>
    # When they reply RESUME, unpause and re-prompt the original step
    if text_upper == "RESUME":
        original = get_paused_state(state)
        if original:
            sms_save(phone, "state", original)
            prompt_msg = get_resume_prompt(original, lang, student)
            await send_reply(phone, prompt_msg)
        else:
            await send_reply(phone, M.get("done", "Reply START to begin."))
        return ""

    # ── GEMINI #3: Mid-flow question detection ───────────────────
    # If student is PAUSED (already asked a question), keep answering
    # until they say RESUME. Also detect new questions in non-strict states.
    paused_original = get_paused_state(state)
    if paused_original:
        # They are paused — treat any non-RESUME text as a follow-up question
        if is_cbe_question(text_clean, state=""):
            answer = await ask_gemini(phone, text_clean, context_state=paused_original)
            await send_reply(phone, answer)
            return ""
        # If they sent a digit/command while paused, nudge them to RESUME
        await send_reply(phone, f"Still paused. Reply RESUME to continue your assessment, or ask another CBE question.")
        return ""

    # Detect a fresh mid-flow question (in non-strict state)
    if is_cbe_question(text_clean, state=state):
        # Pause the current state, answer the question
        pause_state(phone, state, sms_save)
        answer = await ask_gemini(phone, text_clean, context_state=state)
        await send_reply(phone, answer)
        return ""

    # ── MORE command ──────────────────────────────────────────────
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

        # ── Senior Grade → skip term, go straight to pathway ─────
        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(text_clean)
            if not g:
                await send_reply(phone, M["grade_err"]+"\n"+M["senior_grade"]); return ""
            sms_save(phone, "grade", g)
            sms_save(phone, "state", "SENIOR_PATHWAY")
            await send_reply(phone, M["senior_pathway"])

        # ── Term (JSS only) ───────────────────────────────────────
        elif state == "TERM":
            t = TERMS.get(text_clean)
            if not t:
                await send_reply(phone, M["term_err"]+"\n"+M["term"]); return ""
            sms_save(phone, "term", t)
            sms_save(phone, "state", "MATH")
            await send_reply(phone, f"Rate Math:\n{RATING_OPTIONS_SMS}")

        # ── Senior Pathway → show career list ────────────────────
        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(text_clean)
            if not chosen:
                await send_reply(phone, M["pathway_err"]+"\n"+M["senior_pathway"]); return ""
            sms_save(phone, "pathway", chosen)
            sms_save(phone, "state", "CAREER_SELECT")
            grade = student[3] or ""
            await send_reply(phone, get_career_list_sms(chosen, lang, grade))

        # ── JSS Subject Ratings ───────────────────────────────────
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
                # ── GEMINI #2: AI-personalised JSS improvement suggestions ──
                suggestions = await gemini_jss_suggestions(
                    grade, term,
                    s[6], s[7], s[8], s[9], s[10],
                    lang
                )
                sms_save(phone, "state", "DONE")
                await send_reply(phone, M["tracking_hdr"].format(grade=grade, term=term)
                                 + M["suggestion"].format(suggestions=suggestions))

        # ── Career Selection (top 5 list) ─────────────────────────
        elif state == "CAREER_SELECT":
            pathway = student[5]
            if not pathway: await send_reply(phone, M["no_pathway"]); return ""
            if text_clean.isdigit() and 1 <= int(text_clean) <= 5:
                idx  = int(text_clean) - 1
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][idx]
                sms_save(phone, "career_interest", name)
                sms_save(phone, "state", "DONE")
                # Send static career detail first (fast, no Gemini latency)
                await send_reply(phone, get_career_detail_sms(pathway, idx, lang))
                # ── GEMINI #1: Personalised career narrative (second SMS) ──
                grade = student[3] or ""
                narrative = await gemini_career_narrative(
                    grade, pathway, name, subjects, demand, lang
                )
                await send_reply(phone, narrative)
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
                idx  = int(text_clean) - 1
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][idx]
                sms_save(phone, "career_interest", name)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, get_career_detail_sms(pathway, idx, lang))
                # ── GEMINI #1: Personalised career narrative ──────
                grade = student[3] or ""
                narrative = await gemini_career_narrative(
                    grade, pathway, name, subjects, demand, lang
                )
                await send_reply(phone, narrative)
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
# Senior flow (NO term):
#   LANG → LEVEL → SENIOR_GRADE → SENIOR_PATHWAY → USSD_CAREER_SELECT → detail END
# JSS flow:
#   LANG → LEVEL → JSS_GRADE → TERM → MATH/SCIENCE/SOCIAL/CREATIVE/TECH → result

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
            labels = {
                "en": "EduTena CBE\n1. JSS (Gr 7-9)\n2. Senior (Gr 10-12)",
                "sw": "EduTena CBE\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
                "lh": "EduTena CBE\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
                "ki": "EduTena CBE\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)",
            }
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
            # ↓ No term for Senior — go directly to CBE pathway
            ussd_save(phone, "state", "SENIOR_PATHWAY")
            return con("Select CBE Pathway:\n1. STEM\n2. Social Sciences\n3. Arts & Sports")

        elif state == "TERM":
            t = TERMS.get(step)
            if not t: return con("Invalid.\n1. Term 1\n2. Term 2\n3. Term 3")
            ussd_save(phone, "term", t)
            ussd_save(phone, "state", "MATH")
            return rating_screen("Math")

        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(step)
            if not chosen:
                return con("Invalid.\n1. STEM\n2. Social Sciences\n3. Arts & Sports")
            ussd_save(phone, "pathway", chosen)
            ussd_save(phone, "state", "USSD_CAREER_SELECT")
            return con(get_career_ussd_list(chosen))

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
                return con(f"Predicted CBE Pathway:\n{pathway}\n\n1. View Careers\n2. Restart\n3. Exit")
            else:
                suggestions = get_improvement_suggestions(s[6],s[7],s[8],s[9],s[10],lang)
                short = suggestions[:90]+"..." if len(suggestions)>90 else suggestions
                ussd_save(phone, "state", "DONE")
                return con(f"{grade}|{term}\n{short}\n\n1. Restart\n2. Exit")

        elif state == "USSD_CAREER_SELECT":
            pathway = student[5]
            if step.isdigit() and 1 <= int(step) <= 6:
                idx = int(step) - 1
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][idx]
                ussd_save(phone, "career_interest", name)
                ussd_save(phone, "state", "DONE")
                short_unis = unis[:45]+"..." if len(unis)>45 else unis
                short_subj = subjects[:45]+"..." if len(subjects)>45 else subjects
                # Show demand % + subjects + colleges on USSD END screen
                return end(
                    f"{name}\n"
                    f"Demand: {demand} | {trend}\n\n"
                    f"Focus: {short_subj}\n\n"
                    f"Colleges: {short_unis}\n\n"
                    f"CBE: STEM/Social/Arts pathway\n"
                    f"SMS START for full CBE entry requirements."
                )
            elif step == "7":
                careers = SENIOR_CAREERS.get(pathway, [])
                lines = f"{pathway} — All Careers:\n"
                for i, (name, demand, trend, *_) in enumerate(careers, 1):
                    short = name[:15] if len(name) > 15 else name
                    lines += f"{i}. {short} {demand}\n"
                lines += "\nReply 1-10 to select."
                ussd_save(phone, "state", "USSD_CAREER_SELECT_ALL")
                return con(lines)
            else:
                return con(get_career_ussd_list(pathway))

        elif state == "USSD_CAREER_SELECT_ALL":
            pathway = student[5]
            if step.isdigit() and 1 <= int(step) <= 10:
                idx = int(step) - 1
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][idx]
                ussd_save(phone, "career_interest", name)
                ussd_save(phone, "state", "DONE")
                short_unis = unis[:45]+"..." if len(unis)>45 else unis
                short_subj = subjects[:45]+"..." if len(subjects)>45 else subjects
                return end(
                    f"{name}\n"
                    f"Demand: {demand} | {trend}\n\n"
                    f"Focus: {short_subj}\n\n"
                    f"Colleges: {short_unis}\n\n"
                    f"SMS START for full CBE entry requirements."
                )
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
