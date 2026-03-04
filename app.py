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
            career_interest TEXT, state TEXT, mode TEXT
        )
    """)
    for col in ["lang","grade","term","pathway","career_interest","mode"]:
        cur.execute(f"ALTER TABLE students ADD COLUMN IF NOT EXISTS {col} TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ussd_students (
            phone TEXT PRIMARY KEY, lang TEXT DEFAULT 'en',
            level TEXT, grade TEXT, term TEXT, pathway TEXT,
            math INTEGER, science INTEGER, social INTEGER,
            creative INTEGER, technical INTEGER,
            career_interest TEXT, state TEXT, mode TEXT
        )
    """)
    for col in ["lang","grade","term","pathway","career_interest","mode"]:
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
#  KENYA LABOUR MARKET 2025 — CAREER DATA
# =============================================================

SENIOR_CAREERS = {
    "STEM": [
        (
            "Software Engineer",
            "23%",
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


# =============================================================
#  DYNAMIC CAREER RANKING
#  Ranks careers based on student's actual CBE score profile.
#  Returns career indices sorted by match strength.
# =============================================================

# Subject-to-score mapping for each career (which raw scores matter most)
CAREER_SCORE_WEIGHTS = {
    "STEM": {
        # (math_weight, science_weight, social_weight, creative_weight, technical_weight)
        0: (3, 2, 0, 0, 2),   # Software Engineer
        1: (3, 2, 0, 0, 1),   # Data Scientist
        2: (2, 1, 0, 0, 3),   # Cybersecurity
        3: (2, 3, 0, 0, 2),   # Renewable Energy Engineer
        4: (2, 3, 0, 0, 1),   # Medical Doctor
        5: (3, 2, 0, 0, 2),   # Civil Engineer
        6: (2, 3, 0, 0, 0),   # Pharmacist
        7: (2, 1, 0, 3, 1),   # Architect
        8: (1, 3, 0, 0, 1),   # Lab Technician
        9: (2, 1, 0, 0, 3),   # ICT Support
    },
    "Social Sciences": {
        0: (3, 0, 2, 0, 0),   # Accountant
        1: (3, 0, 2, 0, 0),   # Finance Manager
        2: (1, 0, 2, 2, 1),   # Digital Marketer
        3: (0, 0, 3, 2, 0),   # Lawyer
        4: (1, 0, 2, 1, 0),   # Sales Executive
        5: (1, 0, 3, 0, 0),   # HR Manager
        6: (3, 0, 2, 0, 0),   # Economist
        7: (1, 0, 2, 2, 0),   # Teacher
        8: (1, 2, 2, 0, 0),   # Psychologist
        9: (0, 0, 2, 3, 0),   # Journalist
    },
    "Arts & Sports Science": {
        0: (2, 0, 0, 3, 1),   # Graphic Designer
        1: (0, 0, 0, 3, 2),   # Film & Content
        2: (2, 0, 0, 3, 2),   # Interior Designer
        3: (0, 3, 0, 2, 0),   # Physiotherapist
        4: (0, 2, 0, 3, 0),   # Sports Coach
        5: (0, 1, 2, 2, 0),   # Tourism & Hospitality
        6: (0, 0, 1, 3, 1),   # Fashion Designer
        7: (0, 2, 0, 2, 0),   # Beauty & Wellness
        8: (0, 0, 0, 3, 0),   # Musician
        9: (0, 0, 3, 1, 0),   # Community Development
    },
}


def rank_careers_by_scores(pathway: str, math: int, science: int, social: int,
                            creative: int, technical: int) -> list[int]:
    """
    Returns career indices (0-9) ranked by how well the student's CBE
    scores match each career's required strengths.
    Higher = better match. Used to personalise the USSD career list.
    """
    weights = CAREER_SCORE_WEIGHTS.get(pathway, {})
    scores  = [math or 0, science or 0, social or 0, creative or 0, technical or 0]
    ranked  = []
    for idx, (mw, sw, sow, cw, tw) in weights.items():
        match = (scores[0]*mw + scores[1]*sw + scores[2]*sow +
                 scores[3]*cw + scores[4]*tw)
        ranked.append((idx, match))
    ranked.sort(key=lambda x: -x[1])
    return [idx for idx, _ in ranked]


def get_career_list_sms(pathway: str, lang: str, grade: str,
                         ranked_indices: list[int] | None = None) -> str:
    """
    Top 5 careers with demand % — student picks 1-5.
    If ranked_indices provided, shows personalised ordering with match note.
    """
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    indices = ranked_indices[:5] if ranked_indices else list(range(5))

    hdr = {
        "en": (
            f"{pathway} Careers | {grade}\n"
            f"Kenya Labour Market 2025\n"
            + ("✓ Personalised by your scores\n" if ranked_indices else "")
            + "Select your interest:\n"
        ),
        "sw": (
            f"Kazi za {pathway} | {grade}\n"
            f"Soko la Kazi Kenya 2025\n"
            + ("✓ Imepangwa na alama zako\n" if ranked_indices else "")
            + "Chagua hamu yako:\n"
        ),
        "lh": (
            f"Emilimo ya {pathway} | {grade}\n"
            f"Soko Kenya 2025\n"
            + ("✓ Imepangwa na alama zako\n" if ranked_indices else "")
            + "Sena hamu yako:\n"
        ),
        "ki": (
            f"Mirimo ya {pathway} | {grade}\n"
            f"Soko Kenya 2025\n"
            + ("✓ Yathurirwo ni mbari yaku\n" if ranked_indices else "")
            + "Thura hamu yako:\n"
        ),
    }
    msg = hdr.get(lang, hdr["en"])
    for display_num, career_idx in enumerate(indices, 1):
        name, demand, trend, subjects, unis, reqs = careers[career_idx]
        msg += f"{display_num}. {name}\n   Demand: {demand} | {trend}\n"

    footer = {
        "en": "\nReply 1-5 to select your career\nReply MORE to see all 10",
        "sw": "\nJibu 1-5 kuchagua kazi\nJibu MORE kuona zote 10",
        "lh": "\nJibu 1-5 okhuсena emilimo\nJibu MORE okhuona yote 10",
        "ki": "\nCookia 1-5 guthura mirimo\nCookia MORE kuona yothe 10",
    }
    msg += footer.get(lang, footer["en"])
    return msg


def get_career_detail_sms(pathway: str, career_idx: int, lang: str) -> str:
    """Full career detail — no length limit."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    if career_idx < 0 or career_idx >= len(careers):
        return "Invalid selection."
    name, demand, trend, subjects, unis, reqs = careers[career_idx]

    msgs = {
        "en": (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"CAREER: {name}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Market Demand: {demand} of all Kenyan job postings in 2025\n"
            f"Trend: {trend}\n\n"
            f"📚 Focus Subjects:\n{subjects}\n\n"
            f"🏫 Universities & Colleges:\n{unis}\n\n"
            f"📋 CBE Entry Requirements:\n{reqs}\n\n"
            f"✅ Saved to your profile!\n"
            f"Reply START to reassess or MENU to go back."
        ),
        "sw": (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"KAZI: {name}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Mahitaji Sokoni: {demand} ya nafasi za kazi Kenya 2025\n"
            f"Mwelekeo: {trend}\n\n"
            f"📚 Masomo ya Kuzingatia:\n{subjects}\n\n"
            f"🏫 Vyuo:\n{unis}\n\n"
            f"📋 Mahitaji ya CBE:\n{reqs}\n\n"
            f"✅ Imehifadhiwa!\n"
            f"Jibu START kuanza upya au MENU kurudi."
        ),
        "lh": (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"EMILIMO: {name}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Haja Sokoni: {demand}\n"
            f"Mwelekeo: {trend}\n\n"
            f"📚 Masomo:\n{subjects}\n\n"
            f"🏫 Vyuo:\n{unis}\n\n"
            f"📋 Mahitaji ya CBE:\n{reqs}\n\n"
            f"✅ Imehifadhiwa!\n"
            f"Jibu START okhuanza au MENU kurudi."
        ),
        "ki": (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"MURIMO: {name}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Hitaji: {demand}\n"
            f"Mwelekeo: {trend}\n\n"
            f"📚 Masomo:\n{subjects}\n\n"
            f"🏫 Vyuo:\n{unis}\n\n"
            f"📋 Mahitaji ya CBE:\n{reqs}\n\n"
            f"✅ Niikuura!\n"
            f"Cookia START gutomia au MENU gũthiĩ."
        ),
    }
    return msgs.get(lang, msgs["en"])


def get_all_careers_sms(pathway: str, lang: str,
                         ranked_indices: list[int] | None = None) -> str:
    """All 10 careers with demand % — optionally ranked."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    indices = ranked_indices if ranked_indices else list(range(10))

    hdr = {
        "en": (
            f"All {pathway} Careers\n"
            f"Kenya Market 2025\n"
            + ("✓ Ranked by your strengths\n" if ranked_indices else "")
            + "Select your interest:\n"
        ),
        "sw": f"Kazi Zote za {pathway}\nSoko Kenya 2025\nChagua:\n",
        "lh": f"Emilimo Yote ya {pathway}\nSoko Kenya 2025\nSena:\n",
        "ki": f"Mirimo Yothe ya {pathway}\nSoko Kenya 2025\nThura:\n",
    }
    msg = hdr.get(lang, hdr["en"])
    for display_num, career_idx in enumerate(indices, 1):
        name, demand, trend, subjects, unis, reqs = careers[career_idx]
        msg += f"{display_num}. {name} — {demand}\n"

    footer = {
        "en": "\nReply 1-10 to select your career interest.",
        "sw": "\nJibu 1-10 kuchagua kazi yako.",
        "lh": "\nJibu 1-10 okhuсena emilimo yako.",
        "ki": "\nCookia 1-10 guthura murimo waku.",
    }
    msg += footer.get(lang, footer["en"])
    return msg


def get_career_ussd_list(pathway: str, ranked_indices: list[int] | None = None) -> str:
    """
    Compact USSD career list.
    If ranked_indices provided, shows careers in personalised order.
    """
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    indices = ranked_indices[:6] if ranked_indices else list(range(6))

    label = "✓Best match" if ranked_indices else pathway
    lines = f"{label}\nSelect Career:\n"
    for display_num, career_idx in enumerate(indices, 1):
        name, demand, trend, subjects, unis, reqs = careers[career_idx]
        short = name[:16] if len(name) > 16 else name
        lines += f"{display_num}. {short} {demand}\n"
    lines += "7. More careers"
    return lines


def get_career_ussd_all(pathway: str, ranked_indices: list[int] | None = None) -> str:
    """All 10 careers for USSD — optionally ranked."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    indices = ranked_indices if ranked_indices else list(range(10))
    lines   = f"{pathway} — All:\n"
    for display_num, career_idx in enumerate(indices, 1):
        name, demand, *_ = careers[career_idx]
        short = name[:15] if len(name) > 15 else name
        lines += f"{display_num}. {short} {demand}\n"
    lines += "\nSelect 1-10."
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
- Be warm, speak like a trusted Kenyan teacher or older sibling
- Never give medical, legal or financial investment advice
- If asked something unrelated to CBE/CBC, say:
  "I only help with CBE questions. Reply RESUME to continue your assessment."
- You may write as many sentences as needed to fully answer the question.
  Do NOT truncate your answer — explain thoroughly and clearly.
"""

# =============================================================
#  MODE SELECT
# =============================================================

MODE_SELECT_MSG = {
    "en": (
        "What would you like to do?\n"
        "1. Pathway & Career Guide\n"
        "   (assess your level & explore careers)\n"
        "2. CBE Assistant\n"
        "   (ask questions, homework help)"
    ),
    "sw": (
        "Unataka kufanya nini?\n"
        "1. Mwongozo wa Njia & Kazi\n"
        "   (tathmini kiwango chako)\n"
        "2. Msaidizi wa CBE\n"
        "   (uliza maswali, msaada wa kazi)"
    ),
    "lh": (
        "Okhwenenda khukola nini?\n"
        "1. Mwongozo wa Njia & Emilimo\n"
        "2. Msaidizi wa CBE\n"
        "   (uliza maswali, msaada wa masomo)"
    ),
    "ki": (
        "Ni uria ukenda gukora?\n"
        "1. Mwongozo wa Njia & Mirimo\n"
        "2. Msaidizi wa CBE\n"
        "   (uiguithia maswali, uthuri wa masomo)"
    ),
}

RAG_WELCOME_MSG = {
    "en": (
        "CBE Assistant ready!\n"
        "Ask me anything about:\n"
        "- CBE subjects & pathways\n"
        "- Assignment help\n"
        "- How CBE works\n"
        "- Career questions\n\n"
        "Just type your question.\n"
        "Reply MENU to go back."
    ),
    "sw": (
        "Msaidizi wa CBE yuko tayari!\n"
        "Niulize chochote kuhusu:\n"
        "- Masomo & njia za CBE\n"
        "- Msaada wa kazi za nyumbani\n"
        "- Maswali ya kazi\n\n"
        "Andika swali lako.\n"
        "Jibu MENU kurudi."
    ),
    "lh": (
        "Msaidizi wa CBE yuko tayari!\n"
        "Niulize chochote:\n"
        "- Masomo & njia za CBE\n"
        "- Msaada wa kazi\n\n"
        "Andika swali lako.\n"
        "Jibu MENU kurudi."
    ),
    "ki": (
        "Msaidizi wa CBE arĩ ũhoro!\n"
        "Niiguithia ũũ wowote:\n"
        "- Masomo & njia cia CBE\n"
        "- Uthuri wa ũthuri\n\n"
        "Andika swali riaku.\n"
        "Cookia MENU gũthiĩ."
    ),
}

DOCUMENT_CONTEXT = """
[CBE curriculum document context will be injected here once linked.
 This will include syllabus guides, past papers, and pathway descriptions.]
"""

CBE_ASSISTANT_SYSTEM = """
You are EduTena CBE Assistant — a friendly, knowledgeable tutor for
Kenyan students and parents navigating the Competency Based Education (CBE) system.

You can help with:
- Explaining CBE concepts, competency levels, and how the system works
- Homework and assignment guidance (explain concepts, don't just give answers)
- Subject-specific questions across JSS and Senior Secondary subjects
- Pathway and career exploration
- How to prepare a CBE portfolio for university entry
- Understanding CBE vs the old 844 system
- Advice for parents supporting their children in CBE

DOCUMENT CONTEXT (CBE curriculum materials):
{document_context}

RULES:
- Be warm, speak like a trusted Kenyan teacher
- For homework: guide the student to think, don't just give the answer
- If completely unrelated to CBE/education, politely redirect
- NEVER give medical, legal, or financial investment advice
- Write as many sentences as needed to fully and clearly answer the question.
  Never cut your answer short — students deserve complete, thorough explanations.
"""


# =============================================================
#  GEMINI API CALLER
# =============================================================

async def gemini_call(prompt: str, max_tokens: int, temperature: float, label: str) -> str | None:
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-001:generateContent?key={GEMINI_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
                }
            )
            data = response.json()
            print(f"[{label}] HTTP {response.status_code} | keys: {list(data.keys())}")

            if "error" in data:
                print(f"[{label}] API error: {data['error']}")
                return None

            candidates = data.get("candidates", [])
            if not candidates:
                print(f"[{label}] Empty candidates. Full response: {data}")
                return None

            candidate = candidates[0]
            finish = candidate.get("finishReason", "")
            if finish == "SAFETY":
                print(f"[{label}] Blocked by safety filter")
                return "__SAFETY__"

            return candidate["content"]["parts"][0]["text"].strip()

    except Exception as e:
        print(f"[{label}] Exception {type(e).__name__}: {e}")
        return None


# =============================================================
#  GEMINI FUNCTION 1 — PERSONALISED CAREER NARRATIVE
# =============================================================

async def gemini_career_narrative(
    grade: str,
    pathway: str,
    career: str,
    subjects: str,
    demand: str,
    lang: str
) -> str:
    if not GEMINI_KEY:
        return (
            f"Great choice! Focus on {subjects} and build a strong CBE portfolio "
            f"to reach your goal of becoming a {career}. You've got this!"
        )

    lang_instruction = {
        "sw": "Respond in Kiswahili.",
        "lh": "Respond in simple Luhya mixed with English.",
        "ki": "Respond in simple Kikuyu mixed with English.",
    }.get(lang, "Respond in English.")

    prompt = (
        f"{CBE_KNOWLEDGE}\n\n"
        f"TASK: Write a personalised motivational message for a Kenyan student "
        f"who just chose their career interest. The message should be warm, personal, "
        f"and specific — not generic. Write as many sentences as needed to:\n\n"
        f"1. Affirm their career choice with a specific reason tied to their CBE pathway\n"
        f"2. Explain what this career actually involves day-to-day (give real-world detail)\n"
        f"3. Give 2-3 concrete, actionable next steps they can take right now in school\n"
        f"4. Mention the specific subjects they must focus on and why\n"
        f"5. End with genuine encouragement that mentions the Kenya job market opportunity\n\n"
        f"Student profile:\n"
        f"- Grade: {grade}\n"
        f"- CBE Pathway: {pathway}\n"
        f"- Career chosen: {career}\n"
        f"- Key subjects: {subjects}\n"
        f"- Market demand: {demand} of job postings in Kenya 2025\n\n"
        f"Be warm, Kenyan, and encouraging. Do NOT be generic.\n"
        f"{lang_instruction}\n\n"
        f"Message:"
    )

    answer = await gemini_call(prompt, max_tokens=600, temperature=0.7, label="career_narrative")
    if not answer or answer == "__SAFETY__":
        return (
            f"Excellent choice! In {grade}, focus hard on {subjects} — "
            f"these are your foundation for becoming a {career}. "
            f"With {demand} of Kenya's 2025 job postings in this field, "
            f"the opportunity is real. Keep going!"
        )
    return answer


# =============================================================
#  GEMINI FUNCTION 2 — SMART JSS IMPROVEMENT SUGGESTIONS
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
        f"TASK: Write a detailed, personalised improvement message for a JSS student "
        f"who just submitted their self-assessment. Do NOT be brief — give real, "
        f"actionable advice.\n\n"
        f"The message should:\n"
        f"1. Open by acknowledging their strongest subject specifically and warmly\n"
        f"2. For each subject at Approaching or Below Expectation:\n"
        f"   - Name the subject\n"
        f"   - Explain why it matters in CBE\n"
        f"   - Give 2-3 specific, practical daily study tips\n"
        f"   - Suggest one free resource or study method Kenyan students can access\n"
        f"3. Close with a motivating statement that connects their current grade "
        f"to their future Senior Secondary pathway options\n\n"
        f"Student profile:\n"
        f"- Grade: {grade}, {term}\n"
        f"- CBE Performance Levels:\n{scores_text}\n\n"
        f"Be specific, warm, realistic, and Kenyan. Give real advice, not platitudes.\n"
        f"{lang_instruction}\n\n"
        f"Message:"
    )

    answer = await gemini_call(prompt, max_tokens=800, temperature=0.6, label="jss_suggestions")
    return answer if (answer and answer != "__SAFETY__") else fallback


# =============================================================
#  GEMINI FUNCTION 3 — MID-FLOW Q&A (SMS & USSD)
# =============================================================

async def ask_gemini(phone: str, question: str, context_state: str = "",
                     channel: str = "sms") -> str:
    if not GEMINI_KEY:
        return "AI assistant not configured. Reply RESUME to continue your assessment."

    history = get_chat_history(phone, limit=6)
    history_text = "".join(f"{r.upper()}: {m}\n" for r, m in history)

    flow_context = ""
    if context_state:
        flow_context = (
            f"\nNote: This student is currently mid-assessment "
            f"(step: {context_state}). They can reply RESUME to continue.\n"
        )

    resume_hint = (
        "Reply RESUME to continue your assessment."
        if channel == "sms"
        else "Dial back in and select RESUME to continue."
    )

    prompt = (
        f"{CBE_KNOWLEDGE}"
        f"{flow_context}\n"
        f"CONVERSATION HISTORY:\n{history_text}\n"
        f"STUDENT QUESTION: {question}\n\n"
        f"Answer fully and clearly. Do not truncate your answer — "
        f"if the question needs a paragraph, write a paragraph. "
        f"If it needs a list, write a list. "
        f"End with: '{resume_hint}'"
    )

    answer = await gemini_call(prompt, max_tokens=800, temperature=0.4, label="ask_gemini")
    if not answer or answer == "__SAFETY__":
        return f"Sorry, I could not answer that right now. {resume_hint}"
    save_chat(phone, "user", question)
    save_chat(phone, "assistant", answer)
    return answer


# =============================================================
#  GEMINI FUNCTION 4 — RAG CHAT (Mode 2, SMS & USSD via SMS)
# =============================================================

async def ask_gemini_rag(phone: str, question: str, lang: str) -> str:
    if not GEMINI_KEY:
        return "CBE Assistant is not fully configured yet. Reply MENU to go back."

    history = get_chat_history(phone, limit=8)
    history_text = "".join(f"{r.upper()}: {m}\n" for r, m in history)

    lang_instruction = {
        "sw": "Respond in Kiswahili.",
        "lh": "Respond in simple Luhya mixed with English.",
        "ki": "Respond in simple Kikuyu mixed with English.",
    }.get(lang, "Respond in English.")

    doc_section = (
        f"\nREFERENCE DOCUMENTS:\n{DOCUMENT_CONTEXT}\n"
        if DOCUMENT_CONTEXT.strip() and not DOCUMENT_CONTEXT.strip().startswith("[")
        else "(No documents linked yet — use your own knowledge about Kenya CBE/CBC curriculum.)"
    )
    system = CBE_ASSISTANT_SYSTEM.format(document_context=doc_section)

    prompt = (
        f"{system}\n\n"
        f"{lang_instruction}\n\n"
        f"CONVERSATION HISTORY:\n{history_text}\n"
        f"STUDENT: {question}\n\n"
        f"EDUTENA — answer fully and thoroughly. Do NOT cut your response short. "
        f"If the question requires explanation, explain fully. "
        f"If it requires steps, list all steps. "
        f"End with encouragement and remind them they can reply MENU to return to the main menu."
    )

    answer = await gemini_call(prompt, max_tokens=1500, temperature=0.5, label="rag_chat")

    if answer is None:
        return "Sorry, I could not answer that right now. Try again or reply MENU to go back."
    if answer == "__SAFETY__":
        return "I couldn't answer that safely. Please rephrase your question. Reply MENU to go back."

    save_chat(phone, "user", question)
    save_chat(phone, "assistant", answer)
    if "MENU" not in answer.upper():
        answer += "\n\nReply MENU to return to the main menu."
    return answer


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


# =============================================================
#  MID-FLOW QUESTION DETECTION
# =============================================================

_MENU_COMMANDS = {
    "START","RESUME","CAREERS","MORE","HELP","MENU",
    "1","2","3","4","5","6","7","8","9","10",
    "EN","SW","LH","KI",
}

_STRICT_MENU_STATES = {
    "LANG","LEVEL","JSS_GRADE","SENIOR_GRADE","TERM",
    "SENIOR_PATHWAY","MATH","SCIENCE","SOCIAL","CREATIVE","TECH",
    "CAREER_SELECT","CAREER_SELECT_ALL",
    "MODE_SELECT",
}


def is_cbe_question(text: str, state: str = "") -> bool:
    text_upper = text.strip().upper()
    if text_upper in _MENU_COMMANDS:
        return False
    if state in _STRICT_MENU_STATES:
        return False
    if len(text.strip()) > 3 and not text.strip().isdigit():
        return True
    return False


def pause_state(phone: str, current_state: str, save_fn):
    save_fn(phone, "state", f"PAUSED_{current_state}")


def get_paused_state(state: str) -> str | None:
    if state and state.startswith("PAUSED_"):
        return state[len("PAUSED_"):]
    return None


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
#  IMPROVEMENT SUGGESTIONS — FALLBACK (JSS Grade 7, 8)
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
        "mode_err":       "Invalid. Reply 1 for Pathway & Careers or 2 for CBE Assistant.",
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
        "mode_err":       "Batili. Jibu 1 kwa Mwongozo wa Kazi au 2 kwa Msaidizi wa CBE.",
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
        "mode_err":       "Busia. Jibu 1 kwa Mwongozo kamba 2 kwa Msaidizi wa CBE.",
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
        "mode_err":       "Ti wegwaru. Cookia 1 kwa Mwongozo kana 2 kwa Msaidizi wa CBE.",
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

RESUME_PROMPTS = {
    "LANG":           {"en": LANG_SELECT_MSG, "sw": LANG_SELECT_MSG, "lh": LANG_SELECT_MSG, "ki": LANG_SELECT_MSG},
    "LEVEL":          {l: SMS_MENU[l]["welcome"] for l in SMS_MENU},
    "JSS_GRADE":      {l: SMS_MENU[l]["jss_grade"] for l in SMS_MENU},
    "SENIOR_GRADE":   {l: SMS_MENU[l]["senior_grade"] for l in SMS_MENU},
    "TERM":           {l: SMS_MENU[l]["term"] for l in SMS_MENU},
    "SENIOR_PATHWAY": {l: SMS_MENU[l]["senior_pathway"] for l in SMS_MENU},
    "MATH":           {l: f"Rate Math:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "SCIENCE":        {l: f"Rate Science:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "SOCIAL":         {l: f"Rate Social Studies:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "CREATIVE":       {l: f"Rate Creative Arts:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
    "TECH":           {l: f"Rate Technical Skills:\n{RATING_OPTIONS_SMS}" for l in SMS_MENU},
}


def get_resume_prompt(original_state: str, lang: str, student) -> str:
    mapping = RESUME_PROMPTS.get(original_state)
    if mapping:
        return mapping.get(lang, mapping.get("en", "Reply START to begin."))
    if original_state == "CAREER_SELECT":
        pathway = student[5] or ""
        grade   = student[3] or ""
        return get_career_list_sms(pathway, lang, grade)
    return "Reply START to begin your assessment."


# =============================================================
#  SMS DB HELPERS
# =============================================================

SMS_ALLOWED = {
    "lang","level","grade","term","pathway",
    "math","science","social","creative","technical",
    "career_interest","state","mode"
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
               career_interest, state, mode
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
        print(f"[SMS] → {to_phone[:7]}****: {message[:120]}")
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

    if text_upper == "START" or not student:
        sms_save(phone, "state", "LANG")
        sms_save(phone, "mode", "")
        await send_reply(phone, LANG_SELECT_MSG)
        return ""

    lang  = student[1] if student[1] in SMS_MENU else "en"
    state = student[12]
    mode  = student[13] or ""
    M     = SMS_MENU[lang]

    if text_upper == "MENU":
        sms_save(phone, "state", "MODE_SELECT")
        sms_save(phone, "mode", "")
        await send_reply(phone, MODE_SELECT_MSG.get(lang, MODE_SELECT_MSG["en"]))
        return ""

    # ── MODE 2: RAG CHAT ────────────────────────────────────────
    if state == "RAG_CHAT" or mode == "rag":
        if state != "RAG_CHAT":
            sms_save(phone, "state", "RAG_CHAT")
        answer = await ask_gemini_rag(phone, text_clean, lang)
        await send_reply(phone, answer)
        return ""

    # ── RESUME ──────────────────────────────────────────────────
    if text_upper == "RESUME":
        original = get_paused_state(state)
        if original:
            sms_save(phone, "state", original)
            prompt_msg = get_resume_prompt(original, lang, student)
            await send_reply(phone, prompt_msg)
        else:
            await send_reply(phone, M.get("done", "Reply START to begin."))
        return ""

    # ── Mid-assessment pause logic ───────────────────────────────
    paused_original = get_paused_state(state)
    if paused_original:
        if is_cbe_question(text_clean, state=""):
            answer = await ask_gemini(phone, text_clean, context_state=paused_original, channel="sms")
            await send_reply(phone, answer)
            return ""
        await send_reply(phone, "Still paused. Reply RESUME to continue your assessment.")
        return ""

    if is_cbe_question(text_clean, state=state):
        pause_state(phone, state, sms_save)
        answer = await ask_gemini(phone, text_clean, context_state=state, channel="sms")
        await send_reply(phone, answer)
        return ""

    # ── MORE command ─────────────────────────────────────────────
    if text_upper == "MORE":
        pathway = student[5]
        grade   = student[3] or ""
        if not pathway: await send_reply(phone, M["no_pathway"]); return ""
        # Use score-ranked ordering if scores available
        ranked = None
        if all(student[6:11]):
            ranked = rank_careers_by_scores(pathway, *student[6:11])
        await send_reply(phone, get_all_careers_sms(pathway, lang, ranked))
        sms_save(phone, "state", "CAREER_SELECT_ALL")
        return ""

    # ── CAREERS command ──────────────────────────────────────────
    if text_upper == "CAREERS":
        pathway = student[5]
        grade   = student[3] or ""
        if not pathway: await send_reply(phone, M["no_pathway"]); return ""
        ranked = None
        if all(student[6:11]):
            ranked = rank_careers_by_scores(pathway, *student[6:11])
        await send_reply(phone, get_career_list_sms(pathway, lang, grade, ranked))
        sms_save(phone, "state", "CAREER_SELECT")
        return ""

    try:
        # ── Language selection ────────────────────────────────────
        if state == "LANG":
            chosen = LANG_MAP.get(text_clean)
            if not chosen:
                await send_reply(phone, LANG_SELECT_MSG); return ""
            sms_save(phone, "lang", chosen)
            sms_save(phone, "state", "MODE_SELECT")
            await send_reply(phone, MODE_SELECT_MSG.get(chosen, MODE_SELECT_MSG["en"]))

        # ── Mode selection ────────────────────────────────────────
        elif state == "MODE_SELECT":
            if text_clean == "1":
                sms_save(phone, "mode", "assessment")
                sms_save(phone, "state", "LEVEL")
                await send_reply(phone, M["welcome"])
            elif text_clean == "2":
                sms_save(phone, "mode", "rag")
                sms_save(phone, "state", "RAG_CHAT")
                await send_reply(phone, RAG_WELCOME_MSG.get(lang, RAG_WELCOME_MSG["en"]))
            else:
                await send_reply(phone, M["mode_err"])

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

        # ── Senior Grade → pathway ────────────────────────────────
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

        # ── Senior Pathway → career list ──────────────────────────
        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(text_clean)
            if not chosen:
                await send_reply(phone, M["pathway_err"]+"\n"+M["senior_pathway"]); return ""
            sms_save(phone, "pathway", chosen)
            sms_save(phone, "state", "CAREER_SELECT")
            grade = student[3] or ""
            # No scores yet for senior at pathway selection, show standard list
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
                # Gemini JSS suggestions — full, unlimited response
                suggestions = await gemini_jss_suggestions(
                    grade, term, s[6], s[7], s[8], s[9], s[10], lang
                )
                sms_save(phone, "state", "DONE")
                await send_reply(phone,
                    M["tracking_hdr"].format(grade=grade, term=term)
                    + M["suggestion"].format(suggestions=suggestions)
                )

        # ── Career Selection (top 5, ranked) ──────────────────────
        elif state == "CAREER_SELECT":
            pathway = student[5]
            if not pathway: await send_reply(phone, M["no_pathway"]); return ""

            # Reconstruct the ranked ordering to map display number → actual career index
            ranked = None
            if all(student[6:11]):
                ranked = rank_careers_by_scores(pathway, *student[6:11])
            indices = ranked[:5] if ranked else list(range(5))

            if text_clean.isdigit() and 1 <= int(text_clean) <= 5:
                display_pick = int(text_clean) - 1
                career_idx   = indices[display_pick]
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][career_idx]
                sms_save(phone, "career_interest", name)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, get_career_detail_sms(pathway, career_idx, lang))
                grade     = student[3] or ""
                narrative = await gemini_career_narrative(grade, pathway, name, subjects, demand, lang)
                await send_reply(phone, narrative)
            elif text_upper == "MORE":
                ranked_all = None
                if all(student[6:11]):
                    ranked_all = rank_careers_by_scores(pathway, *student[6:11])
                await send_reply(phone, get_all_careers_sms(pathway, lang, ranked_all))
                sms_save(phone, "state", "CAREER_SELECT_ALL")
            else:
                await send_reply(phone, M["invalid_career"])

        # ── Career Selection (all 10, ranked) ─────────────────────
        elif state == "CAREER_SELECT_ALL":
            pathway = student[5]
            if not pathway: await send_reply(phone, M["no_pathway"]); return ""

            ranked = None
            if all(student[6:11]):
                ranked = rank_careers_by_scores(pathway, *student[6:11])
            indices = ranked if ranked else list(range(10))

            if text_clean.isdigit() and 1 <= int(text_clean) <= 10:
                display_pick = int(text_clean) - 1
                career_idx   = indices[display_pick]
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][career_idx]
                sms_save(phone, "career_interest", name)
                sms_save(phone, "state", "DONE")
                await send_reply(phone, get_career_detail_sms(pathway, career_idx, lang))
                grade     = student[3] or ""
                narrative = await gemini_career_narrative(grade, pathway, name, subjects, demand, lang)
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
    "career_interest","state","mode"
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
               career_interest, state, mode
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
            technical=NULL, career_interest=NULL, mode=NULL, state='LANG'
        WHERE phone=%s
    """, (phone,))
    conn.commit(); cur.close(); conn.close()


def con(text):  return f"CON {text}"
def end(text):  return f"END {text}"
def rating_screen(subject): return con(f"Rate {subject}:\n{RATING_OPTIONS_USSD}")
def invalid_rating(s):      return con(f"Invalid.\nRate {s}:\n{RATING_OPTIONS_USSD}")


# =============================================================
#  USSD HELPERS — Dynamic screen builders
# =============================================================

def ussd_pathway_context_msg(pathway: str, grade: str) -> str:
    """
    Dynamic contextual header for the career selection screen.
    Shows which pathway the student is in + current grade.
    """
    pathway_emojis = {
        "STEM": "🔬",
        "Social Sciences": "📊",
        "Arts & Sports Science": "🎨",
    }
    emoji = pathway_emojis.get(pathway, "📚")
    return f"{emoji} {pathway}\n{grade} Career Guide"


def ussd_score_summary(math, science, social, creative, technical) -> str:
    """
    One-line score summary to show at top of result screens.
    e.g. "M:4 Sc:3 So:2 Cr:4 T:3"
    """
    label = {4:"E", 3:"M", 2:"A", 1:"B"}
    return (
        f"M:{label.get(math or 0,'?')} "
        f"Sc:{label.get(science or 0,'?')} "
        f"So:{label.get(social or 0,'?')} "
        f"Cr:{label.get(creative or 0,'?')} "
        f"T:{label.get(technical or 0,'?')}"
    )


def ussd_dynamic_pathway_screen(pathway: str, math, science, social, creative, technical) -> str:
    """
    After Grade 9 pathway prediction — shows the pathway result
    WITH a brief explanation of why this pathway was suggested
    based on their actual strongest subjects.
    """
    scores = {
        "Math": math or 0, "Science": science or 0, "Social": social or 0,
        "Creative": creative or 0, "Technical": technical or 0
    }
    top2 = sorted(scores.items(), key=lambda x: -x[1])[:2]
    top_names = " & ".join(n for n, _ in top2)
    summary = ussd_score_summary(math, science, social, creative, technical)

    return con(
        f"CBE Pathway: {pathway}\n"
        f"Strongest: {top_names}\n"
        f"Scores: {summary}\n\n"
        f"1. View Matched Careers\n"
        f"2. See All Careers\n"
        f"3. Restart\n"
        f"4. Exit"
    )


def ussd_dynamic_jss_result(grade: str, term: str, math, science, social, creative, technical) -> str:
    """
    JSS result screen (Grade 7 & 8).
    Shows strongest and weakest subject dynamically so the student
    immediately sees what to work on — without needing Gemini.
    Full Gemini advice is sent via SMS.
    """
    scores = {
        "Math": math or 0, "Science": science or 0,
        "Social Studies": social or 0, "Creative": creative or 0, "Technical": technical or 0
    }
    sorted_scores = sorted(scores.items(), key=lambda x: -x[1])
    strongest = sorted_scores[0][0]
    weakest   = [n for n, v in sorted_scores if v <= 2]
    weak_str  = ", ".join(weakest[:2]) if weakest else "None"

    summary = ussd_score_summary(math, science, social, creative, technical)

    return con(
        f"{grade} | {term}\n"
        f"Scores: {summary}\n"
        f"Best: {strongest}\n"
        f"Focus on: {weak_str}\n\n"
        f"📱 Full advice sent by SMS!\n\n"
        f"1. Restart\n"
        f"2. Exit"
    )


def ussd_dynamic_career_result(name: str, demand: str, trend: str,
                                subjects: str, unis: str) -> str:
    """
    Career END screen for USSD.
    Shows the key info dynamically — full details sent via SMS.
    """
    # Shorten subjects to first 2 for USSD screen
    subj_list = subjects.split(",")
    short_subj = ", ".join(s.strip() for s in subj_list[:2])
    if len(subj_list) > 2:
        short_subj += f" +{len(subj_list)-2} more"

    # Shorten universities to first institution
    first_uni = unis.split(",")[0].strip()

    return end(
        f"✅ Career Saved!\n\n"
        f"📌 {name}\n"
        f"Demand: {demand} | {trend}\n\n"
        f"📚 Key subjects: {short_subj}\n"
        f"🏫 e.g. {first_uni}\n\n"
        f"📱 Full CBE requirements,\n"
        f"university list & personal\n"
        f"advice sent via SMS!"
    )


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
        return con(
            "Welcome to EduTena CBE\n"
            "Select Language:\n"
            "1. English\n"
            "2. Swahili\n"
            "3. Luhya\n"
            "4. Kikuyu"
        )

    state   = student[12]
    lang    = student[1] if student[1] in SMS_MENU else "en"
    pathway = student[5]

    try:
        # ── Language ──────────────────────────────────────────────
        if state == "LANG":
            chosen = LANG_MAP.get(step)
            if not chosen:
                return con("Invalid.\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            ussd_save(phone, "lang", chosen)
            ussd_save(phone, "state", "MODE_SELECT")
            return con(
                "EduTena CBE\nWhat would you like?\n"
                "1. Pathway & Career Guide\n"
                "2. CBE Assistant\n"
                "   (Ask any CBE question —\n"
                "    answer sent via SMS)"
            )

        # ── Mode select ───────────────────────────────────────────
        elif state == "MODE_SELECT":
            if step == "1":
                ussd_save(phone, "mode", "assessment")
                ussd_save(phone, "state", "LEVEL")
                labels = {
                    "en": "EduTena CBE\n1. JSS (Gr 7-9)\n2. Senior (Gr 10-12)",
                    "sw": "EduTena CBE\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
                    "lh": "EduTena CBE\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
                    "ki": "EduTena CBE\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)",
                }
                return con(labels.get(lang, labels["en"]))
            elif step == "2":
                # ── USSD RAG CHAT ─────────────────────────────────
                # USSD can't receive free text, so we show a menu of
                # common topics. The student picks one and we send the
                # Gemini answer via SMS.
                ussd_save(phone, "mode", "rag")
                ussd_save(phone, "state", "USSD_RAG_TOPIC")
                return con(
                    "CBE Assistant\nSend answer via SMS\n\n"
                    "Pick a topic:\n"
                    "1. What is CBE/CBC?\n"
                    "2. Pathway options explained\n"
                    "3. How to build a portfolio\n"
                    "4. CBE vs old 844 system\n"
                    "5. University entry with CBE\n"
                    "6. Ask your own question (SMS)"
                )
            else:
                return con("Invalid.\n1. Pathway & Career Guide\n2. CBE Assistant")

        # ── USSD RAG TOPIC PICKER ─────────────────────────────────
        elif state == "USSD_RAG_TOPIC":
            topic_questions = {
                "1": "Can you explain what CBE (Competency Based Education) and CBC (Competency Based Curriculum) mean in Kenya? How is it different from what came before?",
                "2": "Can you explain the three Senior Secondary CBE pathways — STEM, Social Sciences, and Arts & Sports Science — in detail? What subjects does each contain and what kinds of careers does each lead to?",
                "3": "How does a student build a CBE competency portfolio for university entry? What should it include and how is it assessed?",
                "4": "What is the difference between the old 844 KCSE system and the new CBE system in Kenya? How does university entry work now?",
                "5": "How do Kenyan universities and colleges admit students under CBE? Which institutions accept CBE portfolios and how does the process work?",
            }
            if step in topic_questions:
                question = topic_questions[step]
                ussd_save(phone, "state", "DONE")
                # Send Gemini answer via SMS — no length limit
                asyncio.create_task(
                    _send_rag_answer_via_sms(phone, question, lang)
                )
                return end(
                    f"✅ Great choice!\n\n"
                    f"Your answer is being prepared\n"
                    f"and will arrive via SMS\n"
                    f"in the next 30 seconds.\n\n"
                    f"Dial back anytime to explore\n"
                    f"more CBE topics!"
                )
            elif step == "6":
                ussd_save(phone, "state", "DONE")
                return end(
                    "To ask your own question:\n\n"
                    f"Text START to this number,\n"
                    f"select option 2 (CBE Assistant),\n"
                    f"then type any question.\n\n"
                    f"Our AI will answer fully\n"
                    f"with no limit on length!"
                )
            else:
                return con(
                    "CBE Assistant Topics:\n"
                    "1. What is CBE/CBC?\n"
                    "2. Pathway options\n"
                    "3. Build a portfolio\n"
                    "4. CBE vs 844\n"
                    "5. University entry\n"
                    "6. Ask own question (SMS)"
                )

        # ── Level ─────────────────────────────────────────────────
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

        # ── JSS Grade ─────────────────────────────────────────────
        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(step)
            if not g: return con("Invalid.\n1. Grade 7\n2. Grade 8\n3. Grade 9")
            ussd_save(phone, "grade", g)
            ussd_save(phone, "state", "TERM")
            return con("Select Term:\n1. Term 1\n2. Term 2\n3. Term 3")

        # ── Senior Grade → pathway ────────────────────────────────
        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(step)
            if not g: return con("Invalid.\n1. Grade 10\n2. Grade 11\n3. Grade 12")
            ussd_save(phone, "grade", g)
            ussd_save(phone, "state", "SENIOR_PATHWAY")
            # Dynamic: show grade in pathway screen
            grade_label = g
            return con(
                f"Senior CBE | {grade_label}\n"
                f"Select your Pathway:\n"
                f"1. STEM\n"
                f"   (Science, Tech, Math)\n"
                f"2. Social Sciences\n"
                f"   (Business, Law, Economics)\n"
                f"3. Arts & Sports\n"
                f"   (Creative, PE, Media)"
            )

        # ── Term (JSS only) ───────────────────────────────────────
        elif state == "TERM":
            t = TERMS.get(step)
            if not t: return con("Invalid.\n1. Term 1\n2. Term 2\n3. Term 3")
            ussd_save(phone, "term", t)
            ussd_save(phone, "state", "MATH")
            grade = student[3] or "JSS"
            return con(
                f"Self-Assessment\n{grade} | {t}\n\n"
                f"Rate Math:\n{RATING_OPTIONS_USSD}"
            )

        # ── Senior Pathway → career list ──────────────────────────
        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(step)
            if not chosen:
                return con("Invalid.\n1. STEM\n2. Social Sciences\n3. Arts & Sports")
            ussd_save(phone, "pathway", chosen)
            ussd_save(phone, "state", "USSD_CAREER_SELECT")
            grade = student[3] or ""
            # No scores for senior students, show standard list
            return con(get_career_ussd_list(chosen))

        # ── JSS Subject Ratings ───────────────────────────────────
        elif state == "MATH":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Math")
            ussd_save(phone, "math", score)
            ussd_save(phone, "state", "SCIENCE")
            return rating_screen("Science")

        elif state == "SCIENCE":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Science")
            ussd_save(phone, "science", score)
            ussd_save(phone, "state", "SOCIAL")
            return rating_screen("Social Studies")

        elif state == "SOCIAL":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Social Studies")
            ussd_save(phone, "social", score)
            ussd_save(phone, "state", "CREATIVE")
            return rating_screen("Creative Arts")

        elif state == "CREATIVE":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Creative Arts")
            ussd_save(phone, "creative", score)
            ussd_save(phone, "state", "TECH")
            return rating_screen("Technical Skills")

        elif state == "TECH":
            score = RATING_MAP.get(step)
            if not score: return invalid_rating("Technical Skills")
            ussd_save(phone, "technical", score)
            s     = ussd_get(phone)
            grade = s[3] or ""
            term  = s[4] or ""
            math_, sci_, soc_, cre_, tec_ = s[6], s[7], s[8], s[9], s[10]

            if grade == "Grade 9":
                pathway_pred = calculate_pathway_from_scores(math_, sci_, soc_, cre_, tec_)
                ussd_save(phone, "pathway", pathway_pred)
                ussd_save(phone, "state", "RESULT")
                return ussd_dynamic_pathway_screen(pathway_pred, math_, sci_, soc_, cre_, tec_)
            else:
                # Fire Gemini JSS suggestions to SMS in background
                asyncio.create_task(
                    _send_jss_suggestions_via_sms(phone, grade, term, math_, sci_, soc_, cre_, tec_, lang)
                )
                ussd_save(phone, "state", "DONE")
                return ussd_dynamic_jss_result(grade, term, math_, sci_, soc_, cre_, tec_)

        # ── Grade 9 pathway result ────────────────────────────────
        elif state == "RESULT":
            pathway_now = student[5] or ussd_calculate_pathway(phone)
            s           = ussd_get(phone)
            math_, sci_, soc_, cre_, tec_ = s[6], s[7], s[8], s[9], s[10]

            if step == "1":
                # View careers RANKED by their scores
                ranked = rank_careers_by_scores(pathway_now, math_ or 0, sci_ or 0,
                                                soc_ or 0, cre_ or 0, tec_ or 0)
                ussd_save(phone, "state", "USSD_CAREER_SELECT")
                return con(get_career_ussd_list(pathway_now, ranked))
            elif step == "2":
                # See all careers (unranked)
                ussd_save(phone, "state", "USSD_CAREER_SELECT_ALL")
                return con(get_career_ussd_all(pathway_now))
            elif step == "3":
                ussd_reset(phone)
                return con(
                    "Welcome to EduTena CBE\n"
                    "Select Language:\n"
                    "1. English\n"
                    "2. Swahili\n"
                    "3. Luhya\n"
                    "4. Kikuyu"
                )
            else:
                return end("Thank you for using EduTena CBE. Good luck!")

        # ── Career Selection (top 6 or ranked) ───────────────────
        elif state == "USSD_CAREER_SELECT":
            pathway = student[5]
            s       = ussd_get(phone)
            math_, sci_, soc_, cre_, tec_ = s[6], s[7], s[8], s[9], s[10]

            # Reconstruct ranked ordering
            ranked  = None
            if all([math_, sci_, soc_, cre_, tec_]):
                ranked = rank_careers_by_scores(pathway, math_, sci_, soc_, cre_, tec_)
            indices = ranked[:6] if ranked else list(range(6))

            if step.isdigit() and 1 <= int(step) <= 6:
                display_pick = int(step) - 1
                career_idx   = indices[display_pick]
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][career_idx]
                ussd_save(phone, "career_interest", name)
                ussd_save(phone, "state", "DONE")

                # Fire both SMS sends as background tasks
                grade = s[3] or ""
                asyncio.create_task(
                    _send_career_detail_via_sms(phone, pathway, career_idx, lang, grade)
                )
                return ussd_dynamic_career_result(name, demand, trend, subjects, unis)

            elif step == "7":
                ussd_save(phone, "state", "USSD_CAREER_SELECT_ALL")
                ranked_all = None
                if all([math_, sci_, soc_, cre_, tec_]):
                    ranked_all = rank_careers_by_scores(pathway, math_, sci_, soc_, cre_, tec_)
                return con(get_career_ussd_all(pathway, ranked_all))
            else:
                return con(get_career_ussd_list(pathway, ranked))

        # ── Career Selection All (10) ─────────────────────────────
        elif state == "USSD_CAREER_SELECT_ALL":
            pathway = student[5]
            s       = ussd_get(phone)
            math_, sci_, soc_, cre_, tec_ = s[6], s[7], s[8], s[9], s[10]

            ranked  = None
            if all([math_, sci_, soc_, cre_, tec_]):
                ranked = rank_careers_by_scores(pathway, math_, sci_, soc_, cre_, tec_)
            indices = ranked if ranked else list(range(10))

            if step.isdigit() and 1 <= int(step) <= 10:
                display_pick = int(step) - 1
                career_idx   = indices[display_pick]
                name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][career_idx]
                ussd_save(phone, "career_interest", name)
                ussd_save(phone, "state", "DONE")

                grade = s[3] or ""
                asyncio.create_task(
                    _send_career_detail_via_sms(phone, pathway, career_idx, lang, grade)
                )
                return ussd_dynamic_career_result(name, demand, trend, subjects, unis)
            else:
                return con(get_career_ussd_all(pathway, ranked))

        # ── Done state (JSS Gr7/8) ────────────────────────────────
        elif state == "DONE":
            if step == "1":
                ussd_reset(phone)
                return con(
                    "Welcome to EduTena CBE\n"
                    "Select Language:\n"
                    "1. English\n"
                    "2. Swahili\n"
                    "3. Luhya\n"
                    "4. Kikuyu"
                )
            else:
                return end("Thank you for using EduTena CBE. Good luck!")

        else:
            ussd_reset(phone)
            return con(
                "Welcome to EduTena CBE\n"
                "Select Language:\n"
                "1. English\n"
                "2. Swahili\n"
                "3. Luhya\n"
                "4. Kikuyu"
            )

    except Exception as e:
        print(f"[USSD] Error: {e}")
        return end("Something went wrong. Please dial again.")


# =============================================================
#  USSD BACKGROUND SMS HELPERS
#  These fire Gemini calls and send the full result via SMS
#  after the USSD session has already ended.
# =============================================================

async def _send_jss_suggestions_via_sms(
    phone: str, grade: str, term: str,
    math: int, science: int, social: int, creative: int, technical: int,
    lang: str
):
    """Send full Gemini JSS suggestions via SMS after USSD session ends."""
    M = SMS_MENU.get(lang, SMS_MENU["en"])
    try:
        suggestions = await gemini_jss_suggestions(
            grade, term, math, science, social, creative, technical, lang
        )
        msg = (
            f"EduTena CBE — {grade} | {term}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            + M["suggestion"].format(suggestions=suggestions)
            + "\n\nReply START for a new assessment."
        )
        await send_reply(phone, msg)
    except Exception as e:
        print(f"[USSD SMS Task] JSS suggestions failed: {e}")


async def _send_career_detail_via_sms(
    phone: str, pathway: str, career_idx: int, lang: str, grade: str
):
    """
    Send full career detail + Gemini narrative via SMS
    after the student picks a career on USSD.
    """
    try:
        detail    = get_career_detail_sms(pathway, career_idx, lang)
        await send_reply(phone, detail)

        # Now send personalised Gemini narrative
        name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][career_idx]
        narrative = await gemini_career_narrative(grade, pathway, name, subjects, demand, lang)
        await send_reply(phone, narrative)
    except Exception as e:
        print(f"[USSD SMS Task] Career detail failed: {e}")


async def _send_rag_answer_via_sms(phone: str, question: str, lang: str):
    """
    Answer a CBE topic question via Gemini and send the full
    response via SMS. Called as a background task from USSD RAG topic picker.
    """
    try:
        answer = await ask_gemini_rag(phone, question, lang)
        intro  = {
            "en": "EduTena CBE Assistant\n━━━━━━━━━━━━━━━━━━━━\n\n",
            "sw": "Msaidizi wa EduTena CBE\n━━━━━━━━━━━━━━━━━━━━\n\n",
            "lh": "Msaidizi wa EduTena CBE\n━━━━━━━━━━━━━━━━━━━━\n\n",
            "ki": "Msaidizi wa EduTena CBE\n━━━━━━━━━━━━━━━━━━━━\n\n",
        }.get(lang, "EduTena CBE Assistant\n━━━━━━━━━━━━━━━━━━━━\n\n")
        await send_reply(phone, intro + answer)
    except Exception as e:
        print(f"[USSD SMS Task] RAG answer failed: {e}")
