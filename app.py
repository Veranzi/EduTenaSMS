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
    conn = get_connection(); cur = conn.cursor()
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
            id SERIAL PRIMARY KEY, phone TEXT, role TEXT,
            message TEXT, created_at TIMESTAMP DEFAULT NOW()
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
    conn.commit(); cur.close(); conn.close()

@app.on_event("startup")
def startup():
    init_db()

# =============================================================
#  SHARED CONSTANTS
# =============================================================

RATING_MAP = {"1": 4, "2": 3, "3": 2, "4": 1}
RATING_OPTIONS_SMS  = "1. Exceeding Expectation\n2. Meeting Expectation\n3. Approaching Expectation\n4. Below Expectation"
RATING_OPTIONS_USSD = "1. Exceeding\n2. Meeting\n3. Approaching\n4. Below"
JSS_GRADES    = {"1": "Grade 7", "2": "Grade 8", "3": "Grade 9"}
SENIOR_GRADES = {"1": "Grade 10", "2": "Grade 11", "3": "Grade 12"}
TERMS         = {"1": "Term 1", "2": "Term 2", "3": "Term 3"}
LANG_MAP      = {"1": "en", "2": "sw", "3": "lh", "4": "ki"}
PATHWAYS      = {"1": "STEM", "2": "Social Sciences", "3": "Arts & Sports Science"}
SCORE_LABEL   = {4: "Exceeding Expectation", 3: "Meeting Expectation",
                 2: "Approaching Expectation", 1: "Below Expectation"}

# =============================================================
#  KENYA LABOUR MARKET 2025 — CAREER DATA
# =============================================================

SENIOR_CAREERS = {
    "STEM": [
        ("Software Engineer","23%","↑ Silicon Savannah boom",
         "Mathematics, Computer Science, Physics",
         "University of Nairobi, Strathmore University, JKUAT, KU, Moi University",
         "CBE Pathway: STEM\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Computer Science — Exceeding Expectation\n• Physics — Meeting Expectation\nEntry: STEM pathway completion + competency portfolio\nNote: Strathmore & KU offer bridging programmes for CBE learners"),
        ("Data Scientist","18%","↑ Highest demand 2025",
         "Mathematics, Statistics, Computer Science",
         "Strathmore University, UoN, JKUAT, African Leadership University",
         "CBE Pathway: STEM\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Computer Science — Meeting Expectation\n• Science (Applied) — Meeting Expectation\nEntry: STEM pathway + numerical reasoning portfolio\nNote: ALU uses CBE-aligned competency portfolios for admission"),
        ("Cybersecurity Specialist","12%","↑ Critical shortage",
         "Computer Science, Mathematics, Physics",
         "Strathmore University, KU, JKUAT, Kenya Polytechnic",
         "CBE Pathway: STEM\nRequired Competencies:\n• Computer Science — Exceeding Expectation\n• Mathematics — Meeting Expectation\n• Technical Skills — Exceeding Expectation\nEntry: STEM pathway + ICT project portfolio\nNote: TVET cybersecurity diplomas available post-CBE"),
        ("Renewable Energy Engineer","9%","↑ Green energy boom",
         "Physics, Chemistry, Mathematics, Technical Drawing",
         "UoN, JKUAT, Moi University, Technical University of Kenya",
         "CBE Pathway: STEM\nRequired Competencies:\n• Physics — Exceeding Expectation\n• Chemistry — Meeting Expectation\n• Mathematics — Meeting Expectation\nEntry: STEM pathway + science project portfolio\nTUK accepts CBE learners via competency assessment"),
        ("Medical Doctor","11%","↑ Healthcare demand growing",
         "Biology, Chemistry, Physics, Mathematics",
         "University of Nairobi, Moi University, KMTC (clinical officer)",
         "CBE Pathway: STEM\nRequired Competencies:\n• Biology — Exceeding Expectation\n• Chemistry — Exceeding Expectation\n• Physics — Meeting Expectation\n• Mathematics — Meeting Expectation\nEntry: STEM pathway + science portfolio + HPEB assessment\nKMTC: Clinical Officer diploma available post-Grade 12 CBE"),
        ("Civil Engineer","8%","→ Steady, housing demand",
         "Mathematics, Physics, Technical Drawing",
         "UoN, JKUAT, Technical University of Kenya, Moi University",
         "CBE Pathway: STEM\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Physics — Meeting Expectation\n• Technical Skills — Meeting Expectation\nEntry: STEM pathway + design/build project portfolio"),
        ("Pharmacist","7%","↑ Pharma sector rising",
         "Chemistry, Biology, Mathematics",
         "UoN School of Pharmacy, KU, Kenyatta University Teaching Hospital",
         "CBE Pathway: STEM\nRequired Competencies:\n• Chemistry — Exceeding Expectation\n• Biology — Exceeding Expectation\n• Mathematics — Meeting Expectation\nEntry: STEM pathway + science competency portfolio"),
        ("Architect","5%","→ Urban projects growing",
         "Mathematics, Physics, Visual Arts & Design",
         "UoN, TUK, JKUAT, Kenyatta University",
         "CBE Pathway: STEM or Arts & Sports Science\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Physics — Meeting Expectation\n• Creative/Design — Exceeding Expectation\nEntry: STEM or Arts pathway + design portfolio"),
        ("Lab Technician","4%","→ Public sector demand",
         "Biology, Chemistry, Physics",
         "KMTC, Kenya Polytechnic, KU, Moi University",
         "CBE Pathway: STEM\nRequired Competencies:\n• Biology — Meeting Expectation\n• Chemistry — Meeting Expectation\nEntry: STEM pathway. KMTC accepts CBE Grade 12 completers\nTVET diploma also available after CBE"),
        ("ICT Support Specialist","3%","→ Steady countrywide",
         "Computer Science, Mathematics",
         "Kenya Polytechnic, KCA University, Zetech University, TVET Colleges",
         "CBE Pathway: STEM\nRequired Competencies:\n• Computer Science — Meeting Expectation\n• Technical Skills — Meeting Expectation\nEntry: STEM pathway OR TVET ICT diploma post-Grade 12\nMany employers accept CBE portfolio directly"),
    ],
    "Social Sciences": [
        ("Accountant / Auditor","22%","↑ Most advertised role 2025",
         "Mathematics, Business Studies, Economics",
         "Strathmore University, UoN, KCA University, ACCA Kenya",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Business Studies — Exceeding Expectation\n• Economics — Meeting Expectation\nEntry: Social Sciences pathway + ACCA / CPA(K) pathway available\nKASNEB accepts CBE learners for CPA professional exams"),
        ("Finance Manager","19%","↑ Fintech driving demand",
         "Mathematics, Business Studies, Economics",
         "Strathmore University, UoN, CFA Institute, KCA University",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Business Studies — Exceeding Expectation\n• Economics — Exceeding Expectation\nEntry: Social Sciences pathway + financial literacy portfolio\nCFA Institute: open to CBE university graduates"),
        ("Digital Marketer","17%","↑ 17% of job postings 2025",
         "Business Studies, ICT, Communication & Media",
         "Strathmore University, USIU-Africa, KCA University, Daystar",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Business Studies — Meeting Expectation\n• ICT / Technical Skills — Meeting Expectation\n• Creative Arts — Meeting Expectation\nEntry: Social Sciences pathway + digital portfolio (content, campaigns)\nMany roles hire on portfolio — university not always required"),
        ("Lawyer / Advocate","11%","↑ Legal services growing",
         "History & Government, CRE/IRE, English/Kiswahili",
         "University of Nairobi, Moi University, KU, Strathmore Law",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• History & Government — Exceeding Expectation\n• English / Kiswahili — Exceeding Expectation\n• CRE/IRE — Meeting Expectation\nEntry: Social Sciences pathway + Kenya School of Law (post-degree)\nLaw degree then advocate training: CBE portfolio accepted"),
        ("Sales Executive","10%","↑ Top 3 most hired role",
         "Business Studies, Communication, Economics",
         "Any university, KISM (Kenya Institute of Sales & Marketing)",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Business Studies — Meeting Expectation\n• Communication — Meeting Expectation\nEntry: Grade 12 CBE completion in any pathway\nKISM offers professional sales diplomas open to CBE completers"),
        ("Human Resource Manager","8%","→ Steady across all sectors",
         "Business Studies, Sociology, Psychology",
         "UoN, KU, Moi University, IHRM Kenya",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Business Studies — Meeting Expectation\n• Social Studies — Meeting Expectation\nEntry: Social Sciences pathway + IHRM professional membership"),
        ("Economist","5%","→ Government & research",
         "Mathematics, Economics, Geography",
         "UoN, Moi University, USIU-Africa, Egerton University",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Mathematics — Exceeding Expectation\n• Economics — Exceeding Expectation\n• Geography — Meeting Expectation\nEntry: Social Sciences pathway + quantitative project portfolio"),
        ("Teacher / Educator","4%","→ High demand, CBC era",
         "Specialisation subject + Education studies",
         "KU, Moi University, Maseno University, Teacher Training Colleges",
         "CBE Pathway: Any pathway\nRequired Competencies:\n• Specialisation subject — Exceeding Expectation\n• Communication — Meeting Expectation\nEntry: Grade 12 CBE completion + KNUT / TSC registration\nP1 Teacher Training Colleges accept CBE Grade 12 completers"),
        ("Psychologist","3%","↑ Mental health demand rising",
         "Biology, CRE/IRE, Social Studies",
         "UoN, USIU-Africa, KU, Catholic University of Eastern Africa",
         "CBE Pathway: Social Sciences\nRequired Competencies:\n• Biology — Meeting Expectation\n• Social Studies — Exceeding Expectation\nEntry: Social Sciences pathway + counselling volunteer portfolio"),
        ("Journalist / Media","1%","↓ Print declining, digital rising",
         "English/Kiswahili, History, ICT",
         "USIU-Africa, Daystar University, KU, Kenya Institute of Mass Communication",
         "CBE Pathway: Social Sciences or Arts & Sports\nRequired Competencies:\n• English / Kiswahili — Exceeding Expectation\n• Creative Arts — Meeting Expectation\nEntry: Any pathway + strong writing/media portfolio\nKIMC accepts CBE completers for journalism diploma"),
    ],
    "Arts & Sports Science": [
        ("Graphic Designer / UI-UX","20%","↑ Digital economy boom",
         "Visual Arts, Computer Science, Mathematics",
         "ADMI, Kenyatta University, Limkokwing University, Strathmore",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Visual Arts — Exceeding Expectation\n• Computer Science — Meeting Expectation\n• Mathematics — Meeting Expectation\nEntry: Arts & Sports pathway + design portfolio (mandatory)\nADMI uses portfolio-based CBE admission — no points system"),
        ("Film & Content Creator","18%","↑ Social media economy",
         "Drama & Theatre, Visual Arts, ICT",
         "ADMI, AFDA Kenya, Daystar University, KCA University",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Drama & Theatre — Exceeding Expectation\n• Visual Arts — Meeting Expectation\n• ICT / Technical Skills — Meeting Expectation\nEntry: Arts & Sports pathway + video/content portfolio"),
        ("Interior Designer","12%","↑ Urban housing boom",
         "Visual Arts, Mathematics, Technical Drawing",
         "ADMI, Technical University of Kenya, Kenyatta University, Limkokwing",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Visual Arts — Exceeding Expectation\n• Mathematics — Meeting Expectation\n• Technical Skills — Meeting Expectation\nEntry: Arts & Sports pathway + design/drawing portfolio"),
        ("Physiotherapist","10%","↑ Sports & healthcare",
         "Physical Education, Biology, Chemistry",
         "UoN, KU, KMTC, Moi University",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Physical Education — Exceeding Expectation\n• Biology — Meeting Expectation\n• Chemistry — Meeting Expectation\nEntry: Arts & Sports pathway + KMTC physiotherapy diploma"),
        ("Sports Coach / Manager","9%","→ Growing, sports academies",
         "Physical Education, Biology, Business Studies",
         "KU, Moi University, Sports Kenya, TVET Sports Colleges",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Physical Education — Exceeding Expectation\n• Biology — Meeting Expectation\nEntry: Arts & Sports pathway + coaching/competition portfolio"),
        ("Tourism & Hospitality Manager","8%","↑ Post-COVID recovery",
         "Geography, Business Studies, Home Science",
         "Utalii College, KU, USIU-Africa, Mombasa Polytechnic",
         "CBE Pathway: Arts & Sports Science or Social Sciences\nRequired Competencies:\n• Business Studies — Meeting Expectation\n• Geography — Meeting Expectation\n• Home Science — Meeting Expectation\nEntry: Any pathway + Utalii College hospitality diploma"),
        ("Fashion Designer","7%","→ Niche but growing",
         "Visual Arts, Home Science, Business Studies",
         "Kenya Fashion Institute, ADMI, Kenyatta University",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Visual Arts — Exceeding Expectation\n• Home Science — Meeting Expectation\nEntry: Arts & Sports pathway + garment/design portfolio"),
        ("Beauty & Wellness Specialist","6%","↑ TVET sector growing",
         "Home Science, Biology, Chemistry",
         "TVET Colleges, Kenya Beauty School, Moi University",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Home Science — Meeting Expectation\n• Biology — Meeting Expectation\nEntry: Grade 12 CBE Arts & Sports completion"),
        ("Musician / Performer","3%","→ Competitive but growing",
         "Music, Drama & Theatre, Visual Arts",
         "Kenya Conservatoire of Music, Daystar University, KIPPRA",
         "CBE Pathway: Arts & Sports Science\nRequired Competencies:\n• Music / Drama — Exceeding Expectation\n• Creative Arts — Exceeding Expectation\nEntry: Arts & Sports pathway + performance/recording portfolio"),
        ("Community Development Officer","7%","→ NGO & county government",
         "History, CRE/IRE, Social Studies",
         "UoN, Moi University, Catholic University, KU",
         "CBE Pathway: Social Sciences or Arts & Sports Science\nRequired Competencies:\n• Social Studies — Meeting Expectation\n• Communication — Meeting Expectation\nEntry: Any pathway + community service/volunteer portfolio"),
    ],
}

# =============================================================
#  MULTILINGUAL UI  — single source of truth for ALL text
#  Use t(lang, key, **kwargs) to fetch any string.
# =============================================================

UI = {
    "en": {
        "welcome_lang":        "Welcome to EduTena CBE.\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu",
        "invalid_lang":        "Invalid.\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu",
        "mode_select":         "What would you like to do?\n1. Pathway & Career Guide\n   (assess your level & explore careers)\n2. CBE Assistant\n   (ask questions, homework help)",
        "mode_err":            "Invalid. Reply 1 for Pathway & Careers or 2 for CBE Assistant.",
        "mode_ussd_2":         "EduTena CBE\nWhat would you like?\n1. Pathway & Career Guide\n2. CBE Assistant\n   (Answer sent via SMS)",
        "mode_ussd_err":       "Invalid.\n1. Pathway & Career Guide\n2. CBE Assistant",
        "rag_sms_only":        "CBE Assistant (SMS only)\n\nText START to this number,\nselect option 2, then type\nany CBE question.\nFull answers, no limits!",
        "welcome":             "EduTena CBE\nSelect Level:\n1. JSS (Grade 7-9)\n2. Senior (Grade 10-12)",
        "level_err":           "Invalid. Reply 1 for JSS or 2 for Senior.",
        "jss_grade":           "Select JSS Grade:\n1. Grade 7\n2. Grade 8\n3. Grade 9",
        "senior_grade":        "Select Senior Grade:\n1. Grade 10\n2. Grade 11\n3. Grade 12",
        "grade_err":           "Invalid. Select 1, 2, or 3.",
        "term":                "Select Term:\n1. Term 1\n2. Term 2\n3. Term 3",
        "term_err":            "Invalid. Select term 1, 2, or 3.",
        "senior_pathway":      "Select your CBE Pathway:\n1. STEM\n   (Science, Tech, Maths)\n2. Social Sciences\n   (Business, Law, Economics)\n3. Arts & Sports Science\n   (Creative, PE, Media)",
        "pathway_err":         "Invalid. Select 1, 2, or 3.",
        "pathway_msg":         "Predicted Pathway: {pathway}\nBased on your Grade 9 scores.\nReply CAREERS to see matched careers.",
        "rate_math":           "Rate your Math performance:\n{opts}",
        "rate_science":        "Rate your Science performance:\n{opts}",
        "rate_social":         "Rate your Social Studies performance:\n{opts}",
        "rate_creative":       "Rate your Creative Arts performance:\n{opts}",
        "rate_technical":      "Rate your Technical Skills performance:\n{opts}",
        "invalid_rating":      "Invalid. Select 1, 2, 3, or 4.",
        "career_hdr":          "{pathway} Careers | {grade}\nKenya Labour Market 2025\nSelect your interest:\n",
        "career_footer":       "\nReply 1-5 to select\nReply MORE to see all 10",
        "all_career_hdr":      "All {pathway} Careers\nKenya Market 2025\nSelect:\n",
        "all_career_footer":   "\nReply 1-10 to select.",
        "no_pathway":          "Complete your assessment first. Reply START.",
        "invalid_career":      "Invalid. Reply a number from the career list.",
        "career_detail":       "━━━━━━━━━━━━━━━━━━━━\nCAREER: {name}\n━━━━━━━━━━━━━━━━━━━━\n\nMarket Demand: {demand} of Kenyan job postings 2025\nTrend: {trend}\n\n📚 Focus Subjects:\n{subjects}\n\n🏫 Universities & Colleges:\n{unis}\n\n📋 CBE Entry Requirements:\n{reqs}\n\n✅ Saved to your profile!\nReply START to reassess or MENU to go back.",
        "ussd_career_end":     "✅ {name}\nDemand: {demand} | {trend}\n\n📚 Focus Subjects:\n{subjects}\n\n🏫 Colleges:\n{unis}\n\n📋 CBE Requirements:\n{reqs}\n\n📱 Full details + personal advice\nsent to your SMS now!",
        "tracking_hdr":        "Performance: {grade} | {term}\n",
        "suggestion":          "{suggestions}\nYou can also ask any CBE question by texting it!",
        "ussd_jss_result":     "{grade} | {term}\nBest subject: {strongest}\nNeeds work: {weak}\n\n📱 Full advice sent via SMS!\n\n1. Restart\n2. Exit",
        "ussd_pathway_result": "CBE Pathway: {pathway}\nStrongest: {top}\nScores: {summary}\n\n1. View Matched Careers\n2. See All Careers\n3. Restart\n4. Exit",
        "ussd_rag_menu":       "CBE Assistant\nAnswer sent via SMS\n\nPick a topic:\n1. What is CBE/CBC?\n2. Pathways explained\n3. How to build a portfolio\n4. CBE vs old 844 system\n5. University entry with CBE\n6. Ask your own (use SMS)",
        "ussd_rag_sending":    "✅ Your answer is being\nprepared and will arrive\nvia SMS in ~30 seconds.\n\nDial back anytime!",
        "ussd_rag_sms_tip":    "To ask your own question:\n\nText START to this number,\nselect option 2 (CBE Assistant)\nthen type any question.\nFull answers, no length limit!",
        "done":                "Assessment saved. Reply CAREERS or ask any CBE question!",
        "paused":              "Still paused. Reply RESUME to continue your assessment.",
        "thank_you":           "Thank you for using EduTena CBE. Good luck!",
        "error":               "Something went wrong. Please try again.",
        "resume_fallback":     "Reply START to begin your assessment.",
        "rag_welcome":         "CBE Assistant ready!\nAsk me anything about:\n- CBE subjects & pathways\n- Assignment help\n- How CBE works\n- Career questions\n\nJust type your question.\nReply MENU to go back.",
        "rag_menu_reminder":   "Reply MENU to return to the main menu.",
    },
    "sw": {
        "welcome_lang":        "Karibu EduTena CBE.\nChagua Lugha:\n1. Kiingereza\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ",
        "invalid_lang":        "Batili.\n1. Kiingereza\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ",
        "mode_select":         "Unataka kufanya nini?\n1. Mwongozo wa Njia & Kazi\n   (tathmini kiwango chako)\n2. Msaidizi wa CBE\n   (uliza maswali, msaada wa kazi)",
        "mode_err":            "Batili. Jibu 1 kwa Mwongozo au 2 kwa Msaidizi.",
        "mode_ussd_2":         "EduTena CBE\nUnataka nini?\n1. Mwongozo wa Njia & Kazi\n2. Msaidizi wa CBE\n   (Jibu linatumwa kwa SMS)",
        "mode_ussd_err":       "Batili.\n1. Mwongozo wa Njia & Kazi\n2. Msaidizi wa CBE",
        "rag_sms_only":        "Msaidizi wa CBE (SMS tu)\n\nTuma START kwa nambari hii,\nchagua chaguo 2, andika\nswali lolote la CBE.\nMajibu kamili, bila kikomo!",
        "welcome":             "EduTena CBE\nChagua Kiwango:\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
        "level_err":           "Batili. Jibu 1 kwa JSS au 2 kwa Sekondari.",
        "jss_grade":           "Chagua Darasa la JSS:\n1. Darasa 7\n2. Darasa 8\n3. Darasa 9",
        "senior_grade":        "Chagua Darasa la Sekondari:\n1. Darasa 10\n2. Darasa 11\n3. Darasa 12",
        "grade_err":           "Batili. Chagua 1, 2, au 3.",
        "term":                "Chagua Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":            "Batili. Chagua muhula 1, 2, au 3.",
        "senior_pathway":      "Chagua Njia yako ya CBE:\n1. STEM\n   (Sayansi, Teknolojia, Hisabati)\n2. Sayansi Jamii\n   (Biashara, Sheria, Uchumi)\n3. Sanaa & Michezo\n   (Ubunifu, PE, Vyombo vya Habari)",
        "pathway_err":         "Batili. Chagua 1, 2, au 3.",
        "pathway_msg":         "Njia Inayotabirika: {pathway}\nKulingana na alama zako za Darasa 9.\nJibu CAREERS kuona kazi zinazolingana.",
        "rate_math":           "Tathmini utendaji wako wa Hisabati:\n{opts}",
        "rate_science":        "Tathmini utendaji wako wa Sayansi:\n{opts}",
        "rate_social":         "Tathmini utendaji wako wa Sayansi Jamii:\n{opts}",
        "rate_creative":       "Tathmini utendaji wako wa Sanaa:\n{opts}",
        "rate_technical":      "Tathmini utendaji wako wa Ujuzi wa Kiufundi:\n{opts}",
        "invalid_rating":      "Batili. Chagua 1, 2, 3, au 4.",
        "career_hdr":          "Kazi za {pathway} | {grade}\nSoko la Kazi Kenya 2025\nChagua hamu yako:\n",
        "career_footer":       "\nJibu 1-5 kuchagua\nJibu MORE kuona zote 10",
        "all_career_hdr":      "Kazi Zote za {pathway}\nSoko Kenya 2025\nChagua:\n",
        "all_career_footer":   "\nJibu 1-10 kuchagua.",
        "no_pathway":          "Maliza tathmini kwanza. Jibu START.",
        "invalid_career":      "Batili. Jibu nambari kutoka orodha ya kazi.",
        "career_detail":       "━━━━━━━━━━━━━━━━━━━━\nKAZI: {name}\n━━━━━━━━━━━━━━━━━━━━\n\nMahitaji Sokoni: {demand} ya nafasi zote za kazi Kenya 2025\nMwelekeo: {trend}\n\n📚 Masomo ya Kuzingatia:\n{subjects}\n\n🏫 Vyuo:\n{unis}\n\n📋 Mahitaji ya CBE:\n{reqs}\n\n✅ Imehifadhiwa kwenye wasifu wako!\nJibu START kuanza upya au MENU kurudi.",
        "ussd_career_end":     "✅ {name}\nMahitaji: {demand} | {trend}\n\n📚 Masomo ya Kuzingatia:\n{subjects}\n\n🏫 Vyuo:\n{unis}\n\n📋 Mahitaji ya CBE:\n{reqs}\n\n📱 Maelezo kamili + ushauri\nwametumwa kwa SMS yako sasa!",
        "tracking_hdr":        "Utendaji: {grade} | {term}\n",
        "suggestion":          "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "ussd_jss_result":     "{grade} | {term}\nSomo bora: {strongest}\nZingatia zaidi: {weak}\n\n📱 Ushauri kamili umetumwa kwa SMS!\n\n1. Anza Upya\n2. Toka",
        "ussd_pathway_result": "Njia ya CBE: {pathway}\nNzuri zaidi: {top}\nAlama: {summary}\n\n1. Tazama Kazi Zinazolingana\n2. Tazama Kazi Zote\n3. Anza Upya\n4. Toka",
        "ussd_rag_menu":       "Msaidizi wa CBE\nJibu litatumwa kwa SMS\n\nChagua mada:\n1. CBE/CBC ni nini?\n2. Njia zote zimeelezwa\n3. Jinsi ya kuunda portfolio\n4. CBE vs mfumo wa zamani 844\n5. Kuingia chuo kikuu na CBE\n6. Uliza swali lako (SMS)",
        "ussd_rag_sending":    "✅ Jibu lako linaandaliwa\nna litatumwa kwa SMS\ndakika moja.\n\nPiga simu tena wakati wowote!",
        "ussd_rag_sms_tip":    "Kuuliza swali lako mwenyewe:\n\nTuma START kwa nambari hii,\nchagua chaguo 2 (Msaidizi wa CBE)\nkisha andika swali lolote.\nMajibu kamili, bila kikomo!",
        "done":                "Imehifadhiwa. Jibu CAREERS au uliza swali lolote la CBE!",
        "paused":              "Bado imesimamishwa. Jibu RESUME kuendelea na tathmini yako.",
        "thank_you":           "Asante kwa kutumia EduTena CBE. Kila la heri!",
        "error":               "Hitilafu imetokea. Tafadhali jaribu tena.",
        "resume_fallback":     "Jibu START kuanza tathmini yako.",
        "rag_welcome":         "Msaidizi wa CBE yuko tayari!\nNiulize chochote kuhusu:\n- Masomo & njia za CBE\n- Msaada wa kazi za nyumbani\n- Jinsi CBE inavyofanya kazi\n- Maswali ya kazi\n\nAndika swali lako.\nJibu MENU kurudi.",
        "rag_menu_reminder":   "Jibu MENU kurudi menyu kuu.",
    },
    "lh": {
        "welcome_lang":        "Karibu EduTena CBE.\nSena Olulimi:\n1. Kiingereza\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ",
        "invalid_lang":        "Busia.\n1. Kiingereza\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ",
        "mode_select":         "Okhwenenda khukola nini?\n1. Mwongozo wa Njia & Emilimo\n2. Msaidizi wa CBE\n   (uliza maswali, msaada wa masomo)",
        "mode_err":            "Busia. Jibu 1 kwa Mwongozo kamba 2 kwa Msaidizi.",
        "mode_ussd_2":         "EduTena CBE\nOkhwenenda nini?\n1. Mwongozo wa Njia & Emilimo\n2. Msaidizi wa CBE\n   (Jibu linatumwa kwa SMS)",
        "mode_ussd_err":       "Busia.\n1. Mwongozo wa Njia & Emilimo\n2. Msaidizi wa CBE",
        "rag_sms_only":        "Msaidizi wa CBE (SMS tu)\n\nTuma START, sena 2,\nandika swali la CBE.",
        "welcome":             "EduTena CBE\nSena Engufu:\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
        "level_err":           "Busia. Jibu 1 kwa JSS kamba 2 kwa Sekondari.",
        "jss_grade":           "Sena Okhufunda lwa JSS:\n1. Okhufunda 7\n2. Okhufunda 8\n3. Okhufunda 9",
        "senior_grade":        "Sena Okhufunda lwa Sekondari:\n1. Okhufunda 10\n2. Okhufunda 11\n3. Okhufunda 12",
        "grade_err":           "Busia. Sena 1, 2, kamba 3.",
        "term":                "Sena Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":            "Busia. Sena muhula 1, 2, kamba 3.",
        "senior_pathway":      "Sena Njia yako ya CBE:\n1. STEM\n   (Sayansi, Teknolojia, Hisabati)\n2. Sayansi Jamii\n   (Biashara, Sheria, Uchumi)\n3. Sanaa & Michezo\n   (Ubunifu, PE, Habari)",
        "pathway_err":         "Busia. Sena 1, 2, kamba 3.",
        "pathway_msg":         "Njia Enyiseniwe: {pathway}\nKulingana na alama zako za Okhufunda 9.\nJibu CAREERS okhuona emilimo inayolingana.",
        "rate_math":           "Sena utendaji wako wa Hisabati:\n{opts}",
        "rate_science":        "Sena utendaji wako wa Sayansi:\n{opts}",
        "rate_social":         "Sena utendaji wako wa Sayansi Jamii:\n{opts}",
        "rate_creative":       "Sena utendaji wako wa Sanaa:\n{opts}",
        "rate_technical":      "Sena utendaji wako wa Ujuzi wa Kiufundi:\n{opts}",
        "invalid_rating":      "Busia. Sena 1, 2, 3, kamba 4.",
        "career_hdr":          "Emilimo ya {pathway} | {grade}\nSoko Kenya 2025\nSena hamu yako:\n",
        "career_footer":       "\nJibu 1-5 okukhusena\nJibu MORE okhuona yote 10",
        "all_career_hdr":      "Emilimo Yote ya {pathway}\nSoko Kenya 2025\nSena:\n",
        "all_career_footer":   "\nJibu 1-10 okukhusena.",
        "no_pathway":          "Maliza tathmini kwanza. Jibu START.",
        "invalid_career":      "Busia. Jibu nambari kutoka orodha ya emilimo.",
        "career_detail":       "━━━━━━━━━━━━━━━━━━━━\nEMILIMO: {name}\n━━━━━━━━━━━━━━━━━━━━\n\nHaja Sokoni: {demand} ya nafasi zote Kenya 2025\nMwelekeo: {trend}\n\n📚 Masomo:\n{subjects}\n\n🏫 Vyuo:\n{unis}\n\n📋 Mahitaji ya CBE:\n{reqs}\n\n✅ Imehifadhiwa!\nJibu START okhuanza au MENU kurudi.",
        "ussd_career_end":     "✅ {name}\nMahitaji: {demand} | {trend}\n\n📚 Masomo:\n{subjects}\n\n🏫 Vyuo:\n{unis}\n\n📋 Mahitaji ya CBE:\n{reqs}\n\n📱 Maelezo kamili + ushauri\nkwa SMS yako sasa!",
        "tracking_hdr":        "Okusema: {grade} | {term}\n",
        "suggestion":          "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "ussd_jss_result":     "{grade} | {term}\nBora: {strongest}\nJaribu zaidi: {weak}\n\n📱 Ushauri kamili kwa SMS!\n\n1. Anza Upya\n2. Toka",
        "ussd_pathway_result": "Njia ya CBE: {pathway}\nBora: {top}\nAlama: {summary}\n\n1. Tazama Emilimo Inayolingana\n2. Tazama Emilimo Yote\n3. Anza Upya\n4. Toka",
        "ussd_rag_menu":       "Msaidizi wa CBE\nJibu kwa SMS\n\nChagua mada:\n1. CBE/CBC ni nini?\n2. Njia zimeelezwa\n3. Jinsi ya portfolio\n4. CBE vs 844\n5. Chuo kikuu na CBE\n6. Uliza swali lako (SMS)",
        "ussd_rag_sending":    "✅ Jibu lako linaandaliwa\nna litatumwa kwa SMS.\n\nPiga simu tena wakati wowote!",
        "ussd_rag_sms_tip":    "Kuuliza swali lako:\nTuma START, sena 2,\nandika swali lolote la CBE.",
        "done":                "Yakhwira. Jibu CAREERS kamba uliza swali la CBE!",
        "paused":              "Bado imesimamishwa. Jibu RESUME kuendelea.",
        "thank_you":           "Asante okhutumia EduTena CBE. Kila la heri!",
        "error":               "Hitilafu imetokea. Tafadhali jaribu tena.",
        "resume_fallback":     "Jibu START okhuanza tathmini yako.",
        "rag_welcome":         "Msaidizi wa CBE yuko tayari!\nNiulize chochote:\n- Masomo & njia za CBE\n- Msaada wa kazi\n- Jinsi CBE inavyofanya kazi\n\nAndika swali lako.\nJibu MENU kurudi.",
        "rag_menu_reminder":   "Jibu MENU kurudi menyu kuu.",
    },
    "ki": {
        "welcome_lang":        "Ũkaribũ EduTena CBE.\nThura Rurimi:\n1. Kiingereza\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ",
        "invalid_lang":        "Ti wegwaru.\n1. Kiingereza\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ",
        "mode_select":         "Ni uria ukenda gukora?\n1. Mwongozo wa Njia & Mirimo\n   (tathmini kiwango chako)\n2. Msaidizi wa CBE\n   (uiguithia maswali, uthuri wa masomo)",
        "mode_err":            "Ti wegwaru. Cookia 1 kwa Mwongozo kana 2 kwa Msaidizi.",
        "mode_ussd_2":         "EduTena CBE\nNi uria ukenda?\n1. Mwongozo wa Njia & Mirimo\n2. Msaidizi wa CBE\n   (Jibu rigatumirwo na SMS)",
        "mode_ussd_err":       "Ti wegwaru.\n1. Mwongozo wa Njia & Mirimo\n2. Msaidizi wa CBE",
        "rag_sms_only":        "Msaidizi wa CBE (SMS tu)\n\nTuma START, thura 2,\nandika swali la CBE.",
        "welcome":             "EduTena CBE\nThura Kiwango:\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)",
        "level_err":           "Ti wegwaru. Cookia 1 JSS kana 2 Sekondari.",
        "jss_grade":           "Thura Kiwango kia JSS:\n1. Kiwango 7\n2. Kiwango 8\n3. Kiwango 9",
        "senior_grade":        "Thura Kiwango kia Sekondari:\n1. Kiwango 10\n2. Kiwango 11\n3. Kiwango 12",
        "grade_err":           "Ti wegwaru. Thura 1, 2, kana 3.",
        "term":                "Thura Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":            "Ti wegwaru. Thura 1, 2, kana 3.",
        "senior_pathway":      "Thura Njia yaku ya CBE:\n1. STEM\n   (Sayansi, Teknolojia, Hisabati)\n2. Sayansi Jamii\n   (Biashara, Sheria, Uchumi)\n3. Sanaa & Michezo\n   (Ubunifu, PE, Habari)",
        "pathway_err":         "Ti wegwaru. Thura 1, 2, kana 3.",
        "pathway_msg":         "Njia Yoneneirwo: {pathway}\nKulingana na mbari yako ya Kiwango 9.\nCookia CAREERS kuona mirimo inayolingana.",
        "rate_math":           "Thura utendaji wako wa Hisabati:\n{opts}",
        "rate_science":        "Thura utendaji wako wa Sayansi:\n{opts}",
        "rate_social":         "Thura utendaji wako wa Sayansi Jamii:\n{opts}",
        "rate_creative":       "Thura utendaji wako wa Sanaa:\n{opts}",
        "rate_technical":      "Thura utendaji wako wa Ujuzi wa Kiufundi:\n{opts}",
        "invalid_rating":      "Ti wegwaru. Thura 1, 2, 3, kana 4.",
        "career_hdr":          "Mirimo ya {pathway} | {grade}\nSoko Kenya 2025\nThura hamu yaku:\n",
        "career_footer":       "\nCookia 1-5 guthura\nCookia MORE kuona yothe 10",
        "all_career_hdr":      "Mirimo Yothe ya {pathway}\nSoko Kenya 2025\nThura:\n",
        "all_career_footer":   "\nCookia 1-10 guthura.",
        "no_pathway":          "Ithoma mbere. Cookia START.",
        "invalid_career":      "Ti wegwaru. Cookia nambari kutoka orodha ya mirimo.",
        "career_detail":       "━━━━━━━━━━━━━━━━━━━━\nMURIMO: {name}\n━━━━━━━━━━━━━━━━━━━━\n\nHitaji Sokoni: {demand} ya nafasi zose Kenya 2025\nMwelekeo: {trend}\n\n📚 Masomo:\n{subjects}\n\n🏫 Vyuo:\n{unis}\n\n📋 Mahitaji ya CBE:\n{reqs}\n\n✅ Niikuura!\nCookia START gutomia au MENU gũthiĩ.",
        "ussd_career_end":     "✅ {name}\nHitaji: {demand} | {trend}\n\n📚 Masomo:\n{subjects}\n\n🏫 Vyuo:\n{unis}\n\n📋 Mahitaji ya CBE:\n{reqs}\n\n📱 Maelezo kamili + ushauri\nkwa SMS yako sasa!",
        "tracking_hdr":        "Mahitio: {grade} | {term}\n",
        "suggestion":          "{suggestions}\nUnaweza pia kuuliza swali lolote la CBE!",
        "ussd_jss_result":     "{grade} | {term}\nNzuri: {strongest}\nIthomia zaidi: {weak}\n\n📱 Ũhoro mũno kwa SMS!\n\n1. Thomia Rĩngĩ\n2. Rũa",
        "ussd_pathway_result": "Njia ya CBE: {pathway}\nNzuri: {top}\nMbari: {summary}\n\n1. Ona Mirimo Inayolingana\n2. Ona Mirimo Yothe\n3. Thomia Rĩngĩ\n4. Rũa",
        "ussd_rag_menu":       "Msaidizi wa CBE\nJibu kwa SMS\n\nThura mada:\n1. CBE/CBC ni nini?\n2. Njia zimeelezwa\n3. Jinsi ya portfolio\n4. CBE vs 844\n5. Chuo kikuu na CBE\n6. Uliza swali lako (SMS)",
        "ussd_rag_sending":    "✅ Jibu riaku rinaandaliwa\nna rigatumirwo kwa SMS.\n\nPiga simu rĩngĩ wakati wowote!",
        "ussd_rag_sms_tip":    "Kuuliza swali lako:\nTuma START, thura 2,\nandika swali la CBE.",
        "done":                "Niikuura. Cookia CAREERS kana uiguithia swali la CBE!",
        "paused":              "Bado imesimamishwa. Cookia RESUME kuendelea.",
        "thank_you":           "Nĩ wega ũgĩtumia EduTena CBE. Kila la heri!",
        "error":               "Kũheo gũtũkite. Gerera rĩngĩ.",
        "resume_fallback":     "Cookia START gũthomia tathmini yaku.",
        "rag_welcome":         "Msaidizi wa CBE arĩ ũhoro!\nNiiguithia ũũ wowote:\n- Masomo & njia cia CBE\n- Uthuri wa ũthuri\n- Jinsi CBE inavyofanya kazi\n\nAndika swali riaku.\nCookia MENU gũthiĩ.",
        "rag_menu_reminder":   "Cookia MENU gũthiĩ menyu kuu.",
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    """Translate key to lang; fall back to English; support format kwargs."""
    text = UI.get(lang, UI["en"]).get(key) or UI["en"].get(key, f"[{key}]")
    return text.format(**kwargs) if kwargs else text


# =============================================================
#  GEMINI — LANGUAGE-AWARE SYSTEM PROMPTS
# =============================================================

def _lang_instruction(lang: str) -> str:
    return {
        "sw": "You MUST respond ONLY in Kiswahili. Do not use English at all.",
        "lh": "You MUST respond in Luhya, mixing English only where Luhya lacks a word.",
        "ki": "You MUST respond in Kikuyu, mixing English only where Kikuyu lacks a word.",
    }.get(lang, "Respond in English.")


def cbe_system_prompt(lang: str) -> str:
    return f"""You are EduTena, a Kenya CBE (Competency Based Education) assistant.
You help students and parents navigate the CBC/CBE curriculum.
IMPORTANT: This is CBE — NOT the old 844 system. Entry to university
is competency portfolio based, not KCSE points.

CBE Structure:
- JSS: Grade 7, 8, 9 (Junior Secondary)
- Senior Secondary: Grade 10, 11, 12 — pathways: STEM, Social Sciences, Arts & Sports Science
- Performance levels: Exceeding Expectation (4), Meeting Expectation (3),
  Approaching Expectation (2), Below Expectation (1)

CRITICAL LANGUAGE RULE: {_lang_instruction(lang)}
Never switch language mid-response. If the student chose Kiswahili, every word must be Kiswahili.

OTHER RULES:
- Be warm, speak like a trusted Kenyan teacher or older sibling
- Never give medical, legal or financial investment advice
- Write as many sentences as needed — do NOT truncate your answer
- If asked something completely unrelated to CBE/education, politely redirect
"""

DOCUMENT_CONTEXT = """
[CBE curriculum document context will be injected here once linked.]
"""

CBE_ASSISTANT_SYSTEM = """\
You are EduTena CBE Assistant — a friendly, knowledgeable tutor for
Kenyan students and parents navigating the Competency Based Education (CBE) system.

You can help with:
- Explaining CBE concepts and how the system works
- Homework and assignment guidance (guide thinking, don't just give answers)
- Subject-specific questions across JSS and Senior Secondary subjects
- Pathway and career exploration
- How to prepare a CBE portfolio for university entry
- Understanding CBE vs the old 844 system
- Advice for parents

DOCUMENT CONTEXT:
{document_context}

CRITICAL LANGUAGE RULE: {lang_instruction}
Never switch language mid-response.

OTHER RULES:
- Be warm, speak like a trusted Kenyan teacher
- Write as many sentences / paragraphs as needed — never truncate
- NEVER give medical, legal, or financial investment advice
"""


# =============================================================
#  GEMINI CALLER
# =============================================================

async def gemini_call(prompt: str, max_tokens: int, temperature: float, label: str) -> str | None:
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-001:generateContent?key={GEMINI_KEY}",
                json={"contents": [{"parts": [{"text": prompt}]}],
                      "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature}}
            )
            data = r.json()
            print(f"[{label}] HTTP {r.status_code} | keys: {list(data.keys())}")
            if "error" in data: print(f"[{label}] {data['error']}"); return None
            candidates = data.get("candidates", [])
            if not candidates: print(f"[{label}] empty candidates: {data}"); return None
            c = candidates[0]
            if c.get("finishReason") == "SAFETY": return "__SAFETY__"
            return c["content"]["parts"][0]["text"].strip()
    except Exception as e:
        print(f"[{label}] {type(e).__name__}: {e}"); return None


# =============================================================
#  GEMINI 1 — CAREER NARRATIVE
# =============================================================

async def gemini_career_narrative(grade, pathway, career, subjects, demand, lang) -> str:
    if not GEMINI_KEY:
        return t(lang, "done")
    prompt = (
        f"{cbe_system_prompt(lang)}\n\n"
        f"TASK: Write a personalised, motivating message for a Kenyan student "
        f"who just chose their career interest.\n\n"
        f"Student profile:\n- Grade: {grade}\n- CBE Pathway: {pathway}\n"
        f"- Career chosen: {career}\n- Key subjects: {subjects}\n"
        f"- Market demand: {demand} of Kenya job postings 2025\n\n"
        f"The message must:\n"
        f"1. Affirm their choice with a specific reason tied to their CBE pathway\n"
        f"2. Explain what this career involves day-to-day (real-world detail)\n"
        f"3. Give 2-3 concrete next steps they can take in school right now\n"
        f"4. Name the specific subjects they must focus on and why each matters\n"
        f"5. End with genuine encouragement mentioning the Kenya job market opportunity\n\n"
        f"Write as many sentences as needed. Do NOT be generic. Be warm and Kenyan.\n\nMessage:"
    )
    a = await gemini_call(prompt, 700, 0.7, "career_narrative")
    return a if (a and a != "__SAFETY__") else f"Great choice! Focus on {subjects} and build your CBE portfolio."


# =============================================================
#  GEMINI 2 — JSS SUGGESTIONS
# =============================================================

async def gemini_jss_suggestions(grade, term, math, science, social, creative, technical, lang) -> str:
    fallback = get_improvement_suggestions(math, science, social, creative, technical, lang)
    if not GEMINI_KEY: return fallback
    scores = (f"Math: {SCORE_LABEL.get(math,'?')}\nScience: {SCORE_LABEL.get(science,'?')}\n"
              f"Social Studies: {SCORE_LABEL.get(social,'?')}\nCreative Arts: {SCORE_LABEL.get(creative,'?')}\n"
              f"Technical Skills: {SCORE_LABEL.get(technical,'?')}")
    prompt = (
        f"{cbe_system_prompt(lang)}\n\n"
        f"TASK: Write a detailed, personalised improvement message for a JSS student "
        f"who just submitted their CBE self-assessment.\n\n"
        f"Student: {grade}, {term}\nPerformance:\n{scores}\n\n"
        f"The message must:\n"
        f"1. Warmly acknowledge their strongest subject\n"
        f"2. For each subject at Approaching/Below Expectation: name it, explain why it matters "
        f"in CBE, give 2-3 specific daily study tips, suggest a free Kenyan resource\n"
        f"3. Close with motivation connecting their grade to Senior pathway options\n\n"
        f"Write as many sentences as needed. Give real, specific advice.\n\nMessage:"
    )
    a = await gemini_call(prompt, 900, 0.6, "jss_suggestions")
    return a if (a and a != "__SAFETY__") else fallback


# =============================================================
#  GEMINI 3 — MID-FLOW Q&A
# =============================================================

async def ask_gemini(phone, question, lang="en", context_state="", channel="sms") -> str:
    if not GEMINI_KEY: return t(lang, "resume_fallback")
    history = "".join(f"{r.upper()}: {m}\n" for r, m in get_chat_history(phone, 6))
    flow = (f"\nNote: Student is mid-assessment (step: {context_state}). "
            f"They can reply RESUME to continue.\n") if context_state else ""
    resume = t(lang, "resume_fallback")
    prompt = (
        f"{cbe_system_prompt(lang)}{flow}\n"
        f"CONVERSATION HISTORY:\n{history}\n"
        f"STUDENT QUESTION: {question}\n\n"
        f"Answer fully and clearly — do not truncate. End with: '{resume}'"
    )
    a = await gemini_call(prompt, 900, 0.4, "ask_gemini")
    if not a or a == "__SAFETY__": return t(lang, "resume_fallback")
    save_chat(phone, "user", question)
    save_chat(phone, "assistant", a)
    return a


# =============================================================
#  GEMINI 4 — RAG CHAT
# =============================================================

async def ask_gemini_rag(phone, question, lang) -> str:
    if not GEMINI_KEY: return t(lang, "done")
    history = "".join(f"{r.upper()}: {m}\n" for r, m in get_chat_history(phone, 8))
    doc = (f"\nREFERENCE DOCUMENTS:\n{DOCUMENT_CONTEXT}\n"
           if DOCUMENT_CONTEXT.strip() and not DOCUMENT_CONTEXT.strip().startswith("[")
           else "(No documents linked yet — use your CBE knowledge.)")
    system = CBE_ASSISTANT_SYSTEM.format(document_context=doc, lang_instruction=_lang_instruction(lang))
    prompt = (
        f"{system}\n\nCONVERSATION HISTORY:\n{history}\n"
        f"STUDENT: {question}\n\n"
        f"EDUTENA — answer fully. Never truncate. "
        f"End with: '{t(lang, 'rag_menu_reminder')}'"
    )
    a = await gemini_call(prompt, 1600, 0.5, "rag_chat")
    if not a or a == "__SAFETY__": return t(lang, "error")
    save_chat(phone, "user", question)
    save_chat(phone, "assistant", a)
    return a


def get_chat_history(phone, limit=6):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("SELECT role,message FROM chat_history WHERE phone=%s ORDER BY created_at DESC LIMIT %s", (phone, limit))
    rows = cur.fetchall(); cur.close(); conn.close()
    return list(reversed(rows))

def save_chat(phone, role, message):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO chat_history(phone,role,message) VALUES(%s,%s,%s)", (phone, role, message))
    conn.commit(); cur.close(); conn.close()


# =============================================================
#  QUESTION DETECTION
# =============================================================

_MENU_COMMANDS = {"START","RESUME","CAREERS","MORE","HELP","MENU",
                  "1","2","3","4","5","6","7","8","9","10","EN","SW","LH","KI"}
_STRICT_MENU_STATES = {"LANG","LEVEL","JSS_GRADE","SENIOR_GRADE","TERM","SENIOR_PATHWAY",
                       "MATH","SCIENCE","SOCIAL","CREATIVE","TECH",
                       "CAREER_SELECT","CAREER_SELECT_ALL","MODE_SELECT"}

def is_cbe_question(text, state=""):
    if text.strip().upper() in _MENU_COMMANDS: return False
    if state in _STRICT_MENU_STATES: return False
    if len(text.strip()) > 3 and not text.strip().isdigit(): return True
    return False

def pause_state(phone, current_state, save_fn): save_fn(phone, "state", f"PAUSED_{current_state}")
def get_paused_state(state): return state[len("PAUSED_"):] if (state and state.startswith("PAUSED_")) else None


# =============================================================
#  PATHWAY CALCULATOR
# =============================================================

def calculate_pathway_from_scores(math, science, social, creative, technical):
    stem = (math or 0) + (science or 0) + (technical or 0)
    soc  = (social or 0) * 2
    arts = (creative or 0) * 2
    if stem >= soc and stem >= arts: return "STEM"
    elif soc >= stem and soc >= arts: return "Social Sciences"
    else: return "Arts & Sports Science"


# =============================================================
#  IMPROVEMENT SUGGESTIONS FALLBACK
# =============================================================

def get_improvement_suggestions(math, science, social, creative, technical, lang="en"):
    subj_en = {"Math": math, "Science": science, "Social Studies": social,
               "Creative Arts": creative, "Technical Skills": technical}
    subj_sw = {"Math": "Hisabati", "Science": "Sayansi", "Social Studies": "Sayansi Jamii",
               "Creative Arts": "Sanaa", "Technical Skills": "Ujuzi wa Kiufundi"}
    use_sw = lang in ("sw","lh","ki")
    weak = [subj_sw[k] if use_sw else k for k, v in subj_en.items() if (v or 0) <= 2]
    if not weak:
        return {"en": "Excellent! You are on track in all subjects. Keep it up!",
                "sw": "Vizuri sana! Uko vizuri katika masomo yote. Endelea!",
                "lh": "Wewe omulahi! Uko sawa kwa masomo yote. Endelea!",
                "ki": "Uria mwega! Uri mwega kwa masomo mothe. Endelea!"}.get(lang, "")
    w = ", ".join(weak)
    return {"en": f"Work harder on: {w}. Ask your teacher for extra help and practice daily.",
            "sw": f"Jaribu zaidi: {w}. Omba mwalimu msaada na fanya mazoezi kila siku.",
            "lh": f"Jaribu khale: {w}. Omba mwalimu msaada na fanya mazoezi.",
            "ki": f"Thiini guthoma: {w}. Uiguithia mwarimu na ithima mara nyingi."}.get(lang, "")


# =============================================================
#  CAREER BUILDERS
# =============================================================

def get_career_list_sms(pathway, lang, grade):
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    msg = t(lang, "career_hdr", pathway=pathway, grade=grade)
    for i, (name, demand, trend, *_) in enumerate(careers[:5], 1):
        msg += f"{i}. {name}\n   {demand} | {trend}\n"
    return msg + t(lang, "career_footer")

def get_all_careers_sms(pathway, lang):
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    msg = t(lang, "all_career_hdr", pathway=pathway)
    for i, (name, demand, *_) in enumerate(careers, 1):
        msg += f"{i}. {name} — {demand}\n"
    return msg + t(lang, "all_career_footer")

def get_career_detail_sms(pathway, career_idx, lang):
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    if career_idx < 0 or career_idx >= len(careers): return t(lang, "invalid_career")
    name, demand, trend, subjects, unis, reqs = careers[career_idx]
    return t(lang, "career_detail", name=name, demand=demand, trend=trend,
             subjects=subjects, unis=unis, reqs=reqs)

def get_career_ussd_list(pathway):
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    lines = f"{pathway}\nSelect Career:\n"
    for i, (name, demand, *_) in enumerate(careers[:6], 1):
        lines += f"{i}. {name[:16]} {demand}\n"
    return lines + "7. More careers"

def get_career_ussd_end(pathway, career_idx, lang):
    """Full career detail on USSD END screen — mirrors SMS detail."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    if career_idx < 0 or career_idx >= len(careers): return t(lang, "invalid_career")
    name, demand, trend, subjects, unis, reqs = careers[career_idx]
    return t(lang, "ussd_career_end", name=name, demand=demand, trend=trend,
             subjects=subjects, unis=unis, reqs=reqs)

def score_summary(math, science, social, creative, technical):
    lb = {4:"E", 3:"M", 2:"A", 1:"B"}
    return (f"M:{lb.get(math or 0,'?')} Sc:{lb.get(science or 0,'?')} "
            f"So:{lb.get(social or 0,'?')} Cr:{lb.get(creative or 0,'?')} "
            f"T:{lb.get(technical or 0,'?')}")


# =============================================================
#  SMS DB HELPERS
# =============================================================

SMS_ALLOWED = {"lang","level","grade","term","pathway","math","science","social",
               "creative","technical","career_interest","state","mode"}

def sms_save(phone, field, value):
    if field not in SMS_ALLOWED: raise ValueError(f"Invalid field: {field}")
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit(); cur.close(); conn.close()

def sms_get(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""SELECT phone,lang,level,grade,term,pathway,math,science,social,
                          creative,technical,career_interest,state,mode
                   FROM students WHERE phone=%s""", (phone,))
    s = cur.fetchone(); cur.close(); conn.close(); return s

async def send_reply(to_phone, message):
    try:
        sms_service.send(message=message, recipients=[to_phone], sender_id=SENDER_ID)
        print(f"[SMS] → {to_phone[:7]}****: {message[:120]}")
    except Exception as e:
        print(f"[SMS] failed: {e}")

def get_resume_prompt(original_state, lang, student):
    m = {"LANG": t(lang,"welcome_lang"), "LEVEL": t(lang,"welcome"),
         "JSS_GRADE": t(lang,"jss_grade"), "SENIOR_GRADE": t(lang,"senior_grade"),
         "TERM": t(lang,"term"), "SENIOR_PATHWAY": t(lang,"senior_pathway"),
         "MATH": t(lang,"rate_math",opts=RATING_OPTIONS_SMS),
         "SCIENCE": t(lang,"rate_science",opts=RATING_OPTIONS_SMS),
         "SOCIAL": t(lang,"rate_social",opts=RATING_OPTIONS_SMS),
         "CREATIVE": t(lang,"rate_creative",opts=RATING_OPTIONS_SMS),
         "TECH": t(lang,"rate_technical",opts=RATING_OPTIONS_SMS),
         "CAREER_SELECT": get_career_list_sms(student[5] or "", lang, student[3] or "")}
    return m.get(original_state, t(lang, "resume_fallback"))


# =============================================================
#  SMS WEBHOOK
# =============================================================

@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(from_: str = Form(..., alias="from"), text: str = Form(...)):
    phone = from_; text_clean = text.strip(); text_upper = text_clean.upper()
    print(f"[SMS] from {phone[:7]}****: {text_clean}")
    student = sms_get(phone)
    if text_upper == "START" or not student:
        sms_save(phone, "state", "LANG"); sms_save(phone, "mode", "")
        await send_reply(phone, t("en","welcome_lang")); return ""
    lang  = student[1] if student[1] in UI else "en"
    state = student[12]; mode = student[13] or ""
    if text_upper == "MENU":
        sms_save(phone, "state", "MODE_SELECT"); sms_save(phone, "mode", "")
        await send_reply(phone, t(lang,"mode_select")); return ""
    # RAG mode
    if state == "RAG_CHAT" or mode == "rag":
        if state != "RAG_CHAT": sms_save(phone, "state", "RAG_CHAT")
        await send_reply(phone, await ask_gemini_rag(phone, text_clean, lang)); return ""
    # RESUME
    if text_upper == "RESUME":
        orig = get_paused_state(state)
        if orig: sms_save(phone, "state", orig); await send_reply(phone, get_resume_prompt(orig, lang, student))
        else: await send_reply(phone, t(lang,"done"))
        return ""
    # Paused
    paused_orig = get_paused_state(state)
    if paused_orig:
        if is_cbe_question(text_clean):
            await send_reply(phone, await ask_gemini(phone, text_clean, lang=lang, context_state=paused_orig))
        else:
            await send_reply(phone, t(lang,"paused"))
        return ""
    # Mid-flow question
    if is_cbe_question(text_clean, state=state):
        pause_state(phone, state, sms_save)
        await send_reply(phone, await ask_gemini(phone, text_clean, lang=lang, context_state=state))
        return ""
    # MORE / CAREERS
    if text_upper == "MORE":
        pw = student[5]
        if not pw: await send_reply(phone, t(lang,"no_pathway")); return ""
        await send_reply(phone, get_all_careers_sms(pw, lang))
        sms_save(phone, "state", "CAREER_SELECT_ALL"); return ""
    if text_upper == "CAREERS":
        pw = student[5]; gr = student[3] or ""
        if not pw: await send_reply(phone, t(lang,"no_pathway")); return ""
        await send_reply(phone, get_career_list_sms(pw, lang, gr))
        sms_save(phone, "state", "CAREER_SELECT"); return ""
    try:
        if state == "LANG":
            chosen = LANG_MAP.get(text_clean)
            if not chosen: await send_reply(phone, t("en","welcome_lang")); return ""
            sms_save(phone, "lang", chosen); sms_save(phone, "state", "MODE_SELECT")
            await send_reply(phone, t(chosen, "mode_select"))
        elif state == "MODE_SELECT":
            if text_clean == "1":
                sms_save(phone,"mode","assessment"); sms_save(phone,"state","LEVEL")
                await send_reply(phone, t(lang,"welcome"))
            elif text_clean == "2":
                sms_save(phone,"mode","rag"); sms_save(phone,"state","RAG_CHAT")
                await send_reply(phone, t(lang,"rag_welcome"))
            else: await send_reply(phone, t(lang,"mode_err"))
        elif state == "LEVEL":
            if text_clean=="1": sms_save(phone,"level","JSS"); sms_save(phone,"state","JSS_GRADE"); await send_reply(phone,t(lang,"jss_grade"))
            elif text_clean=="2": sms_save(phone,"level","Senior"); sms_save(phone,"state","SENIOR_GRADE"); await send_reply(phone,t(lang,"senior_grade"))
            else: await send_reply(phone, t(lang,"level_err"))
        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(text_clean)
            if not g: await send_reply(phone,t(lang,"grade_err")); return ""
            sms_save(phone,"grade",g); sms_save(phone,"state","TERM"); await send_reply(phone,t(lang,"term"))
        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(text_clean)
            if not g: await send_reply(phone,t(lang,"grade_err")); return ""
            sms_save(phone,"grade",g); sms_save(phone,"state","SENIOR_PATHWAY"); await send_reply(phone,t(lang,"senior_pathway"))
        elif state == "TERM":
            tv = TERMS.get(text_clean)
            if not tv: await send_reply(phone,t(lang,"term_err")); return ""
            sms_save(phone,"term",tv); sms_save(phone,"state","MATH"); await send_reply(phone,t(lang,"rate_math",opts=RATING_OPTIONS_SMS))
        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(text_clean)
            if not chosen: await send_reply(phone,t(lang,"pathway_err")); return ""
            sms_save(phone,"pathway",chosen); sms_save(phone,"state","CAREER_SELECT")
            await send_reply(phone,get_career_list_sms(chosen,lang,student[3] or ""))
        elif state == "MATH":
            sc = RATING_MAP.get(text_clean)
            if not sc: await send_reply(phone,t(lang,"invalid_rating")); return ""
            sms_save(phone,"math",sc); sms_save(phone,"state","SCIENCE"); await send_reply(phone,t(lang,"rate_science",opts=RATING_OPTIONS_SMS))
        elif state == "SCIENCE":
            sc = RATING_MAP.get(text_clean)
            if not sc: await send_reply(phone,t(lang,"invalid_rating")); return ""
            sms_save(phone,"science",sc); sms_save(phone,"state","SOCIAL"); await send_reply(phone,t(lang,"rate_social",opts=RATING_OPTIONS_SMS))
        elif state == "SOCIAL":
            sc = RATING_MAP.get(text_clean)
            if not sc: await send_reply(phone,t(lang,"invalid_rating")); return ""
            sms_save(phone,"social",sc); sms_save(phone,"state","CREATIVE"); await send_reply(phone,t(lang,"rate_creative",opts=RATING_OPTIONS_SMS))
        elif state == "CREATIVE":
            sc = RATING_MAP.get(text_clean)
            if not sc: await send_reply(phone,t(lang,"invalid_rating")); return ""
            sms_save(phone,"creative",sc); sms_save(phone,"state","TECH"); await send_reply(phone,t(lang,"rate_technical",opts=RATING_OPTIONS_SMS))
        elif state == "TECH":
            sc = RATING_MAP.get(text_clean)
            if not sc: await send_reply(phone,t(lang,"invalid_rating")); return ""
            sms_save(phone,"technical",sc); s2 = sms_get(phone)
            gr = s2[3] or ""; tv = s2[4] or ""
            if gr == "Grade 9":
                pw = calculate_pathway_from_scores(s2[6],s2[7],s2[8],s2[9],s2[10])
                sms_save(phone,"pathway",pw); sms_save(phone,"state","DONE")
                await send_reply(phone, t(lang,"pathway_msg",pathway=pw))
            else:
                suggestions = await gemini_jss_suggestions(gr,tv,s2[6],s2[7],s2[8],s2[9],s2[10],lang)
                sms_save(phone,"state","DONE")
                await send_reply(phone, t(lang,"tracking_hdr",grade=gr,term=tv) + t(lang,"suggestion",suggestions=suggestions))
        elif state == "CAREER_SELECT":
            pw = student[5]
            if not pw: await send_reply(phone,t(lang,"no_pathway")); return ""
            if text_clean.isdigit() and 1 <= int(text_clean) <= 5:
                idx = int(text_clean)-1
                name,demand,trend,subjects,unis,reqs = SENIOR_CAREERS[pw][idx]
                sms_save(phone,"career_interest",name); sms_save(phone,"state","DONE")
                await send_reply(phone, get_career_detail_sms(pw,idx,lang))
                await send_reply(phone, await gemini_career_narrative(student[3] or "",pw,name,subjects,demand,lang))
            elif text_upper == "MORE":
                await send_reply(phone,get_all_careers_sms(pw,lang)); sms_save(phone,"state","CAREER_SELECT_ALL")
            else: await send_reply(phone,t(lang,"invalid_career"))
        elif state == "CAREER_SELECT_ALL":
            pw = student[5]
            if not pw: await send_reply(phone,t(lang,"no_pathway")); return ""
            if text_clean.isdigit() and 1 <= int(text_clean) <= 10:
                idx = int(text_clean)-1
                name,demand,trend,subjects,unis,reqs = SENIOR_CAREERS[pw][idx]
                sms_save(phone,"career_interest",name); sms_save(phone,"state","DONE")
                await send_reply(phone, get_career_detail_sms(pw,idx,lang))
                await send_reply(phone, await gemini_career_narrative(student[3] or "",pw,name,subjects,demand,lang))
            else: await send_reply(phone,t(lang,"invalid_career"))
        else:
            await send_reply(phone, t(lang,"done"))
    except Exception as e:
        print(f"[SMS] Error: {e}"); await send_reply(phone, t(lang,"error"))
    return ""


# =============================================================
#  USSD DB HELPERS
# =============================================================

USSD_ALLOWED = {"lang","level","grade","term","pathway","math","science","social",
                "creative","technical","career_interest","state","mode"}

def ussd_save(phone, field, value):
    if field not in USSD_ALLOWED: raise ValueError(f"Invalid field: {field}")
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO ussd_students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE ussd_students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit(); cur.close(); conn.close()

def ussd_get(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""SELECT phone,lang,level,grade,term,pathway,math,science,social,
                          creative,technical,career_interest,state,mode
                   FROM ussd_students WHERE phone=%s""", (phone,))
    s = cur.fetchone(); cur.close(); conn.close(); return s

def ussd_calculate_pathway(phone):
    s = ussd_get(phone)
    if not s: return None
    pw = calculate_pathway_from_scores(s[6],s[7],s[8],s[9],s[10])
    ussd_save(phone,"pathway",pw); return pw

def ussd_reset(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""UPDATE ussd_students
                   SET lang=NULL,level=NULL,grade=NULL,term=NULL,pathway=NULL,
                       math=NULL,science=NULL,social=NULL,creative=NULL,
                       technical=NULL,career_interest=NULL,mode=NULL,state='LANG'
                   WHERE phone=%s""", (phone,))
    conn.commit(); cur.close(); conn.close()

def con(text): return f"CON {text}"
def end(text): return f"END {text}"

def ussd_lang_screen():
    # Bilingual so ALL users can identify their language
    return con("Welcome / Karibu\nEduTena CBE\n\nChagua / Select:\n1. English\n2. Kiswahili\n3. Kiluhya\n4. Gĩkũyũ")


# =============================================================
#  USSD BACKGROUND SMS TASKS
# =============================================================

async def _sms_career_detail(phone, pathway, career_idx, lang, grade):
    try:
        await send_reply(phone, get_career_detail_sms(pathway, career_idx, lang))
        name, demand, trend, subjects, unis, reqs = SENIOR_CAREERS[pathway][career_idx]
        await send_reply(phone, await gemini_career_narrative(grade, pathway, name, subjects, demand, lang))
    except Exception as e: print(f"[USSD SMS career] {e}")

async def _sms_jss_suggestions(phone, grade, term, math, sci, soc, cre, tec, lang):
    try:
        suggestions = await gemini_jss_suggestions(grade, term, math, sci, soc, cre, tec, lang)
        msg = (f"EduTena CBE — {grade} | {term}\n━━━━━━━━━━━━━━━━━━━━\n\n"
               + t(lang,"suggestion", suggestions=suggestions)
               + "\n\n" + t(lang,"resume_fallback"))
        await send_reply(phone, msg)
    except Exception as e: print(f"[USSD SMS JSS] {e}")

async def _sms_rag_answer(phone, question, lang):
    try:
        header = {"en":"EduTena CBE Assistant\n━━━━━━━━━━━━━━━━━━━━\n\n",
                  "sw":"Msaidizi wa EduTena CBE\n━━━━━━━━━━━━━━━━━━━━\n\n",
                  "lh":"Msaidizi wa EduTena CBE\n━━━━━━━━━━━━━━━━━━━━\n\n",
                  "ki":"Msaidizi wa EduTena CBE\n━━━━━━━━━━━━━━━━━━━━\n\n"}.get(lang,"")
        await send_reply(phone, header + await ask_gemini_rag(phone, question, lang))
    except Exception as e: print(f"[USSD SMS RAG] {e}")


# =============================================================
#  USSD WEBHOOK
# =============================================================

@app.post("/ussd", response_class=PlainTextResponse)
async def ussd_callback(
    sessionId: str = Form(...), serviceCode: str = Form(...),
    phoneNumber: str = Form(...), text: str = Form(default="")
):
    phone = phoneNumber
    steps = [s.strip() for s in text.split("*")] if text else []
    step  = steps[-1] if steps else ""
    print(f"[USSD] session={sessionId} phone={phone[:7]}**** steps={steps}")
    student = ussd_get(phone)
    if not text or not student:
        ussd_save(phone, "state", "LANG"); return ussd_lang_screen()
    state = student[12]
    lang  = student[1] if student[1] in UI else "en"
    try:
        if state == "LANG":
            chosen = LANG_MAP.get(step)
            if not chosen: return con(t("en","invalid_lang"))
            ussd_save(phone,"lang",chosen); ussd_save(phone,"state","MODE_SELECT")
            return con(t(chosen,"mode_ussd_2"))

        elif state == "MODE_SELECT":
            if step == "1":
                ussd_save(phone,"mode","assessment"); ussd_save(phone,"state","LEVEL")
                return con(t(lang,"welcome"))
            elif step == "2":
                ussd_save(phone,"mode","rag"); ussd_save(phone,"state","USSD_RAG_TOPIC")
                return con(t(lang,"ussd_rag_menu"))
            else: return con(t(lang,"mode_ussd_err"))

        elif state == "USSD_RAG_TOPIC":
            topics = {
                "1": "Can you explain in detail what CBE (Competency Based Education) and CBC (Competency Based Curriculum) mean in Kenya? How is it different from what came before and why was it introduced?",
                "2": "Please explain the three Senior Secondary CBE pathways in detail — STEM, Social Sciences, and Arts & Sports Science. What subjects does each contain, what competencies are assessed, and what careers does each pathway lead to?",
                "3": "How does a student build a CBE competency portfolio for university entry? What should it include, how is it assessed, who reviews it, and which universities in Kenya already accept CBE portfolios?",
                "4": "What is the full difference between the old 844 KCSE system and the new CBE system in Kenya? How does university entry work now versus before? What does this mean for students currently in school?",
                "5": "How do Kenyan universities and colleges admit students under CBE? Which specific institutions have confirmed CBE portfolio pathways, what are their requirements, and how does the process work step by step?",
            }
            if step in topics:
                ussd_save(phone,"state","DONE")
                asyncio.create_task(_sms_rag_answer(phone, topics[step], lang))
                return end(t(lang,"ussd_rag_sending"))
            elif step == "6":
                ussd_save(phone,"state","DONE"); return end(t(lang,"ussd_rag_sms_tip"))
            else: return con(t(lang,"ussd_rag_menu"))

        elif state == "LEVEL":
            if step=="1": ussd_save(phone,"level","JSS"); ussd_save(phone,"state","JSS_GRADE"); return con(t(lang,"jss_grade"))
            elif step=="2": ussd_save(phone,"level","Senior"); ussd_save(phone,"state","SENIOR_GRADE"); return con(t(lang,"senior_grade"))
            else: return con(t(lang,"level_err"))

        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(step)
            if not g: return con(t(lang,"grade_err"))
            ussd_save(phone,"grade",g); ussd_save(phone,"state","TERM"); return con(t(lang,"term"))

        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(step)
            if not g: return con(t(lang,"grade_err"))
            ussd_save(phone,"grade",g); ussd_save(phone,"state","SENIOR_PATHWAY")
            return con(t(lang,"senior_pathway"))

        elif state == "TERM":
            tv = TERMS.get(step)
            if not tv: return con(t(lang,"term_err"))
            ussd_save(phone,"term",tv); ussd_save(phone,"state","MATH")
            return con(t(lang,"rate_math",opts=RATING_OPTIONS_USSD))

        elif state == "SENIOR_PATHWAY":
            chosen = PATHWAYS.get(step)
            if not chosen: return con(t(lang,"pathway_err"))
            ussd_save(phone,"pathway",chosen); ussd_save(phone,"state","USSD_CAREER_SELECT")
            return con(get_career_ussd_list(chosen))

        elif state == "MATH":
            sc = RATING_MAP.get(step)
            if not sc: return con(t(lang,"invalid_rating"))
            ussd_save(phone,"math",sc); ussd_save(phone,"state","SCIENCE"); return con(t(lang,"rate_science",opts=RATING_OPTIONS_USSD))

        elif state == "SCIENCE":
            sc = RATING_MAP.get(step)
            if not sc: return con(t(lang,"invalid_rating"))
            ussd_save(phone,"science",sc); ussd_save(phone,"state","SOCIAL"); return con(t(lang,"rate_social",opts=RATING_OPTIONS_USSD))

        elif state == "SOCIAL":
            sc = RATING_MAP.get(step)
            if not sc: return con(t(lang,"invalid_rating"))
            ussd_save(phone,"social",sc); ussd_save(phone,"state","CREATIVE"); return con(t(lang,"rate_creative",opts=RATING_OPTIONS_USSD))

        elif state == "CREATIVE":
            sc = RATING_MAP.get(step)
            if not sc: return con(t(lang,"invalid_rating"))
            ussd_save(phone,"creative",sc); ussd_save(phone,"state","TECH"); return con(t(lang,"rate_technical",opts=RATING_OPTIONS_USSD))

        elif state == "TECH":
            sc = RATING_MAP.get(step)
            if not sc: return con(t(lang,"invalid_rating"))
            ussd_save(phone,"technical",sc); s2 = ussd_get(phone)
            gr = s2[3] or ""; tv = s2[4] or ""
            m,sci,so,cr,tc = s2[6],s2[7],s2[8],s2[9],s2[10]
            if gr == "Grade 9":
                pw = calculate_pathway_from_scores(m,sci,so,cr,tc)
                ussd_save(phone,"pathway",pw); ussd_save(phone,"state","RESULT")
                scores_d = {"Math":m or 0,"Science":sci or 0,"Social":so or 0,"Creative":cr or 0,"Technical":tc or 0}
                top2 = sorted(scores_d.items(), key=lambda x:-x[1])[:2]
                top_str = " & ".join(n for n,_ in top2)
                return con(t(lang,"ussd_pathway_result", pathway=pw, top=top_str, summary=score_summary(m,sci,so,cr,tc)))
            else:
                asyncio.create_task(_sms_jss_suggestions(phone,gr,tv,m,sci,so,cr,tc,lang))
                ussd_save(phone,"state","DONE")
                scores_d = {"Math":m or 0,"Science":sci or 0,"Social Studies":so or 0,"Creative Arts":cr or 0,"Technical":tc or 0}
                sorted_sc = sorted(scores_d.items(),key=lambda x:-x[1])
                strongest = sorted_sc[0][0]
                weak_list = [n for n,v in sorted_sc if v<=2]
                weak_str  = ", ".join(weak_list[:2]) if weak_list else ("None" if lang=="en" else "Hakuna")
                return con(t(lang,"ussd_jss_result",grade=gr,term=tv,strongest=strongest,weak=weak_str))

        elif state == "RESULT":
            pw = student[5] or ussd_calculate_pathway(phone)
            if step=="1":
                ussd_save(phone,"state","USSD_CAREER_SELECT"); return con(get_career_ussd_list(pw))
            elif step=="2":
                careers = SENIOR_CAREERS.get(pw,[])
                lines = f"{pw} — All:\n"
                for i,(name,demand,*_) in enumerate(careers,1): lines += f"{i}. {name[:15]} {demand}\n"
                ussd_save(phone,"state","USSD_CAREER_SELECT_ALL"); return con(lines)
            elif step=="3": ussd_reset(phone); return ussd_lang_screen()
            else: return end(t(lang,"thank_you"))

        elif state == "USSD_CAREER_SELECT":
            pw = student[5]
            if step.isdigit() and 1 <= int(step) <= 6:
                idx = int(step)-1
                ussd_save(phone,"career_interest",SENIOR_CAREERS[pw][idx][0])
                ussd_save(phone,"state","DONE")
                asyncio.create_task(_sms_career_detail(phone,pw,idx,lang,student[3] or ""))
                # Show full detail on USSD END screen (same structure as SMS)
                return end(get_career_ussd_end(pw,idx,lang))
            elif step=="7":
                careers = SENIOR_CAREERS.get(pw,[])
                lines = f"{pw} — All:\n"
                for i,(name,demand,*_) in enumerate(careers,1): lines += f"{i}. {name[:15]} {demand}\n"
                ussd_save(phone,"state","USSD_CAREER_SELECT_ALL"); return con(lines)
            else: return con(get_career_ussd_list(pw))

        elif state == "USSD_CAREER_SELECT_ALL":
            pw = student[5]
            if step.isdigit() and 1 <= int(step) <= 10:
                idx = int(step)-1
                ussd_save(phone,"career_interest",SENIOR_CAREERS[pw][idx][0])
                ussd_save(phone,"state","DONE")
                asyncio.create_task(_sms_career_detail(phone,pw,idx,lang,student[3] or ""))
                return end(get_career_ussd_end(pw,idx,lang))
            else:
                careers = SENIOR_CAREERS.get(pw,[])
                lines = f"{pw} — All:\n"
                for i,(name,demand,*_) in enumerate(careers,1): lines += f"{i}. {name[:15]} {demand}\n"
                return con(lines)

        elif state == "DONE":
            if step=="1": ussd_reset(phone); return ussd_lang_screen()
            else: return end(t(lang,"thank_you"))

        else:
            ussd_reset(phone); return ussd_lang_screen()

    except Exception as e:
        print(f"[USSD] Error: {e}")
        return end(t(lang if student else "en","error"))
