from fastapi import FastAPI, Form
from fastapi.responses import PlainTextResponse
import psycopg2
from urllib.parse import urlparse
import os
import africastalking

app = FastAPI()

AT_USERNAME = os.getenv("AT_USERNAME")
AT_API_KEY  = os.getenv("AT_API_KEY")
africastalking.initialize(username=AT_USERNAME, api_key=AT_API_KEY)
sms_service = africastalking.SMS
SENDER_ID   = os.getenv("AT_SENDER_ID", "98449")

@app.get("/")
def root():
    return {"status": "EduTena API is running", "endpoints": {"sms": "/sms", "ussd": "/ussd"}}

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    url = urlparse(DATABASE_URL)
    return psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password, host=url.hostname, port=url.port)

def init_db():
    conn = get_connection()
    cur  = conn.cursor()

    # SMS table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students (
            phone TEXT PRIMARY KEY, lang TEXT DEFAULT 'en',
            level TEXT, grade TEXT, term TEXT,
            math INTEGER, science INTEGER, social INTEGER,
            creative INTEGER, technical INTEGER,
            pathway TEXT, state TEXT
        )
    """)
    for col in ["lang", "grade", "term"]:
        cur.execute(f"ALTER TABLE students ADD COLUMN IF NOT EXISTS {col} TEXT")

    # USSD table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ussd_students (
            phone TEXT PRIMARY KEY, lang TEXT DEFAULT 'en',
            level TEXT, grade TEXT, term TEXT,
            math INTEGER, science INTEGER, social INTEGER,
            creative INTEGER, technical INTEGER,
            pathway TEXT, state TEXT
        )
    """)
    for col in ["lang", "grade", "term"]:
        cur.execute(f"ALTER TABLE ussd_students ADD COLUMN IF NOT EXISTS {col} TEXT")

    # Clean corrupted rows where lang has wrong value (old schema leftover)
    cur.execute("""
        UPDATE students SET lang='en', state='LANG',
            level=NULL, grade=NULL, term=NULL,
            math=NULL, science=NULL, social=NULL,
            creative=NULL, technical=NULL, pathway=NULL
        WHERE lang NOT IN ('en','sw','lh','ki') OR lang IS NULL
    """)
    cur.execute("""
        UPDATE ussd_students SET lang='en', state='LANG',
            level=NULL, grade=NULL, term=NULL,
            math=NULL, science=NULL, social=NULL,
            creative=NULL, technical=NULL, pathway=NULL
        WHERE lang NOT IN ('en','sw','lh','ki') OR lang IS NULL
    """)

    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

# =============================================================
#  SHARED HELPERS
# =============================================================

# CBE Grade structure:
# JSS    → Grade 7, Grade 8, Grade 9
# Senior → Grade 10, Grade 11, Grade 12
#
# PURPOSE PER GRADE:
# Grade 7  → Track performance + improvement suggestions
# Grade 8  → Track performance + improvement suggestions
# Grade 9  → Pathway PREDICTION (STEM / Social Sciences / Arts)
# Grade 10 → Senior pathway tracking
# Grade 11 → Senior pathway tracking
# Grade 12 → Senior pathway tracking (final)

RATING_MAP = {"1": 4, "2": 3, "3": 2, "4": 1}
# 4=Exceeding, 3=Meeting, 2=Approaching, 1=Below

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

def get_improvement_suggestions(math, science, social, creative, technical, lang="en"):
    """
    For Grade 7 & 8: identify weak subjects (score <= 2 = Approaching or Below)
    and return suggestions in the chosen language.
    """
    weak = []
    subjects = {
        "Math": math, "Science": science, "Social Studies": social,
        "Creative Arts": creative, "Technical Skills": technical
    }
    subjects_sw = {
        "Math": "Hisabati", "Science": "Sayansi", "Social Studies": "Sayansi Jamii",
        "Creative Arts": "Sanaa", "Technical Skills": "Ujuzi wa Kiufundi"
    }
    for subj, score in subjects.items():
        if (score or 0) <= 2:
            weak.append(subjects_sw[subj] if lang == "sw" else subj)

    if not weak:
        msgs = {
            "en": "Excellent! Keep it up. You are on track in all subjects!",
            "sw": "Vizuri sana! Endelea hivyo. Uko vizuri katika masomo yote!",
            "lh": "Wewe omulahi! Endelea. Uko sawa kwa masomo yote!",
            "ki": "Uria mwega! Endelea. Uri mwega kwa masomo mothe!",
        }
        return msgs.get(lang, msgs["en"])

    weak_str = ", ".join(weak)
    msgs = {
        "en": f"Focus on improving: {weak_str}.\nStudy more, ask your teacher for help, and practice regularly.",
        "sw": f"Jaribu kuboresha: {weak_str}.\nSoma zaidi, uliza mwalimu wako msaada, na fanya mazoezi mara kwa mara.",
        "lh": f"Jaribu okhukoresa: {weak_str}.\nSoma khale, omba mwalimu msaada, na fanya mazoezi.",
        "ki": f"Thiini guthoma: {weak_str}.\nThoma na hinya, uiguithia mwarimu, na ithima mara nyingi.",
    }
    return msgs.get(lang, msgs["en"])


# =============================================================
#  LANGUAGE MENUS
# =============================================================

# Always start in English for language selection — then switch
LANG_SELECT_MSG = (
    "Welcome to EduTena CBE.\n"
    "Select Language:\n"
    "1. English\n"
    "2. Swahili\n"
    "3. Luhya\n"
    "4. Kikuyu"
)

LANG_MAP = {"1": "en", "2": "sw", "3": "lh", "4": "ki"}

SMS_MENU = {
    "en": {
        "lang_confirm":  "Language: English\nReply START to begin.",
        "welcome":       "EduTena CBE\nSelect Level:\n1. JSS (Grade 7-9)\n2. Senior (Grade 10-12)",
        "level_err":     "Invalid. Select:\n1. JSS (Gr 7-9)\n2. Senior (Gr 10-12)",
        "jss_grade":     "Select JSS Grade:\n1. Grade 7\n2. Grade 8\n3. Grade 9",
        "senior_grade":  "Select Senior Grade:\n1. Grade 10\n2. Grade 11\n3. Grade 12",
        "grade_err":     "Invalid. Select grade 1, 2, or 3.",
        "term":          "Select Term:\n1. Term 1\n2. Term 2\n3. Term 3",
        "term_err":      "Invalid. Select term 1, 2, or 3.",
        "math":          f"Rate Math:\n{RATING_OPTIONS_SMS}",
        "science":       f"Rate Science:\n{RATING_OPTIONS_SMS}",
        "social":        f"Rate Social Studies:\n{RATING_OPTIONS_SMS}",
        "creative":      f"Rate Creative Arts:\n{RATING_OPTIONS_SMS}",
        "tech":          f"Rate Technical Skills:\n{RATING_OPTIONS_SMS}",
        "invalid":       "Invalid. Reply 1, 2, 3, or 4.",
        # Grade 7 & 8 — performance tracking
        "tracking_hdr":  "Performance Summary\n{grade} | {term}\n",
        "suggestion":    "{suggestions}\nReply START to reassess.",
        # Grade 9 — pathway prediction
        "pathway_msg":   "Predicted Pathway:\n{pathway}\n(Based on Grade 9 scores)\nReply CAREERS to see options.",
        # Senior — tracking
        "senior_msg":    "Senior Performance\n{grade} | {term}\n{suggestions}\nReply START to reassess.",
        "careers_stem":  "STEM Careers:\n- Engineering\n- Data Science\n- Medicine\n- Architecture\n- Pharmacy",
        "careers_soc":   "Social Sciences:\n- Law\n- Psychology\n- Economics\n- Education\n- Journalism",
        "careers_arts":  "Arts & Sports:\n- Design\n- Music\n- Sports Science\n- Film & Media\n- Fashion",
        "no_pathway":    "Complete assessment first. Reply START.",
        "done":          "Assessment saved.\nReply CAREERS or START to reassess.",
    },
    "sw": {
        "lang_confirm":  "Lugha: Kiswahili\nJibu START kuanza.",
        "welcome":       "EduTena CBE\nChagua Kiwango:\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)",
        "level_err":     "Batili. Chagua:\n1. JSS\n2. Sekondari",
        "jss_grade":     "Chagua Darasa la JSS:\n1. Darasa 7\n2. Darasa 8\n3. Darasa 9",
        "senior_grade":  "Chagua Darasa la Sekondari:\n1. Darasa 10\n2. Darasa 11\n3. Darasa 12",
        "grade_err":     "Batili. Chagua darasa 1, 2, au 3.",
        "term":          "Chagua Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":      "Batili. Chagua muhula 1, 2, au 3.",
        "math":          "Kadiria Hisabati:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia\n4. Chini",
        "science":       "Kadiria Sayansi:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia\n4. Chini",
        "social":        "Kadiria Sayansi Jamii:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia\n4. Chini",
        "creative":      "Kadiria Sanaa:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia\n4. Chini",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Kuzidi\n2. Kukidhi\n3. Kukaribia\n4. Chini",
        "invalid":       "Batili. Jibu 1, 2, 3, au 4.",
        "tracking_hdr":  "Muhtasari wa Utendaji\n{grade} | {term}\n",
        "suggestion":    "{suggestions}\nJibu START kuanza upya.",
        "pathway_msg":   "Njia Inayotabirika:\n{pathway}\n(Kulingana na alama za Darasa 9)\nJibu CAREERS kuona kazi.",
        "senior_msg":    "Utendaji wa Sekondari\n{grade} | {term}\n{suggestions}\nJibu START kuanza upya.",
        "careers_stem":  "Kazi za STEM:\n- Uhandisi\n- Sayansi ya Data\n- Dawa\n- Usanifu\n- Famasia",
        "careers_soc":   "Sayansi Jamii:\n- Sheria\n- Saikolojia\n- Uchumi\n- Elimu\n- Uandishi",
        "careers_arts":  "Sanaa & Michezo:\n- Usanifu\n- Muziki\n- Sayansi ya Michezo\n- Filamu\n- Mitindo",
        "no_pathway":    "Maliza tathmini kwanza. Jibu START.",
        "done":          "Tathmini imehifadhiwa.\nJibu CAREERS au START kuanza upya.",
    },
    "lh": {
        "lang_confirm":  "Olulimi: Luhya\nJibu START okhuandaa.",
        "welcome":       "EduTena CBE\nSena Engufu:\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)",
        "level_err":     "Busia. Sena:\n1. JSS\n2. Sekondari",
        "jss_grade":     "Sena Okhufunda lwa JSS:\n1. Okhufunda 7\n2. Okhufunda 8\n3. Okhufunda 9",
        "senior_grade":  "Sena Okhufunda lwa Sekondari:\n1. Okhufunda 10\n2. Okhufunda 11\n3. Okhufunda 12",
        "grade_err":     "Busia. Sena okhufunda 1, 2, kamba 3.",
        "term":          "Sena Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":      "Busia. Sena muhula 1, 2, kamba 3.",
        "math":          "Kadiria Hesabu:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "science":       "Kadiria Sayansi:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "social":        "Kadiria Elimu ya Jamii:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "creative":      "Kadiria Sanaa:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "invalid":       "Busia. Jibu 1, 2, 3, kamba 4.",
        "tracking_hdr":  "Okusema kwa Masomo\n{grade} | {term}\n",
        "suggestion":    "{suggestions}\nJibu START okhuanza.",
        "pathway_msg":   "Njia Enyiseniwe:\n{pathway}\nJibu CAREERS okhuona emilimo.",
        "senior_msg":    "Okusema kwa Sekondari\n{grade} | {term}\n{suggestions}\nJibu START okhuanza.",
        "careers_stem":  "Emilimo ya STEM:\n- Uhandisi\n- Sayansi ya Data\n- Dawa\n- Usanifu\n- Famasia",
        "careers_soc":   "Elimu ya Jamii:\n- Sheria\n- Saikolojia\n- Uchumi\n- Elimu\n- Habari",
        "careers_arts":  "Sanaa & Michezo:\n- Usanifu\n- Muziki\n- Sayansi ya Michezo\n- Filamu\n- Mitindo",
        "no_pathway":    "Maliza tathmini kwanza. Jibu START.",
        "done":          "Tathmini yakhwira.\nJibu CAREERS kamba START okhuanza.",
    },
    "ki": {
        "lang_confirm":  "Rurimi: Kikuyu\nCookia START guthomia.",
        "welcome":       "EduTena CBE\nThura Kiwango:\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)",
        "level_err":     "Ti wegwaru. Thura:\n1. JSS\n2. Sekondari",
        "jss_grade":     "Thura Kiwango kia JSS:\n1. Kiwango 7\n2. Kiwango 8\n3. Kiwango 9",
        "senior_grade":  "Thura Kiwango kia Sekondari:\n1. Kiwango 10\n2. Kiwango 11\n3. Kiwango 12",
        "grade_err":     "Ti wegwaru. Thura kiwango 1, 2, kana 3.",
        "term":          "Thura Muhula:\n1. Muhula 1\n2. Muhula 2\n3. Muhula 3",
        "term_err":      "Ti wegwaru. Thura muhula 1, 2, kana 3.",
        "math":          "Kadiria Hesabu:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "science":       "Kadiria Sayansi:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "social":        "Kadiria Maarifa ya Jamii:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "creative":      "Kadiria Sanaa:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "invalid":       "Ti wegwaru. Cookia 1, 2, 3, kana 4.",
        "tracking_hdr":  "Mahitio ma Guthoma\n{grade} | {term}\n",
        "suggestion":    "{suggestions}\nCookia START gutomia.",
        "pathway_msg":   "Njia Yoneneirwo:\n{pathway}\nCookia CAREERS kuona mirimo.",
        "senior_msg":    "Mahitio ma Sekondari\n{grade} | {term}\n{suggestions}\nCookia START gutomia.",
        "careers_stem":  "Mirimo ya STEM:\n- Uhandisi\n- Sayansi ya Data\n- Dawa\n- Usanifu\n- Famasia",
        "careers_soc":   "Maarifa ya Jamii:\n- Sheria\n- Saikolojia\n- Uchumi\n- Elimu\n- Habari",
        "careers_arts":  "Sanaa & Michezo:\n- Usanifu\n- Muziki\n- Sayansi ya Michezo\n- Filamu\n- Mitindo",
        "no_pathway":    "Ithoma mbere. Cookia START.",
        "done":          "Ithomo niikuura.\nCookia CAREERS kana START gutomia.",
    },
}

# =============================================================
#  SMS DB HELPERS
# =============================================================
# Column order: phone(0) lang(1) level(2) grade(3) term(4)
#               math(5) science(6) social(7) creative(8)
#               technical(9) pathway(10) state(11)

SMS_ALLOWED = {
    "lang", "level", "grade", "term", "math", "science",
    "social", "creative", "technical", "pathway", "state"
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
        SELECT phone, lang, level, grade, term,
               math, science, social, creative, technical,
               pathway, state
        FROM students WHERE phone=%s
    """, (phone,))
    s = cur.fetchone(); cur.close(); conn.close()
    return s
    # phone(0) lang(1) level(2) grade(3) term(4)
    # math(5) science(6) social(7) creative(8) technical(9)
    # pathway(10) state(11)

def sms_calculate_pathway(phone):
    s = sms_get(phone)
    if not s: return None
    pathway = calculate_pathway_from_scores(s[5], s[6], s[7], s[8], s[9])
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

    # ── Always start with language selection ─────────────────────
    if not student or text_upper == "START":
        sms_save(phone, "state", "LANG")
        await send_reply(phone, LANG_SELECT_MSG)
        return ""

    lang  = student[1] if student[1] in SMS_MENU else "en"
    state = student[11]
    M     = SMS_MENU[lang]

    # ── MORE command — full career list with labour market data ──
    if text_upper == "MORE":
        pathway = student[10] if student else None
        grade   = student[3]  if student else ""
        if not pathway:
            pathway = sms_calculate_pathway(phone)
        if pathway:
            full_careers = format_senior_all_careers_sms(pathway, lang)
            await send_reply(phone, full_careers)
        else:
            await send_reply(phone, M["no_pathway"])
        return ""

    # ── CAREERS command ──────────────────────────────────────────
    if text_upper == "CAREERS":
        pathway = student[10]
        grade   = student[3] or ""
        if not pathway:
            pathway = sms_calculate_pathway(phone)
        if not pathway:
            await send_reply(phone, M["no_pathway"]); return ""
        if pathway == "STEM":          await send_reply(phone, M["careers_stem"])
        elif pathway == "Social Sciences": await send_reply(phone, M["careers_soc"])
        else:                          await send_reply(phone, M["careers_arts"])
        return ""

    try:
        # ── Language selection ───────────────────────────────────
        if state == "LANG":
            chosen = LANG_MAP.get(text_clean)
            if not chosen:
                await send_reply(phone, LANG_SELECT_MSG); return ""
            sms_save(phone, "lang", chosen)
            sms_save(phone, "state", "LEVEL")
            await send_reply(phone, SMS_MENU[chosen]["welcome"])

        # ── Level selection ──────────────────────────────────────
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

        # ── JSS Grade (7, 8, 9) ──────────────────────────────────
        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(text_clean)
            if not g:
                await send_reply(phone, M["grade_err"] + "\n" + M["jss_grade"]); return ""
            sms_save(phone, "grade", g)
            sms_save(phone, "state", "TERM")
            await send_reply(phone, M["term"])

        # ── Senior Grade (10, 11, 12) ────────────────────────────
        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(text_clean)
            if not g:
                await send_reply(phone, M["grade_err"] + "\n" + M["senior_grade"]); return ""
            sms_save(phone, "grade", g)
            sms_save(phone, "state", "TERM")
            await send_reply(phone, M["term"])

        # ── Term selection ───────────────────────────────────────
        elif state == "TERM":
            t = TERMS.get(text_clean)
            if not t:
                await send_reply(phone, M["term_err"] + "\n" + M["term"]); return ""
            sms_save(phone, "term", t)
            sms_save(phone, "state", "MATH")
            await send_reply(phone, M["math"])

        # ── Subject ratings ──────────────────────────────────────
        elif state == "MATH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['math']}"); return ""
            sms_save(phone, "math", score)
            sms_save(phone, "state", "SCIENCE")
            await send_reply(phone, M["science"])

        elif state == "SCIENCE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['science']}"); return ""
            sms_save(phone, "science", score)
            sms_save(phone, "state", "SOCIAL")
            await send_reply(phone, M["social"])

        elif state == "SOCIAL":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['social']}"); return ""
            sms_save(phone, "social", score)
            sms_save(phone, "state", "CREATIVE")
            await send_reply(phone, M["creative"])

        elif state == "CREATIVE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['creative']}"); return ""
            sms_save(phone, "creative", score)
            sms_save(phone, "state", "TECH")
            await send_reply(phone, M["tech"])

        elif state == "TECH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['tech']}"); return ""
            sms_save(phone, "technical", score)
            sms_save(phone, "state", "DONE")

            # Reload student with all scores
            s     = sms_get(phone)
            grade = s[3] or ""
            term  = s[4] or ""
            math, science, social, creative, technical = s[5], s[6], s[7], s[8], s[9]

            # ── Grade 9: Predict pathway ─────────────────────────
            if grade == "Grade 9":
                pathway = sms_calculate_pathway(phone)
                await send_reply(phone, M["pathway_msg"].format(pathway=pathway))

            # ── Grade 7 & 8: Track + suggest improvements ────────
            elif grade in ("Grade 7", "Grade 8"):
                suggestions = get_improvement_suggestions(
                    math, science, social, creative, technical, lang
                )
                header = M["tracking_hdr"].format(grade=grade, term=term)
                await send_reply(phone, header + M["suggestion"].format(suggestions=suggestions))

            # ── Senior Grades 10-12: Track + pathway careers ─────
            else:
                # 1. Performance suggestions
                suggestions = get_improvement_suggestions(
                    math, science, social, creative, technical, lang
                )
                await send_reply(phone, M["senior_msg"].format(
                    grade=grade, term=term, suggestions=suggestions
                ))
                # 2. Career options with Kenya labour market ratings
                pathway = student[10] or sms_calculate_pathway(phone)
                career_msg = format_senior_careers_sms(pathway, grade, lang)
                await send_reply(phone, career_msg)

        else:
            await send_reply(phone, M["done"])

    except Exception as e:
        print(f"[SMS] Error: {e}")
        await send_reply(phone, "Error. Reply START to try again.")

    return ""


# =============================================================
#  USSD DB HELPERS
# =============================================================
# Column order: phone(0) lang(1) level(2) grade(3) term(4)
#               math(5) science(6) social(7) creative(8)
#               technical(9) pathway(10) state(11)

USSD_ALLOWED = {
    "lang", "level", "grade", "term", "math", "science",
    "social", "creative", "technical", "pathway", "state"
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
        SELECT phone, lang, level, grade, term,
               math, science, social, creative, technical,
               pathway, state
        FROM ussd_students WHERE phone=%s
    """, (phone,))
    s = cur.fetchone(); cur.close(); conn.close()
    return s
    # phone(0) lang(1) level(2) grade(3) term(4)
    # math(5) science(6) social(7) creative(8) technical(9)
    # pathway(10) state(11)

def ussd_calculate_pathway(phone):
    s = ussd_get(phone)
    if not s: return None
    pathway = calculate_pathway_from_scores(s[5], s[6], s[7], s[8], s[9])
    ussd_save(phone, "pathway", pathway)
    return pathway

def ussd_reset(phone):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE ussd_students
        SET lang=NULL, level=NULL, grade=NULL, term=NULL,
            math=NULL, science=NULL, social=NULL, creative=NULL,
            technical=NULL, pathway=NULL, state='LANG'
        WHERE phone=%s
    """, (phone,))
    conn.commit(); cur.close(); conn.close()

def con(text):  return f"CON {text}"
def end(text):  return f"END {text}"
def rating_screen(subject):  return con(f"Rate {subject}:\n{RATING_OPTIONS_USSD}")
def invalid_rating(subject): return con(f"Invalid. Rate {subject}:\n{RATING_OPTIONS_USSD}")

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
    print(f"[USSD] *384*59423# | session={sessionId} | phone={phone[:7]}**** | steps={steps}")

    student = ussd_get(phone)

    # ── Always start with English language selection ─────────────
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

    state = student[11]
    lang  = student[1] if student[1] in SMS_MENU else "en"

    try:
        # ── Language selection ───────────────────────────────────
        if state == "LANG":
            chosen = LANG_MAP.get(step)
            if not chosen:
                return con("Invalid.\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            ussd_save(phone, "lang", chosen)
            ussd_save(phone, "state", "LEVEL")
            lang = chosen
            if lang == "en":
                return con("EduTena CBE\nSelect Level:\n1. JSS (Grade 7-9)\n2. Senior (Grade 10-12)")
            elif lang == "sw":
                return con("EduTena CBE\nChagua Kiwango:\n1. JSS (Darasa 7-9)\n2. Sekondari (Darasa 10-12)")
            elif lang == "lh":
                return con("EduTena CBE\nSena Engufu:\n1. JSS (Okhufunda 7-9)\n2. Sekondari (Okhufunda 10-12)")
            else:
                return con("EduTena CBE\nThura Kiwango:\n1. JSS (Kiwango 7-9)\n2. Sekondari (Kiwango 10-12)")

        # ── Level selection ──────────────────────────────────────
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

        # ── JSS Grade ────────────────────────────────────────────
        elif state == "JSS_GRADE":
            g = JSS_GRADES.get(step)
            if not g:
                return con("Invalid.\n1. Grade 7\n2. Grade 8\n3. Grade 9")
            ussd_save(phone, "grade", g)
            ussd_save(phone, "state", "TERM")
            return con("Select Term:\n1. Term 1\n2. Term 2\n3. Term 3")

        # ── Senior Grade ─────────────────────────────────────────
        elif state == "SENIOR_GRADE":
            g = SENIOR_GRADES.get(step)
            if not g:
                return con("Invalid.\n1. Grade 10\n2. Grade 11\n3. Grade 12")
            ussd_save(phone, "grade", g)
            ussd_save(phone, "state", "TERM")
            return con("Select Term:\n1. Term 1\n2. Term 2\n3. Term 3")

        # ── Term selection ───────────────────────────────────────
        elif state == "TERM":
            t = TERMS.get(step)
            if not t:
                return con("Invalid.\n1. Term 1\n2. Term 2\n3. Term 3")
            ussd_save(phone, "term", t)
            ussd_save(phone, "state", "MATH")
            return rating_screen("Math")

        # ── Subject ratings ──────────────────────────────────────
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
            math, science, social, creative, technical = s[5], s[6], s[7], s[8], s[9]

            # ── Grade 9: Predict pathway ─────────────────────────
            if grade == "Grade 9":
                pathway = ussd_calculate_pathway(phone)
                ussd_save(phone, "state", "RESULT")
                return con(
                    f"Predicted Pathway:\n{pathway}\n\n"
                    "1. View Careers\n"
                    "2. Restart\n"
                    "3. Exit"
                )

            # ── Grade 7 & 8: Performance tracking ────────────────
            elif grade in ("Grade 7", "Grade 8"):
                suggestions = get_improvement_suggestions(math, science, social, creative, technical, lang)
                ussd_save(phone, "state", "DONE")
                # Trim for USSD screen
                short = suggestions[:100] + "..." if len(suggestions) > 100 else suggestions
                return con(
                    f"{grade} | {term}\n"
                    f"{short}\n\n"
                    "1. Restart\n"
                    "2. Exit"
                )

            # ── Senior: Performance tracking + career demand ──────
            else:
                suggestions = get_improvement_suggestions(math, science, social, creative, technical, lang)
                pathway     = ussd_calculate_pathway(phone)
                ussd_save(phone, "state", "SENIOR_CAREERS")
                short = suggestions[:80] + "..." if len(suggestions) > 80 else suggestions
                return con(
                    f"{grade} | {term}\n"
                    f"{short}\n\n"
                    "1. View Career Demand\n"
                    "2. Restart\n"
                    "3. Exit"
                )

        # ── RESULT menu (Grade 9 pathway) ────────────────────────
        elif state == "RESULT":
            pathway = student[10] or ussd_calculate_pathway(phone)
            if step == "1":
                ussd_save(phone, "state", "CAREERS")
                if pathway == "STEM":
                    return con("STEM Careers:\n- Engineering\n- Data Science\n- Medicine\n- Architecture\n- Pharmacy\n\n0. Back")
                elif pathway == "Social Sciences":
                    return con("Social Sciences:\n- Law\n- Psychology\n- Economics\n- Education\n- Journalism\n\n0. Back")
                else:
                    return con("Arts & Sports:\n- Design\n- Music\n- Sports Science\n- Film & Media\n- Fashion\n\n0. Back")
            elif step == "2":
                ussd_reset(phone)
                return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            elif step == "3":
                return end("Thank you for using EduTena CBE.\nGood luck!")
            else:
                return con(f"Pathway: {pathway}\n\n1. View Careers\n2. Restart\n3. Exit")

        # ── SENIOR_CAREERS menu — career demand per pathway ────────
        elif state == "SENIOR_CAREERS":
            pathway = student[10] or ussd_calculate_pathway(phone)
            if step == "1":
                ussd_save(phone, "state", "SENIOR_CAREERS_LIST")
                career_lines = format_senior_careers_ussd(pathway)
                return con(career_lines)
            elif step == "2":
                ussd_reset(phone)
                return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            else:
                return end("Thank you for using EduTena CBE.\nGood luck!")

        # ── SENIOR_CAREERS_LIST — showing career demand list ─────────
        elif state == "SENIOR_CAREERS_LIST":
            if step == "0":
                ussd_save(phone, "state", "SENIOR_CAREERS")
                return con("1. View Career Demand\n2. Restart\n3. Exit")
            else:
                return end("Thank you!\nSend START via SMS for full career details.")

        # ── DONE menu (Grade 7, 8, Senior) ───────────────────────
        elif state == "DONE":
            if step == "1":
                ussd_reset(phone)
                return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")
            else:
                return end("Thank you for using EduTena CBE.\nGood luck!")

        # ── CAREERS ──────────────────────────────────────────────
        elif state == "CAREERS":
            if step == "0":
                pathway = student[10]
                ussd_save(phone, "state", "RESULT")
                return con(f"Pathway: {pathway}\n\n1. View Careers\n2. Restart\n3. Exit")
            else:
                return end("Thank you for using EduTena CBE.\nGood luck!")

        else:
            ussd_reset(phone)
            return con("Welcome to EduTena CBE\nSelect Language:\n1. English\n2. Swahili\n3. Luhya\n4. Kikuyu")

    except Exception as e:
        print(f"[USSD] Error: {e}")
        return end("Something went wrong. Please dial again.")


# =============================================================
#  KENYA LABOUR MARKET DATA 2025
#  Source: Kenya Labour Market Information System (KLMIS),
#          MyJobMag 2025, By Appointment Africa 2025
#
#  Demand rating scale:
#  ⭐⭐⭐⭐⭐  = Very High Demand  (5★)
#  ⭐⭐⭐⭐    = High Demand       (4★)
#  ⭐⭐⭐      = Moderate Demand   (3★)
#  ⭐⭐        = Lower Demand      (2★)
# =============================================================

SENIOR_CAREERS = {
    "STEM": [
        # career, demand stars, avg salary KES/month, growth trend
        ("Software Engineer",        "5★", "80,000-200,000",  "↑ Growing fast — Silicon Savannah"),
        ("Data Analyst/Scientist",   "5★", "70,000-180,000",  "↑ Highest demand in tech 2025"),
        ("Cybersecurity Specialist", "5★", "90,000-250,000",  "↑ Critical shortage nationwide"),
        ("Renewable Energy Engineer","4★", "60,000-150,000",  "↑ Kenya green energy boom"),
        ("Pharmacist",               "4★", "50,000-120,000",  "↑ Healthcare sector growing"),
        ("Civil/Structural Engineer","4★", "55,000-130,000",  "→ Steady, housing demand high"),
        ("Medical Doctor",           "4★", "80,000-300,000",  "↑ Public & private sector need"),
        ("ICT Support Specialist",   "3★", "30,000-70,000",   "→ Steady demand countrywide"),
        ("Laboratory Technician",    "3★", "25,000-55,000",   "→ Moderate, mostly public sector"),
        ("Architect",                "3★", "50,000-120,000",  "→ Urban development projects"),
    ],
    "Social Sciences": [
        ("Accountant/Auditor",       "5★", "50,000-150,000",  "↑ Most advertised job in Kenya 2025"),
        ("Finance Manager",          "5★", "80,000-200,000",  "↑ Fintech growth driving demand"),
        ("Lawyer/Advocate",          "4★", "60,000-250,000",  "↑ Legal services expanding"),
        ("Digital Marketer",         "4★", "35,000-120,000",  "↑ 17% of all job postings 2025"),
        ("Sales Executive",          "4★", "30,000-100,000",  "↑ Top 3 most hired role Kenya"),
        ("Human Resource Manager",   "3★", "45,000-120,000",  "→ Steady across all sectors"),
        ("Economist",                "3★", "50,000-130,000",  "→ Government & research orgs"),
        ("Teacher/Educator",         "3★", "25,000-60,000",   "→ High need, CBC transition"),
        ("Journalist/Media",         "2★", "20,000-60,000",   "↓ Print declining, digital rising"),
        ("Psychologist/Counsellor",  "3★", "30,000-80,000",   "↑ Mental health awareness growing"),
    ],
    "Arts & Sports Science": [
        ("Graphic Designer/UI-UX",   "4★", "35,000-120,000",  "↑ Digital economy driving demand"),
        ("Film & Content Creator",   "4★", "20,000-150,000",  "↑ YouTube/social media economy"),
        ("Sports Coach/Manager",     "3★", "25,000-80,000",   "→ Growing with sports academies"),
        ("Fashion Designer",         "2★", "15,000-80,000",   "→ Niche but growing locally"),
        ("Musician/Performer",       "2★", "Variable",        "→ Competitive, digital revenue"),
        ("Interior Designer",        "3★", "30,000-100,000",  "↑ Urban housing boom"),
        ("Physiotherapist",          "3★", "35,000-90,000",   "↑ Sports & healthcare sector"),
        ("Tourism/Hospitality Mgr",  "3★", "30,000-100,000",  "↑ Post-COVID recovery strong"),
        ("Beauty & Wellness Therapist","3★","20,000-70,000",  "↑ TVET boom, rising demand"),
        ("Community Development",    "2★", "25,000-60,000",   "→ NGO-driven employment"),
    ]
}

def format_senior_careers_sms(pathway: str, grade: str, lang: str) -> str:
    """
    Format career options with Kenya labour market ratings for SMS.
    Shows top 5 careers to fit SMS length limits.
    """
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])

    headers = {
        "en": f"Career Options | {grade}\nKenya Labour Market 2025\n",
        "sw": f"Chaguzi za Kazi | {grade}\nSoko la Kazi Kenya 2025\n",
        "lh": f"Emilimo | {grade}\nSoko ya Kazi Kenya 2025\n",
        "ki": f"Mirimo | {grade}\nSoko ria Kazi Kenya 2025\n",
    }
    demand_labels = {
        "en": "Demand", "sw": "Mahitaji", "lh": "Haja", "ki": "Hitaji"
    }
    salary_labels = {
        "en": "Salary/mo", "sw": "Mshahara/mwezi", "lh": "Mishahara", "ki": "Mshahara"
    }

    msg = headers.get(lang, headers["en"])
    # Show top 5 by demand (already sorted highest first)
    for name, stars, salary, trend in careers[:5]:
        msg += f"\n{name}\n{demand_labels.get(lang,'Demand')}: {stars}\n{salary_labels.get(lang,'Salary')}: KES {salary}\n{trend}\n"

    footer = {
        "en": "\nReply MORE for all careers or START to reassess.",
        "sw": "\nJibu MORE kwa kazi zote au START kuanza upya.",
        "lh": "\nJibu MORE kwa emilimo yote kamba START okhuanza.",
        "ki": "\nCookia MORE mirimo yothe kana START gutomia.",
    }
    msg += footer.get(lang, footer["en"])
    return msg

def format_senior_careers_ussd(pathway: str) -> str:
    """
    Format career options for USSD screen — compact version.
    Shows top 6 careers with demand rating only (no salary — too long).
    """
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    lines = "Top Careers | Kenya 2025\n"
    for name, stars, salary, trend in careers[:6]:
        # Shorten name to fit USSD screen
        short_name = name[:18] if len(name) > 18 else name
        lines += f"{short_name}: {stars}\n"
    lines += "\n1. Full Details (SMS)\n2. Restart\n3. Exit"
    return lines

def format_senior_all_careers_sms(pathway: str, lang: str) -> str:
    """All 10 careers — sent as reply to MORE command."""
    careers = SENIOR_CAREERS.get(pathway, SENIOR_CAREERS["STEM"])
    demand_label = {"en": "Demand", "sw": "Mahitaji", "lh": "Haja", "ki": "Hitaji"}

    msg = f"All {pathway} Careers\nKenya Labour Market 2025\n"
    for name, stars, salary, trend in careers:
        msg += f"\n• {name} {stars}\n  KES {salary}\n  {trend}\n"
    return msg


# =============================================================
#  PATCH: Add MORE command handler to SMS webhook
#  and update Senior result to use new career data
# =============================================================

@app.post("/sms/more", response_class=PlainTextResponse)
async def sms_more(from_: str = Form(..., alias="from"), text: str = Form(...)):
    """
    Handles MORE command — sends all 10 careers with full labour market data.
    AT routes this from the main /sms webhook via text=="MORE".
    """
    pass  # Handled inside main /sms webhook below — this is a placeholder
