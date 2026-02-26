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
    return {
        "status": "EduTena API is running",
        "endpoints": {
            "sms":  "/sms",
            "ussd": "/ussd"
        }
    }

# ---------- DATABASE ----------
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

    # SMS students table
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
    cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'en'")

    # USSD students table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ussd_students (
            phone      TEXT PRIMARY KEY,
            level      TEXT,
            math       INTEGER,
            science    INTEGER,
            social     INTEGER,
            creative   INTEGER,
            technical  INTEGER,
            pathway    TEXT,
            state      TEXT
        )
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

# CBC 4-level performance rating
# 4 = Exceeding Expectation
# 3 = Meeting Expectation
# 2 = Approaching Expectation
# 1 = Below Expectation
RATING_MAP = {
    "1": 4,  # Exceeding Expectation
    "2": 3,  # Meeting Expectation
    "3": 2,  # Approaching Expectation
    "4": 1,  # Below Expectation
}

# Rating labels for SMS (short)
RATING_OPTIONS_SMS = (
    "1. Exceeding Expectation\n"
    "2. Meeting Expectation\n"
    "3. Approaching Expectation\n"
    "4. Below Expectation"
)

# Rating labels for USSD (short to fit screen)
RATING_OPTIONS_USSD = (
    "1. Exceeding\n"
    "2. Meeting\n"
    "3. Approaching\n"
    "4. Below"
)

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
#  SMS SECTION
# =============================================================

SMS_ALLOWED_FIELDS = {
    "level", "math", "science", "social", "creative",
    "technical", "pathway", "state", "lang"
}

def sms_save(phone, field, value):
    if field not in SMS_ALLOWED_FIELDS:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("INSERT INTO students(phone) VALUES(%s) ON CONFLICT DO NOTHING", (phone,))
    cur.execute(f"UPDATE students SET {field}=%s WHERE phone=%s", (value, phone))
    conn.commit()
    cur.close()
    conn.close()

def sms_get(phone):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM students WHERE phone=%s", (phone,))
    student = cur.fetchone()
    cur.close()
    conn.close()
    return student
    # phone(0) level(1) math(2) science(3) social(4)
    # creative(5) technical(6) pathway(7) state(8) lang(9)

def sms_calculate_pathway(phone):
    student = sms_get(phone)
    if not student:
        return None
    _, _, math, science, social, creative, technical, _, _, _ = student
    pathway = calculate_pathway_from_scores(math, science, social, creative, technical)
    sms_save(phone, "pathway", pathway)
    return pathway

async def send_reply(to_phone: str, message: str):
    try:
        response = sms_service.send(
            message=message,
            recipients=[to_phone],
            sender_id=SENDER_ID
        )
        print(f"[SMS] reply to {to_phone[:7]}****: {message[:80]}")
        print("AT:", response)
    except Exception as e:
        print(f"[SMS] failed: {str(e)}")

# SMS Language menus
SMS_MENU = {
    "en": {
        "lang_confirm":  "Language: English ✅\nReply START to begin.",
        "welcome":       "Welcome to EduTena CBE.\nSelect Level:\n1. JSS\n2. Senior\n\nChange language:\nSW=Swahili LH=Luhya KI=Kikuyu",
        "level_err":     "Invalid. Select:\n1. JSS\n2. Senior",
        "math":          f"Rate Math performance:\n{RATING_OPTIONS_SMS}",
        "science":       f"Rate Science performance:\n{RATING_OPTIONS_SMS}",
        "social":        f"Rate Social Studies:\n{RATING_OPTIONS_SMS}",
        "creative":      f"Rate Creative Arts:\n{RATING_OPTIONS_SMS}",
        "tech":          f"Rate Technical Skills:\n{RATING_OPTIONS_SMS}",
        "invalid":       "Invalid. Reply 1, 2, 3, or 4.",
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
        "math":          "Kadiria Hisabati:\n1. Kuzidi Matarajio\n2. Kukidhi Matarajio\n3. Kukaribia Matarajio\n4. Chini ya Matarajio",
        "science":       "Kadiria Sayansi:\n1. Kuzidi Matarajio\n2. Kukidhi Matarajio\n3. Kukaribia Matarajio\n4. Chini ya Matarajio",
        "social":        "Kadiria Sayansi Jamii:\n1. Kuzidi Matarajio\n2. Kukidhi Matarajio\n3. Kukaribia Matarajio\n4. Chini ya Matarajio",
        "creative":      "Kadiria Sanaa:\n1. Kuzidi Matarajio\n2. Kukidhi Matarajio\n3. Kukaribia Matarajio\n4. Chini ya Matarajio",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Kuzidi Matarajio\n2. Kukidhi Matarajio\n3. Kukaribia Matarajio\n4. Chini ya Matarajio",
        "invalid":       "Batili. Jibu 1, 2, 3, au 4.",
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
        "math":          "Kadiria Hesabu:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "science":       "Kadiria Sayansi:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "social":        "Kadiria Elimu ya Jamii:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "creative":      "Kadiria Sanaa:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Okhupiha\n2. Okhufika\n3. Okhuneela\n4. Wansi",
        "invalid":       "Busia. Jibu 1, 2, 3, kamba 4.",
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
        "math":          "Kadiria Hesabu:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "science":       "Kadiria Sayansi:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "social":        "Kadiria Maarifa ya Jamii:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "creative":      "Kadiria Sanaa:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "tech":          "Kadiria Ujuzi wa Kiufundi:\n1. Gucokia\n2. Gufika\n3. Guserekania\n4. Hasi",
        "invalid":       "Ti wegwaru. Cookia 1, 2, 3, kana 4.",
        "pathway_msg":   "Njia Yoneneirwo:\n{pathway}\nCookia CAREERS kuona mirimo.",
        "careers_stem":  "Mirimo ya STEM:\n1. Uhandisi\n2. Sayansi ya Data\n3. Dawa",
        "careers_soc":   "Mirimo ya Jamii:\n1. Sheria\n2. Saikolojia\n3. Uchumi",
        "careers_arts":  "Mirimo ya Sanaa:\n1. Usanifu\n2. Muziki\n3. Michezo",
        "no_pathway":    "Ithoma mbere. Cookia START.",
        "done":          "Ithomo niikuura.\nCookia CAREERS mirimo kana START gutomia.",
    },
}

LANG_TRIGGERS = {"EN": "en", "SW": "sw", "LH": "lh", "KI": "ki"}

# ---------- SMS WEBHOOK ----------
@app.post("/sms", response_class=PlainTextResponse)
async def receive_sms(
    from_: str = Form(..., alias="from"),
    text:  str = Form(...)
):
    phone      = from_
    text_clean = text.strip()
    text_upper = text_clean.upper()
    print(f"[SMS] from {phone[:7]}****: {text_clean}")

    student = sms_get(phone)

    # Language switch
    if text_upper in LANG_TRIGGERS:
        new_lang = LANG_TRIGGERS[text_upper]
        sms_save(phone, "lang", new_lang)
        await send_reply(phone, SMS_MENU[new_lang]["lang_confirm"])
        return ""

    lang = "en"
    if student and len(student) >= 10 and student[9]:
        lang = student[9]
    M = SMS_MENU[lang]

    if text_upper == "START" or not student:
        sms_save(phone, "state", "LEVEL")
        sms_save(phone, "lang", lang)
        await send_reply(phone, M["welcome"])
        return ""

    state   = student[8]
    pathway = student[7]

    if text_upper == "CAREERS":
        if not pathway:
            pathway = sms_calculate_pathway(phone)
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

    try:
        if state == "LEVEL":
            if text_clean == "1":
                sms_save(phone, "level", "JSS")
            elif text_clean == "2":
                sms_save(phone, "level", "Senior")
            else:
                await send_reply(phone, M["level_err"])
                return ""
            sms_save(phone, "state", "MATH")
            await send_reply(phone, M["math"])

        elif state == "MATH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['math']}")
                return ""
            sms_save(phone, "math", score)
            sms_save(phone, "state", "SCIENCE")
            await send_reply(phone, M["science"])

        elif state == "SCIENCE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['science']}")
                return ""
            sms_save(phone, "science", score)
            sms_save(phone, "state", "SOCIAL")
            await send_reply(phone, M["social"])

        elif state == "SOCIAL":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['social']}")
                return ""
            sms_save(phone, "social", score)
            sms_save(phone, "state", "CREATIVE")
            await send_reply(phone, M["creative"])

        elif state == "CREATIVE":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['creative']}")
                return ""
            sms_save(phone, "creative", score)
            sms_save(phone, "state", "TECH")
            await send_reply(phone, M["tech"])

        elif state == "TECH":
            score = RATING_MAP.get(text_clean)
            if not score:
                await send_reply(phone, f"{M['invalid']}\n{M['tech']}")
                return ""
            sms_save(phone, "technical", score)
            sms_save(phone, "state", "DONE")
            pathway = sms_calculate_pathway(phone)
            await send_reply(phone, M["pathway_msg"].format(pathway=pathway))

        else:
            await send_reply(phone, M["done"])

    except Exception as e:
        print(f"[SMS] Error: {e}")
        await send_reply(phone, "Error. Reply START to try again.")

    return ""


# =============================================================
#  USSD SECTION
# =============================================================

USSD_ALLOWED_FIELDS = {
    "level", "math", "science", "social",
    "creative", "technical", "pathway", "state"
}

def ussd_save(phone, field, value):
    if field not in USSD_ALLOWED_FIELDS:
        raise ValueError(f"Invalid field: {field}")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO ussd_students(phone) VALUES(%s) ON CONFLICT DO NOTHING",
        (phone,)
    )
    cur.execute(
        f"UPDATE ussd_students SET {field}=%s WHERE phone=%s",
        (value, phone)
    )
    conn.commit()
    cur.close()
    conn.close()

def ussd_get(phone):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM ussd_students WHERE phone=%s", (phone,))
    student = cur.fetchone()
    cur.close()
    conn.close()
    return student
    # phone(0) level(1) math(2) science(3) social(4)
    # creative(5) technical(6) pathway(7) state(8)

def ussd_calculate_pathway(phone):
    student = ussd_get(phone)
    if not student:
        return None
    _, _, math, science, social, creative, technical, _, _ = student
    pathway = calculate_pathway_from_scores(math, science, social, creative, technical)
    ussd_save(phone, "pathway", pathway)
    return pathway

def ussd_reset(phone):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE ussd_students
        SET level=NULL, math=NULL, science=NULL, social=NULL,
            creative=NULL, technical=NULL, pathway=NULL, state='LEVEL'
        WHERE phone=%s
    """, (phone,))
    conn.commit()
    cur.close()
    conn.close()

def con(text):
    return f"CON {text}"

def end(text):
    return f"END {text}"

def rating_screen(subject):
    """USSD rating screen for any subject."""
    return con(
        f"Rate {subject}:\n"
        f"{RATING_OPTIONS_USSD}"
    )

def invalid_rating(subject):
    """USSD invalid input for rating screen."""
    return con(
        f"Invalid. Rate {subject}:\n"
        f"{RATING_OPTIONS_USSD}"
    )

# ---------- USSD WEBHOOK ----------
@app.post("/ussd", response_class=PlainTextResponse)
async def ussd_callback(
    sessionId:   str = Form(...),
    serviceCode: str = Form(...),
    phoneNumber: str = Form(...),
    text:        str = Form(default="")
):
    phone  = phoneNumber
    steps  = [s.strip() for s in text.split("*")] if text else []
    step   = steps[-1] if steps else ""
    print(f"[USSD] *384*59423# | session={sessionId} | phone={phone[:7]}**** | steps={steps}")

    student = ussd_get(phone)

    # Fresh dial-in
    if not text or not student:
        ussd_save(phone, "state", "LEVEL")
        return con(
            "Welcome to EduTena CBE\n"
            "CBC Pathway Assessment\n"
            "Select Level:\n"
            "1. JSS\n"
            "2. Senior"
        )

    state   = student[8]
    pathway = student[7]

    try:
        if state == "LEVEL":
            if step == "1":
                ussd_save(phone, "level", "JSS")
            elif step == "2":
                ussd_save(phone, "level", "Senior")
            else:
                return con(
                    "Invalid.\nSelect Level:\n"
                    "1. JSS\n"
                    "2. Senior"
                )
            ussd_save(phone, "state", "MATH")
            return rating_screen("Math")

        elif state == "MATH":
            score = RATING_MAP.get(step)
            if not score:
                return invalid_rating("Math")
            ussd_save(phone, "math", score)
            ussd_save(phone, "state", "SCIENCE")
            return rating_screen("Science")

        elif state == "SCIENCE":
            score = RATING_MAP.get(step)
            if not score:
                return invalid_rating("Science")
            ussd_save(phone, "science", score)
            ussd_save(phone, "state", "SOCIAL")
            return rating_screen("Social Studies")

        elif state == "SOCIAL":
            score = RATING_MAP.get(step)
            if not score:
                return invalid_rating("Social Studies")
            ussd_save(phone, "social", score)
            ussd_save(phone, "state", "CREATIVE")
            return rating_screen("Creative Arts")

        elif state == "CREATIVE":
            score = RATING_MAP.get(step)
            if not score:
                return invalid_rating("Creative Arts")
            ussd_save(phone, "creative", score)
            ussd_save(phone, "state", "TECH")
            return rating_screen("Technical Skills")

        elif state == "TECH":
            score = RATING_MAP.get(step)
            if not score:
                return invalid_rating("Technical Skills")
            ussd_save(phone, "technical", score)
            pathway = ussd_calculate_pathway(phone)
            ussd_save(phone, "state", "RESULT")
            return con(
                f"Your Pathway: {pathway}\n\n"
                "1. View Career Options\n"
                "2. Restart\n"
                "3. Exit"
            )

        elif state == "RESULT":
            if not pathway:
                pathway = ussd_calculate_pathway(phone)

            if step == "1":
                ussd_save(phone, "state", "CAREERS")
                if pathway == "STEM":
                    return con(
                        "STEM Careers:\n"
                        "- Engineering\n"
                        "- Data Science\n"
                        "- Medicine\n"
                        "- Architecture\n"
                        "- Pharmacy\n\n"
                        "0. Back"
                    )
                elif pathway == "Social Sciences":
                    return con(
                        "Social Sciences:\n"
                        "- Law\n"
                        "- Psychology\n"
                        "- Economics\n"
                        "- Education\n"
                        "- Journalism\n\n"
                        "0. Back"
                    )
                else:
                    return con(
                        "Arts & Sports:\n"
                        "- Graphic Design\n"
                        "- Music\n"
                        "- Sports Science\n"
                        "- Film & Media\n"
                        "- Fashion\n\n"
                        "0. Back"
                    )

            elif step == "2":
                ussd_reset(phone)
                return con(
                    "Assessment reset.\n"
                    "Select Level:\n"
                    "1. JSS\n"
                    "2. Senior"
                )

            elif step == "3":
                return end(
                    "Thank you for using EduTena CBE.\n"
                    "Good luck with your studies!"
                )

            else:
                return con(
                    f"Your Pathway: {pathway}\n\n"
                    "1. View Career Options\n"
                    "2. Restart\n"
                    "3. Exit"
                )

        elif state == "CAREERS":
            if step == "0":
                pathway = student[7]
                ussd_save(phone, "state", "RESULT")
                return con(
                    f"Your Pathway: {pathway}\n\n"
                    "1. View Career Options\n"
                    "2. Restart\n"
                    "3. Exit"
                )
            else:
                return end("Thank you for using EduTena CBE.\nGood luck!")

        else:
            ussd_save(phone, "state", "LEVEL")
            return con(
                "Welcome to EduTena CBE\n"
                "Select Level:\n"
                "1. JSS\n"
                "2. Senior"
            )

    except Exception as e:
        print(f"[USSD] Error: {e}")
        return end("Something went wrong. Please dial again.")
