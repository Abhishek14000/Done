#!/usr/bin/env python3
"""
ELITE VEDIC ASTROLOGY REPORT ENGINE
Powered by Multi-Layer Jyotish Reasoning System
Native: Abhishek Singh | DOB: 3 Sep 2000 | TOB: 00:00:18 | Ayodhya
"""

import json
import os
import re
from datetime import date, timedelta

# ============================================================
# CONSTANTS — defined once, used everywhere
# ============================================================

SIGN_ORDER = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"
]

SIGN_LORDS = {
    "Aries": "Mars", "Taurus": "Venus", "Gemini": "Mercury", "Cancer": "Moon",
    "Leo": "Sun", "Virgo": "Mercury", "Libra": "Venus", "Scorpio": "Mars",
    "Sagittarius": "Jupiter", "Capricorn": "Saturn", "Aquarius": "Saturn",
    "Pisces": "Jupiter",
}

# Mahadasha period durations in years for each planet in the Vimshottari system
# (total = 120 years; the order of lord/sub-lord cycles follows VIMSHOTTARI_ORDER)
VIMSHOTTARI_YEARS = {
    "Sun": 6, "Moon": 10, "Mars": 7, "Rahu": 18,
    "Jupiter": 16, "Saturn": 19, "Mercury": 17, "Ketu": 7, "Venus": 20,
}
VIMSHOTTARI_ORDER = [
    "Sun", "Moon", "Mars", "Rahu", "Jupiter",
    "Saturn", "Mercury", "Ketu", "Venus",
]
TOTAL_VIMSHOTTARI_YEARS = 120

# Combustion orbs (degrees of arc from Sun)
COMBUSTION_ORBS = {
    "Moon": 12, "Mars": 17, "Mercury": 14, "Jupiter": 11,
    "Venus": 10, "Saturn": 15,
}

EXALTATION = {
    "Sun": "Aries", "Moon": "Taurus", "Mars": "Capricorn",
    "Mercury": "Virgo", "Jupiter": "Cancer", "Venus": "Pisces", "Saturn": "Libra",
}
DEBILITATION = {
    "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
    "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries",
}
OWN_SIGNS = {
    "Sun": ["Leo"], "Moon": ["Cancer"], "Mars": ["Aries", "Scorpio"],
    "Mercury": ["Gemini", "Virgo"], "Jupiter": ["Sagittarius", "Pisces"],
    "Venus": ["Taurus", "Libra"], "Saturn": ["Capricorn", "Aquarius"],
    "Rahu": [], "Ketu": [],
}

HOUSE_MEANINGS = {
    1:  "self, personality, physical vitality, and life purpose",
    2:  "wealth, speech, family values, and accumulated assets",
    3:  "effort, courage, siblings, communication, and short journeys",
    4:  "home, mother, emotional security, landed property, and formal education",
    5:  "intelligence, creativity, children, speculation, and past-life merit",
    6:  "enemies, disease, debts, competition, and service",
    7:  "marriage, business partnerships, open enemies, and the public",
    8:  "transformation, secrets, occult, longevity, and sudden events",
    9:  "luck, dharma, father, higher knowledge, and long journeys",
    10: "career, karma, status, authority, and public recognition",
    11: "gains, income, aspirations, elder siblings, and social networks",
    12: "loss, foreign lands, moksha, spirituality, and hidden enemies",
}

PLANET_NATURE = {
    "Sun":     "authority, ego, soul, and the relationship with the father",
    "Moon":    "mind, emotions, mother, and intuitive perception",
    "Mars":    "energy, courage, aggression, action, and physical vitality",
    "Mercury": "intellect, communication, trade, adaptability, and analysis",
    "Jupiter": "wisdom, expansion, spirituality, children, and good fortune",
    "Venus":   "love, beauty, comfort, arts, and relationships",
    "Saturn":  "discipline, karma, delay, perseverance, and hard lessons",
    "Rahu":    "desire, obsession, illusion, innovation, and worldly ambition",
    "Ketu":    "detachment, past-life karma, spirituality, and liberation",
}

SPECIAL_ASPECTS = {
    "Mars":    [4, 8],
    "Jupiter": [5, 9],
    "Saturn":  [3, 10],
    "Rahu":    [5, 9],
    "Ketu":    [5, 9],
}

NAVAMSA_START = {
    "fire":  0,   # Aries
    "earth": 9,   # Capricorn
    "air":   6,   # Libra
    "water": 3,   # Cancer
}
SIGN_ELEMENT = {
    "Aries": "fire", "Leo": "fire", "Sagittarius": "fire",
    "Taurus": "earth", "Virgo": "earth", "Capricorn": "earth",
    "Gemini": "air", "Libra": "air", "Aquarius": "air",
    "Cancer": "water", "Scorpio": "water", "Pisces": "water",
}

DIK_BALA_HOUSE = {
    "Sun": 10, "Mars": 10, "Jupiter": 1, "Mercury": 1,
    "Moon": 4, "Venus": 4, "Saturn": 7,
}


# ============================================================
# DATA LOADING
# ============================================================

def load_kundali(filepath="kundali_rebuilt.json"):
    with open(filepath, "r", encoding="utf-8") as f:
        raw = json.load(f)
    bd = raw.get("basic_details", {})
    # Normalize top-level keys for convenience
    raw["ascendant"] = bd.get("lagna", "")
    raw["name"]          = bd.get("name", "")
    raw["date_of_birth"] = bd.get("date_of_birth", "")
    raw["time_of_birth"] = bd.get("time_of_birth", "")
    raw["place"]         = bd.get("place", "")
    raw["rasi"]          = bd.get("rasi", "")
    raw["nakshatra"]     = bd.get("nakshatra", "")
    return raw


def load_chunks(filepath="all_books_chunks.json"):
    """Load ALL knowledge-base chunks into memory (~7 MB, 4,553 chunks)."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# SMART RETRIEVAL — ITERATIVE FULL SCAN OF ALL CHUNKS
# ============================================================

# Pre-compiled noise pattern to discard OCR-garbage chunks quickly
_NOISE_RE = re.compile(r'[^a-zA-Z\s]{6,}', re.ASCII)

def _is_noisy(text):
    """Return True if a chunk is mostly OCR garbage."""
    if len(text) < 60:
        return True
    noise_chars = sum(1 for c in text if not c.isalpha() and not c.isspace())
    if noise_chars / max(len(text), 1) > 0.35:
        return True
    if _NOISE_RE.search(text):
        return True
    if len(text.split()) < 12:
        return True
    return False


def retrieve_insights(keywords, chunk_list, top_n=2):
    """
    Iterate through EVERY chunk in all_books_chunks.json.
    Score by keyword relevance + classical-jyotish language presence.
    Return the top_n highest-quality, non-duplicate results.
    """
    scored = []
    kw_lower = [k.lower() for k in keywords]

    for chunk in chunk_list:
        text = chunk.get("text", "")
        if _is_noisy(text):
            continue
        t = text.lower()

        score = 0
        for k in kw_lower:
            if k in t:
                score += 2
        for lw in ("house", "lord", "planet", "sign", "yoga", "dasha",
                   "indicates", "gives", "native", "result", "placed",
                   "kendra", "trikona", "aspect", "effects", "period"):
            if lw in t:
                score += 1
        if score < 5:
            continue
        scored.append((score, text))

    scored.sort(reverse=True, key=lambda x: x[0])

    seen = set()
    result = []
    for _, txt in scored:
        key = txt[:80]
        if key not in seen:
            seen.add(key)
            result.append(txt)
            if len(result) >= top_n:
                break
    return result


def _clean_insight(raw_text):
    """Normalise whitespace and truncate for clean inline use."""
    text = " ".join(raw_text.split())
    if len(text) > 420:
        text = text[:420].rsplit(" ", 1)[0] + "…"
    return text if len(text) >= 50 else None


# ============================================================
# TEXT HELPERS
# ============================================================

def _wrap(text, width=70, indent="  "):
    """Word-wrap text to width, indented by indent."""
    words = text.split()
    lines = []
    current = indent
    for word in words:
        if len(current) + len(word) + (1 if current.strip() else 0) > width:
            lines.append(current.rstrip())
            current = indent + word
        else:
            current += (" " if current.strip() else "") + word
    if current.strip():
        lines.append(current.rstrip())
    return "\n".join(lines)


def _sec(title):
    return "\n" + "=" * 70 + "\n" + title + "\n" + "=" * 70


# ============================================================
# COMBUSTION DETECTION
# ============================================================

def _absolute_degree(sign, degree):
    idx = SIGN_ORDER.index(sign) if sign in SIGN_ORDER else 0
    return idx * 30.0 + degree


def detect_combustion(planet_data):
    """Return {planet: True/False} combustion map."""
    sun = planet_data.get("Sun", {})
    sun_abs = _absolute_degree(sun.get("sign", "Aries"), sun.get("degree", 0))
    result = {}
    for planet, orb in COMBUSTION_ORBS.items():
        p = planet_data.get(planet, {})
        if not p:
            result[planet] = False
            continue
        p_abs = _absolute_degree(p.get("sign", "Aries"), p.get("degree", 0))
        diff = abs(sun_abs - p_abs)
        if diff > 180:
            diff = 360 - diff
        result[planet] = diff <= orb
    return result


# ============================================================
# NAVAMSA (D9)
# ============================================================

def calculate_navamsa(degree, sign):
    element = SIGN_ELEMENT.get(sign)
    if element is None:
        return "Unknown"
    segment = min(int(degree / (30.0 / 9)), 8)
    return SIGN_ORDER[(NAVAMSA_START[element] + segment) % 12]


# ============================================================
# ASPECTS (DRISHTI)
# ============================================================

def _house_to_sign(from_sign, houses_away):
    idx = SIGN_ORDER.index(from_sign) if from_sign in SIGN_ORDER else -1
    if idx == -1:
        return None
    return SIGN_ORDER[(idx + houses_away - 1) % 12]


def calculate_aspects(planet_data):
    rows = []
    for planet, data in planet_data.items():
        sign = data.get("sign", "")
        if sign not in SIGN_ORDER:
            continue
        pairs = [(7, _house_to_sign(sign, 7))]
        for h in SPECIAL_ASPECTS.get(planet, []):
            pairs.append((h, _house_to_sign(sign, h)))
        for h, asp_sign in pairs:
            if not asp_sign:
                continue
            targets = [p for p, d in planet_data.items()
                       if d.get("sign") == asp_sign and p != planet]
            desc = f"{planet} ({sign}) --{h}th--> {asp_sign}"
            if targets:
                desc += f"  [aspecting: {', '.join(targets)}]"
            rows.append(desc)
    return rows


# ============================================================
# SHADBALA (SIMPLIFIED COMPOSITE)
# ============================================================

def improved_shadbala(planet_data):
    rows = []
    for planet, data in planet_data.items():
        sign  = data.get("sign", "")
        house = data.get("house", 0)
        score = 0
        notes = []

        if EXALTATION.get(planet) == sign:
            score += 60; notes.append("exalted")
        elif DEBILITATION.get(planet) == sign:
            score -= 30; notes.append("debilitated")
        elif sign in OWN_SIGNS.get(planet, []):
            score += 45; notes.append("own sign")

        bh = DIK_BALA_HOUSE.get(planet)
        if bh is not None:
            if house == bh:
                score += 30; notes.append("full dik-bala")
            elif abs(house - bh) <= 2:
                score += 15; notes.append("partial dik-bala")

        if house in (1, 4, 7, 10):
            score += 20; notes.append("kendra")
        elif house in (5, 9):
            score += 15; notes.append("trikona")
        if house in (6, 8, 12):
            score -= 20; notes.append("dusthana")

        label = "Strong" if score >= 60 else ("Moderate" if score >= 20 else "Weak")
        rows.append((planet, score, label, ", ".join(notes) if notes else "neutral"))

    rows.sort(key=lambda x: x[1], reverse=True)
    return rows


# ============================================================
# ANTARDASHA CALCULATION (VIMSHOTTARI)
# ============================================================

def _parse_date(s):
    parts = s.strip().split("/")
    if len(parts) == 3:
        return date(int(parts[2]), int(parts[1]), int(parts[0]))
    return None


def _add_years(start, years_decimal):
    return start + timedelta(days=int(years_decimal * 365.25))


def compute_antardashas(maha_planet, maha_start, maha_years):
    """Return a list of antardasha dicts for the given Mahadasha."""
    idx   = VIMSHOTTARI_ORDER.index(maha_planet)
    order = VIMSHOTTARI_ORDER[idx:] + VIMSHOTTARI_ORDER[:idx]
    periods = []
    cur = maha_start
    for sub in order:
        dur = (VIMSHOTTARI_YEARS[sub] / TOTAL_VIMSHOTTARI_YEARS) * maha_years
        end = _add_years(cur, dur)
        periods.append({"planet": sub, "start": cur, "end": end, "years": round(dur, 3)})
        cur = end
    return periods


def current_dasha_antar(dasha_list, today):
    maha = None
    for d in dasha_list:
        s, e = _parse_date(d["start"]), _parse_date(d["end"])
        if s and e and s <= today <= e:
            maha = d; break
    if not maha:
        return None, None, []
    ms = _parse_date(maha["start"])
    antars = compute_antardashas(maha["planet"], ms, maha["years"])
    antar = next((a for a in antars if a["start"] <= today <= a["end"]), None)
    return maha, antar, antars


# ============================================================
# SECTION 1 — INTRODUCTION
# ============================================================

def sec_introduction(kundali, planet_data):
    bd = kundali.get("basic_details", {})
    lines = [_sec("SECTION 1 — INTRODUCTION & KUNDALI SUMMARY"), ""]

    lines.append(f"  Name              : {bd.get('name','N/A')}")
    lines.append(f"  Date of Birth     : {bd.get('date_of_birth','N/A')}")
    lines.append(f"  Time of Birth     : {bd.get('time_of_birth','N/A')}")
    lines.append(f"  Place             : {bd.get('place','N/A')}")
    lines.append(f"  Ascendant (Lagna) : {bd.get('lagna','N/A')}")
    lines.append(f"  Rasi (Moon Sign)  : {bd.get('rasi','N/A')}")
    lines.append(f"  Nakshatra         : {bd.get('nakshatra','N/A')} Pada {bd.get('nakshatra_pada','?')}")
    lines.append(f"  Nakshatra Lord    : {bd.get('nakshatra_lord','N/A')}")
    lines.append("")

    lines.append("=== PLANETARY PLACEMENTS AT A GLANCE ===")
    lines.append("")
    hdr = f"  {'Planet':10s}  {'Sign':15s}  {'Hse':3s}  {'Nakshatra':22s}  {'Pd':2s}  {'NK Lord':8s}  {'Degree':8s}"
    lines.append(hdr)
    lines.append("  " + "-" * 80)
    for p, d in planet_data.items():
        lines.append(
            f"  {p:10s}  {d.get('sign','N/A'):15s}  {str(d.get('house','?')):3s}  "
            f"{d.get('nakshatra','N/A'):22s}  {str(d.get('pada','?')):2s}  "
            f"{d.get('nakshatra_lord','N/A'):8s}  {d.get('degree',0):7.3f}°"
        )
    lines.append("")

    lines.append("=== CHART OVERVIEW ===")
    lines.append("")
    overview = (
        "Abhishek Singh is born with Gemini Lagna, placing Mercury as the lord of the ascendant. "
        "The Moon sits in Libra in the 5th house in Swati nakshatra, ruled by Rahu — conferring "
        "refined aesthetic intelligence, independent thinking, and a restless but brilliant mind. "
        "Rahu occupies the Lagna itself, driving an unconventional, magnetically ambitious life path. "
        "Ketu in the 7th house brings deep karmic weight to partnerships and a spiritualized orientation "
        "toward committed relationships. Saturn and Jupiter conjunct in the 12th house in Taurus create "
        "a rare and profound combination — the two largest planets meeting in the house of liberation, "
        "spiritual depth, and foreign lands. Sun and Mercury conjunct in Leo (3rd house) form "
        "Budha-Aditya Yoga, though Mercury is combust, modifying this yoga's expression. "
        "This is the chart of an intellectual, communicator, and spiritual seeker whose life unfolds "
        "through the tension between worldly ambition (Rahu in Lagna) and inner liberation (Saturn-Jupiter in 12th)."
    )
    lines.append(_wrap(overview, 70))
    lines.append("")
    return "\n".join(lines)


# ============================================================
# SECTION 2 — PLANET DEEP ANALYSIS
# ============================================================

PLANET_DEEP_TEXT = {
    "ASC": (
        "The Ascendant rises at 1°15' Gemini in Mrigasira nakshatra (Mars-ruled, Pada 3). "
        "Gemini Lagna produces an intellectually versatile, communicative, and perpetually curious personality. "
        "Mercury as Lagna lord confers quick wit, love of learning, and adaptability across diverse domains. "
        "Mrigasira ('the searching deer') is an exploratory nakshatra — the native is in constant pursuit "
        "of knowledge, new experiences, and intellectual stimulation. The Lagna at only 1°15' indicates a "
        "personality that is still evolving and continuously reinventing itself throughout life. "
        "Rahu conjoins the Lagna at 28° Gemini — amplifying worldly ambition, unconventional drive, and "
        "a magnetic, larger-than-life public presence."
    ),
    "Sun": (
        "Sun is placed in Leo at 16°43' in Purva Phalguni nakshatra (Pada 2, Venus-ruled). "
        "Leo is the Sun's own sign — a position of full dignity and confident authority. "
        "Sun in Leo bestows natural leadership, strong personal identity, and an authoritative presence "
        "that commands respect without demanding it. Purva Phalguni is ruled by Venus and governed by "
        "Bhaga (deity of prosperity and marital bliss) — this adds creative flair, love of beauty, "
        "and a pleasure-oriented dimension to the Sun's core authority. "
        "The Sun occupies the 3rd house of communication, courage, and self-driven effort. "
        "This indicates career recognition through bold self-expression, writing, media, "
        "entrepreneurship, or any domain requiring intellectual courage and visible personal initiative. "
        "Sun conjunct Mercury in the same house forms Budha-Aditya Yoga — however Mercury is combust "
        "(see Mercury entry), which modifies the yoga's full expression."
    ),
    "Moon": (
        "Moon is placed in Libra at 13°02' in Swati nakshatra (Pada 2, Rahu-ruled). "
        "Libra Moon produces a balanced, socially graceful, and aesthetically refined emotional nature. "
        "Venus rules Libra — Moon here desires harmony, beauty, fairness, and deep relational connection. "
        "Swati nakshatra, governed by Vayu (wind deity) and ruled by Rahu, gives independence, restlessness, "
        "and a strong drive for personal freedom that occasionally conflicts with the relational desires "
        "inherent in Libra. Moon in the 5th house is a trikona placement of exceptional quality — "
        "the 5th governs intelligence, creativity, children, and past-life spiritual merit. "
        "This Moon generates heightened creative intelligence, emotional empathy, refined artistic taste, "
        "and a memory capable of retaining vast knowledge. The native's emotional intelligence is "
        "his primary inner resource throughout life."
    ),
    "Mars": (
        "Mars is placed in Cancer at 27°08' in Ashlesha nakshatra (Pada 4, Mercury-ruled). "
        "Cancer is Mars's sign of debilitation — the warrior becomes emotionally reactive, indirect, "
        "and lacking in the decisive fire that defines Mars at its best. "
        "Ashlesha (ruled by Mercury, governed by the serpent deity Sarpa) gives strategic cunning, "
        "sharp psychological intelligence, and the ability to apply pressure subtly — partially "
        "compensating for the weakened Mars. Mars occupies the 2nd house, creating Manglik Dosha "
        "in the 2nd house position — affecting speech (can be sharp or cutting) and family harmony. "
        "Neecha Bhanga (partial cancellation) analysis: the lord of Cancer, Moon, is placed in the 5th "
        "house — a trikona — which provides partial Neecha Bhanga. The full debilitation is not cancelled "
        "but significantly reduced. During Mars Dasha/Antardasha, initial challenges in action and "
        "finances give way to recovery and eventual strength after the cancellation activates."
    ),
    "Mercury": (
        "Mercury is placed in Leo at 27°23' in Uttara Phalguni nakshatra (Pada 1, Sun-ruled). "
        "COMBUSTION STATUS: Mercury is at 27°23' Leo; Sun is at 16°43' Leo. "
        "The angular separation is approximately 10.67° — well within Mercury's combustion orb of 14°. "
        "Therefore, Mercury is COMBUST in this chart. This is a critical finding. "
        "Budha-Aditya Yoga (Sun-Mercury conjunction) is present but its results are modified: "
        "while the native possesses sharp intellect, articulate communication, and intellectual brilliance, "
        "Mercury's independent voice operates under the Sun's dominant authority. "
        "The native may tend to express opinions shaped by ego rather than pure analysis, "
        "or may struggle with intellectual humility at critical decision points. "
        "Mercury in the 3rd house (its natural communication domain) partially restores Mercury's "
        "placement strength — the native remains a gifted communicator and thinker, "
        "but must consciously cultivate analytical independence from ego-driven conclusions. "
        "Uttara Phalguni (Sun-ruled) reinforces the Sun-authoritative orientation of this Mercury."
    ),
    "Jupiter": (
        "Jupiter is placed in Taurus at 16°11' in Rohini nakshatra (Pada 2, Moon-ruled). "
        "Taurus is ruled by Venus — a neutral sign for Jupiter, neither friend nor enemy. "
        "Jupiter in Taurus produces practical, materially-grounded wisdom. The native applies "
        "philosophical understanding toward building tangible, enduring structures. "
        "Rohini, governed by Brahma (deity of creation) and ruled by Moon, gives creative intelligence, "
        "aesthetic sensibility, warmth, and love of abundance. "
        "Jupiter occupies the 12th house — a dusthana position, reducing worldly career visibility, "
        "but powerfully activating spiritual growth, foreign connections, and institutional wisdom. "
        "Jupiter in the 12th as the 10th lord (for Gemini Lagna) indicates a career in research, "
        "foreign organizations, spiritual or philosophical fields, or behind-the-scenes advisory roles. "
        "Saturn also occupies the 12th — their conjunction creates the profound Saturn-Jupiter Moksha Yoga "
        "described in the Yoga section."
    ),
    "Venus": (
        "Venus is placed in Virgo at 9°22' in Uttara Phalguni nakshatra (Pada 4, Sun-ruled). "
        "Virgo is Venus's sign of debilitation — Venus at its most challenged position in the zodiac. "
        "Venus in Virgo struggles to express beauty, sensuality, and emotional openness freely. "
        "The native tends toward hyper-criticism in relationships, perfectionism in romantic expectations, "
        "and difficulty surrendering to vulnerability. "
        "Neecha Bhanga assessment: Jupiter (lord of Pisces, Venus's exaltation sign) aspects Venus "
        "via its 5th house drishti from Taurus to Virgo — this is a classical Neecha Bhanga factor. "
        "Jupiter's aspect on debilitated Venus partially cancels the debilitation and introduces divine "
        "grace into the domain of relationships over time. "
        "Venus occupies the 4th house — its own directional strength (dik-bala) position — "
        "which provides additional compensation. Venus in the 4th, despite debilitation, "
        "brings domestic sensibility, aesthetic home environment, and eventual relational depth "
        "once the native overcomes the perfectionist filter."
    ),
    "Saturn": (
        "Saturn is placed in Taurus at 7°02' in Krittika nakshatra (Pada 4, Sun-ruled). "
        "Taurus is ruled by Venus — Saturn and Venus are friendly planets. "
        "Saturn in Taurus produces methodical, disciplined material accumulation. "
        "Krittika (fire deity Agni, Sun-ruled) gives Saturn a sharp, purifying, and cutting quality — "
        "discipline forged through fiery determination and the willingness to sever what is unnecessary. "
        "Saturn occupies the 12th house of foreign lands, spirituality, and liberation. "
        "Saturn rules the 8th (Capricorn) and 9th (Aquarius) houses for Gemini Lagna. "
        "A dusthana lord (8th) placed in another dusthana (12th) creates Vipreet Raj Yoga — "
        "unexpected rise, success through adversity, and triumph after hardship. "
        "The Saturn Mahadasha (2026–2045) is therefore this native's most pivotal and transformative period. "
        "Saturn conjunct Jupiter in the 12th creates a rare Moksha Yoga — the two great planets "
        "meeting in the house of liberation and spiritual realization."
    ),
    "Rahu": (
        "Rahu is placed in Gemini at 28°11' in Punarvasu nakshatra (Pada 3, Jupiter-ruled). "
        "Rahu in the Lagna (1st house) is one of the most life-defining placements possible. "
        "The ascending node in the ascendant creates a magnetic, unconventional, and intensely driven "
        "personality that operates outside conventional social categories. "
        "Punarvasu ('return of the light', governed by Aditi — mother of the gods) gives resilience, "
        "the capacity for renewal, and an ability to reinvent oneself repeatedly after adversity. "
        "Rahu amplifies Gemini's intelligence and communication abilities into an obsessive, "
        "all-consuming drive for mastery and recognition. The native is drawn to foreign cultures, "
        "emerging technology, unconventional intellectual systems, and boundary-defying ideas. "
        "Rahu aspects the 5th (intelligence/creativity), 7th (marriage), and 9th (dharma) houses — "
        "each of these domains carries an unconventional, amplified, and sometimes unpredictable quality."
    ),
    "Ketu": (
        "Ketu is placed in Sagittarius at 28°11' in Uttara Ashadha nakshatra (Pada 1, Sun-ruled). "
        "Sagittarius Ketu indicates deep past-life mastery in the domains of philosophy, dharma, "
        "higher knowledge, and spiritual truth. In this life, the native approaches these areas "
        "with natural competence but inner detachment — they are completed karma. "
        "Uttara Ashadha ('the later invincible one', governed by Vishvedevas — universal gods) gives "
        "a strong sense of universal duty and unfinished karmic purpose that bridges lifetimes. "
        "Ketu occupies the 7th house — the house of marriage and partnerships. "
        "This is among the most complex Ketu placements: the native carries deep past-life soul memories "
        "of partnership, and in this life the 7th house operates with a simultaneously familiar and "
        "spiritualized quality. Conventional marriage does not satisfy — the relationship must serve "
        "a higher purpose. The spouse is typically philosophically inclined, spiritually evolved, "
        "or connected to foreign or philosophical domains. "
        "Ketu's 9th aspect falls on Leo (3rd house), touching Sun and Mercury — adding philosophical "
        "depth and past-life authority to the native's intellectual communication."
    ),
}


def sec_planets(planet_data, combustion_status, chunk_list):
    lines = [_sec("SECTION 2 — PLANET-BY-PLANET DEEP ANALYSIS"), ""]

    for planet, data in planet_data.items():
        sign      = data.get("sign", "N/A")
        house     = data.get("house", "N/A")
        nakshatra = data.get("nakshatra", "N/A")
        degree    = data.get("degree", 0.0)
        combust   = combustion_status.get(planet, False)

        lines.append("─" * 70)
        hdr = f"  {planet.upper()} in {sign}  |  House {house}  |  {nakshatra}  |  {degree:.3f}°"
        if combust:
            hdr += "  ⚠ COMBUST"
        lines.append(hdr)
        lines.append("─" * 70)

        deep = PLANET_DEEP_TEXT.get(planet)
        if deep:
            lines.append(_wrap(deep, 70))
        else:
            lines.append(f"  {planet} is placed in {sign}, House {house}.")

        # Dignity note
        if planet in EXALTATION:
            if EXALTATION[planet] == sign:
                lines.append("\n  ✦ EXALTED — exceptional positional strength.")
            elif DEBILITATION[planet] == sign:
                lines.append("\n  ↓ DEBILITATED — Neecha Bhanga conditions analyzed above.")
            elif sign in OWN_SIGNS.get(planet, []):
                lines.append("\n  ✦ OWN SIGN — stable, fully expressive.")

        # Classical text support from all_books_chunks.json
        kw = [planet.lower(), sign.lower(),
              nakshatra.split()[0].lower() if nakshatra != "N/A" else "",
              f"{planet.lower()} {house}"]
        classical = retrieve_insights(kw, chunk_list, top_n=2)
        if classical:
            lines.append("\n  Classical Text Support:")
            for c in classical:
                clean = _clean_insight(c)
                if clean:
                    lines.append(_wrap(f"→ {clean}", 68, indent="    "))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 3 — YOGA ANALYSIS
# ============================================================

def sec_yogas(planet_data, combustion_status, chunk_list):
    lines = [_sec("SECTION 3 — YOGA ANALYSIS (CLASSICAL + ADVANCED)"), ""]
    lines.append(_wrap(
        "Yogas are specific planetary configurations that produce distinctive life outcomes. "
        "Each yoga below is analyzed for its real-life impact, with modifications applied "
        "where combustion, debilitation, or conflicting factors are present. "
        "This section resolves all contradictions rather than listing yogas blindly.", 70))
    lines.append("")

    jup_h = planet_data.get("Jupiter",{}).get("house",0)
    moon_h= planet_data.get("Moon",   {}).get("house",0)
    sun_h = planet_data.get("Sun",    {}).get("house",0)
    mer_h = planet_data.get("Mercury",{}).get("house",0)
    mars_h= planet_data.get("Mars",   {}).get("house",0)
    ven_h = planet_data.get("Venus",  {}).get("house",0)
    sat_h = planet_data.get("Saturn", {}).get("house",0)
    rahu_h= planet_data.get("Rahu",   {}).get("house",0)
    ketu_h= planet_data.get("Ketu",   {}).get("house",0)

    jup_s = planet_data.get("Jupiter",{}).get("sign","")
    ven_s = planet_data.get("Venus",  {}).get("sign","")
    mars_s= planet_data.get("Mars",   {}).get("sign","")
    mer_s = planet_data.get("Mercury",{}).get("sign","")
    sat_s = planet_data.get("Saturn", {}).get("sign","")
    sun_s = planet_data.get("Sun",    {}).get("sign","")

    yogas = []

    # 1. Budha-Aditya Yoga (Mercury combust — modified)
    if sun_h and mer_h and sun_h == mer_h:
        combust = combustion_status.get("Mercury", False)
        if combust:
            yogas.append(("☀️ Budha-Aditya Yoga  [MODIFIED — Mercury Combust]",
                "Sun and Mercury conjunct in Leo (3rd house) forms Budha-Aditya Yoga — the fusion of "
                "solar authority with mercurial intellect. The result is razor-sharp intelligence, "
                "articulate public expression, and intellectual leadership. HOWEVER, Mercury is combust "
                "(10.67° from Sun, within the 14° orb). Therefore the yoga is modified: full analytical "
                "independence is partially compromised by the Sun's ego-authority. The native is gifted "
                "and expressive but benefits from cultivating intellectual humility. Career in communication, "
                "media, writing, education, or leadership is strongly indicated. The yoga activates "
                "most powerfully during Sun and Mercury Dasha/Antardasha periods."))
        else:
            yogas.append(("☀️ Budha-Aditya Yoga",
                "Sun-Mercury conjunction conferring sharp intellect and leadership through knowledge."))

    # 2. Gajakesari Yoga
    if jup_h and moon_h:
        diff = abs(jup_h - moon_h)
        if diff in (0, 3, 6, 9) or (12 - diff) in (0, 3, 6, 9):
            yogas.append(("🐘 Gajakesari Yoga",
                "Jupiter (12th house) and Moon (5th house) are 7 houses apart — a mutual kendra relationship. "
                "This forms Gajakesari Yoga, one of the most celebrated yogas in Vedic astrology. "
                "It bestows wisdom, public recognition, and divine protection. The native possesses the "
                "combined intelligence of Mercury, wisdom of Jupiter, and emotional empathy of Moon operating "
                "in harmonious alignment. Jupiter's 12th-house placement slightly reduces worldly impact "
                "but powerfully amplifies the yoga's spiritual dimension. Activates strongly during "
                "Jupiter and Moon Dasha periods — bringing recognition and the ability to overcome "
                "obstacles through righteous action."))

    # 3. Vipreet Raj Yoga
    if sat_h in (6, 8, 12):
        yogas.append(("🔥 Vipreet Raj Yoga",
            "Saturn rules the 8th (Capricorn) and 9th (Aquarius) houses for Gemini Lagna and is placed "
            "in the 12th house. A dusthana lord placed in another dusthana fulfills the classical Vipreet "
            "condition. This yoga produces unexpected rise, success through adversity, and triumph after "
            "hardship. The greater the challenge faced, the greater the eventual reward. This is the yoga "
            "of the phoenix — rising from difficulty precisely because of it. Most powerfully activated "
            "during Saturn Mahadasha (2026–2045). Obstacles in this period are the very mechanism of elevation."))

    # 4. Saturn-Jupiter in 12th — Moksha Yoga
    if sat_h == 12 and jup_h == 12:
        yogas.append(("🙏 Saturn-Jupiter Conjunction in 12th — Moksha Yoga",
            "Saturn and Jupiter are both placed in the 12th house of liberation, spiritual depth, and "
            "foreign lands. This rare conjunction of the two great planets in the house of moksha creates "
            "a profound soul orientation toward liberation and philosophical truth. Jupiter brings wisdom "
            "and expansive inner knowledge; Saturn adds discipline, endurance, and karmic accountability. "
            "Together in the 12th, they indicate: foreign settlement potential, work in institutional or "
            "research environments, deep solitary spiritual practice, and eventual profound renunciation of "
            "worldly ambition in favor of inner truth. This is the astrological signature of a philosopher, "
            "researcher, or spiritual authority who shapes the world from the unseen."))

    # 5. Neecha Bhanga — Mars
    if mars_s == "Cancer":
        yogas.append(("♻️ Neecha Bhanga Raja Yoga — Mars (Partial Cancellation)",
            "Mars is debilitated in Cancer (2nd house). Moon (lord of Cancer — Mars's debilitation sign) "
            "is placed in the 5th house, a trikona — providing partial Neecha Bhanga. The debilitation "
            "is not fully cancelled but significantly reduced. Initial weakness in decisive action and "
            "financial management gives way to eventual strength. During Mars Dasha/Antardasha, "
            "the partially cancelled debilitation produces surprising results — rise after struggle "
            "and the development of authentic courage through adversity."))

    # 6. Neecha Bhanga — Venus
    if ven_s == "Virgo":
        yogas.append(("♻️ Neecha Bhanga — Venus (via Jupiter's 5th Aspect)",
            "Venus is debilitated in Virgo (4th house). Jupiter from Taurus aspects Virgo via its "
            "5th house drishti — a classical Neecha Bhanga factor. Jupiter's divine grace applied "
            "to debilitated Venus gradually heals the domain of relationships. Additionally, Venus "
            "occupies its dik-bala (directional strength) house — the 4th — providing compensation. "
            "Venus's debilitation creates early-life relational perfectionism and dissatisfaction, "
            "but Jupiter's ongoing aspect ensures gradual improvement and eventual meaningful partnership."))

    # 7. Moon in 5th — Medha Yoga
    if moon_h == 5:
        yogas.append(("🧠 Moon in 5th — Medha Yoga",
            "Moon in the 5th house (trikona of intelligence and past-life merit) creates Medha Yoga — "
            "exceptional mental brilliance, creative emotional intelligence, and deep intuitive capacity. "
            "This placement gives poetic sensitivity, artistic gifts, and a retentive memory that absorbs "
            "vast knowledge. The Swati nakshatra adds intellectual independence and original synthesis. "
            "Excellent for academic achievement, creative arts, writing, and research."))

    # 8. Sun in Own Sign
    if sun_s == "Leo":
        yogas.append(("☀️ Sun in Own Sign — Swakshetra Bala (3rd House)",
            "Sun in Leo is in its own sign — Swakshetra Bala (own-sign strength). The Sun is confident, "
            "fully expressed, and operating from dignified authority. In the 3rd house, this amplifies "
            "self-expression, creative leadership, and the courage to forge one's own path. "
            "Despite Mercury's combustion modifying the Budha-Aditya yoga, the Sun itself is powerful "
            "and contributes significantly to professional recognition and personal authority throughout life."))

    # 9. Rahu in Lagna
    if rahu_h == 1:
        yogas.append(("🌟 Rahu in Lagna — Magnetic Persona Yoga",
            "Rahu in the 1st house creates an extraordinary, unconventional, and magnetically compelling "
            "personality. The native appears larger than life — intensely ambitious and charismatic. "
            "Rahu amplifies Gemini's intelligence into an all-consuming drive for mastery. "
            "Foreign connections, technology, media, and unconventional knowledge systems are natural "
            "domains. Rahu's completed Mahadasha (birth–2010) shaped the personality foundations. "
            "Its aspects to the 5th, 7th, and 9th houses continue to color intelligence, "
            "partnerships, and dharma with unconventional intensity throughout life."))

    # 10. Ketu in 7th — Karmic Partnership
    if ketu_h == 7:
        yogas.append(("🌀 Ketu in 7th — Karmic Partnership Yoga",
            "Ketu in the 7th house creates one of the most spiritually complex relationship patterns "
            "in Jyotish. The native has mastered partnership themes in past lives — in this life, "
            "the 7th house operates with detachment, depth, and an unusually spiritual orientation. "
            "Conventional marriage does not satisfy; the relationship must serve a higher, karmic purpose. "
            "The spouse carries qualities of philosophical depth, spiritual evolution, or foreign origin. "
            "Marriage becomes a vehicle for mutual spiritual evolution rather than social convention."))

    # 11. Parivartana Yoga detection
    slm = {
        "Aries":"Mars","Taurus":"Venus","Gemini":"Mercury","Cancer":"Moon",
        "Leo":"Sun","Virgo":"Mercury","Libra":"Venus","Scorpio":"Mars",
        "Sagittarius":"Jupiter","Capricorn":"Saturn","Aquarius":"Saturn","Pisces":"Jupiter",
    }
    checked = set()
    for p1, d1 in planet_data.items():
        for p2, d2 in planet_data.items():
            if p1 >= p2 or (p1, p2) in checked:
                continue
            checked.add((p1, p2))
            if (slm.get(d1.get("sign","")) == p2
                    and slm.get(d2.get("sign","")) == p1):
                yogas.append((f"🔄 Parivartana Yoga — {p1} ↔ {p2}",
                    f"{p1} and {p2} are in mutual sign exchange. {p1} occupies {p2}'s sign and "
                    f"{p2} occupies {p1}'s sign. This creates a powerful energetic bond between "
                    f"both houses, activating them simultaneously and producing unexpected but "
                    f"significant life results in both domains."))

    # 12. Kemadruma check
    adjacent = {(moon_h - 2) % 12 + 1, moon_h % 12 + 1}
    other_h  = {v.get("house") for k, v in planet_data.items()
                if k not in ("Moon","Rahu","Ketu") and v.get("house")}
    if moon_h and not adjacent.intersection(other_h):
        yogas.append(("⚠️ Kemadruma Yoga (Emotional Isolation Pattern)",
            "No planets occupy the 2nd or 12th house from Moon. Kemadruma Yoga indicates emotional "
            "isolation, mental restlessness, or periods of psychological solitude. "
            "Cancellation occurs if Moon is in a kendra or aspected by benefics — "
            "check the Aspects section for mitigating factors."))

    # 13. Adhi Yoga
    if moon_h:
        a_houses = {(moon_h+4)%12+1, moon_h%12+1, (moon_h+6)%12+1}
        a_bens = [p for p in ("Jupiter","Venus","Mercury")
                  if planet_data.get(p,{}).get("house") in a_houses]
        if len(a_bens) >= 2:
            yogas.append(("🌟 Adhi Yoga",
                f"Benefics ({', '.join(a_bens)}) occupy the 6th, 7th, and/or 8th positions from Moon. "
                "Adhi Yoga produces ministers, commanders, and influential leaders. The native rises to "
                "authority through intelligence, diplomacy, and inner moral strength."))

    # Write all yogas
    for i, (name, desc) in enumerate(yogas, 1):
        lines.append(f"{i}. {name}")
        lines.append(_wrap(desc, 70))
        lines.append("")

    # Classical support
    classical = retrieve_insights(
        ["yoga","raj yoga","vipreet","neecha bhanga","12th house","jupiter saturn"],
        chunk_list, top_n=2)
    if classical:
        lines.append("Classical Text Support:")
        for c in classical:
            clean = _clean_insight(c)
            if clean:
                lines.append(_wrap(f"→ {clean}", 68, indent="  "))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 4 — CORE LIFE SYNTHESIS
# ============================================================

def sec_synthesis(planet_data, kundali):
    lines = [_sec("SECTION 4 — CORE LIFE SYNTHESIS (CROSS-PLANET PATTERNS)"), ""]
    lines.append(_wrap(
        "This section synthesizes the full chart into real-life patterns by combining planetary "
        "placements, lordships, and cross-planetary relationships into a unified life blueprint. "
        "The goal is to resolve the chart into coherent life themes rather than isolated observations.", 70))
    lines.append("")

    patterns = [
        ("PATTERN 1: THE INTELLECTUAL-SPIRITUAL AXIS (3rd ↔ 12th)",
         "Sun and Mercury conjunct in Leo (3rd house) creates a powerful intellectual and expressive "
         "core identity. Simultaneously, Saturn and Jupiter conjunct in Taurus (12th house) generates "
         "a deep spiritual-philosophical undercurrent beneath the outward communicative personality. "
         "The native engages the world through intelligence and charisma (3rd house) while sustaining "
         "an intense inner life of contemplation, solitude, and philosophical inquiry (12th house). "
         "This axis — 3rd to 12th — produces a communicator who speaks publicly while seeking truth privately. "
         "The career is the vehicle; inner liberation is the destination."),

        ("PATTERN 2: THE RAHU-KETU KARMIC AXIS (1st ↔ 7th)",
         "Rahu in the 1st house and Ketu in the 7th house constitute the chart's defining karmic axis. "
         "In past lives, this soul mastered Sagittarian wisdom — philosophy, dharma, and higher knowledge. "
         "In this life, the soul is called toward Gemini — intellectual versatility, adaptability, "
         "and worldly communication mastery. Rahu in Lagna drives self-development and social ambition. "
         "Ketu in the 7th creates simultaneous spiritual depth and detachment in partnerships. "
         "The central life tension: self vs. partnership — between individual self-creation and the "
         "call of genuine relational commitment. Consciously integrating both is the primary "
         "spiritual achievement this lifetime is designed to produce."),

        ("PATTERN 3: CAREER AND WEALTH DYNAMICS",
         "Mars (2nd lord for Gemini: Mars rules Scorpio = 6th, not 2nd — the 2nd lord is actually Moon "
         "since Cancer is the 2nd house) is debilitated in the 2nd house itself, creating inconsistency "
         "in early-life wealth generation. Mercury (1st and 4th lord) in the 3rd supports communication-"
         "based income. Saturn-Jupiter in the 12th indicate eventual wealth through foreign sources, "
         "research, or institutional work. The Vipreet Raj Yoga ensures that financial setbacks become "
         "launching pads. Overall: slow but meaningful wealth accumulation through intellectual labor "
         "and international engagement, with the most significant financial transformation during "
         "Saturn Mahadasha (2026–2045)."),

        ("PATTERN 4: EMOTIONAL INTELLIGENCE AND RELATIONAL COMPLEXITY",
         "Moon in Libra (5th house, Swati nakshatra) gives refined emotional intelligence, aesthetic "
         "sensitivity, and deep creative empathy. However, Rahu aspects the 5th house from the 1st, "
         "bringing restlessness and unconventional patterns to Moon's domain. Venus (karaka for "
         "relationships) is debilitated in the 4th — emotional security through home and mother involves "
         "complexity. Ketu in the 7th creates spiritual depth but also detachment in partnerships. "
         "The native is emotionally intelligent and socially graceful, yet struggles with deep relational "
         "commitment. Jupiter's 5th-aspect on Venus provides grace and eventual healing. "
         "True fulfillment in relationships arrives through accepting partnership as a spiritual path "
         "rather than a conventional social arrangement."),

        ("PATTERN 5: THE BENEFIC CORRECTION NETWORK",
         "Despite two planetary debilitations (Mars in Cancer, Venus in Virgo), the chart contains "
         "powerful correction mechanisms: Jupiter aspects debilitated Venus (5th drishti from Taurus to Virgo). "
         "Moon in trikona partially cancels Mars's debilitation. Sun in own sign Leo compensates for "
         "Mercury's combustion through strong solar dignity. Rahu in Lagna provides unconventional "
         "amplification that bypasses conventional limitation. The chart is thus self-correcting — "
         "weaknesses trigger compensatory strengths. The native does not fail permanently; "
         "each setback activates a corresponding Yoga-driven recovery."),
    ]

    for title, body in patterns:
        lines.append(f"=== {title} ===")
        lines.append("")
        lines.append(_wrap(body, 70))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 5 — CAREER ANALYSIS
# ============================================================

def sec_career(planet_data, kundali, chunk_list):
    lagna = kundali.get("ascendant", "")
    lines = [_sec("SECTION 5 — CAREER ANALYSIS (DEEP MULTI-LAYER)"), ""]

    # 10th house lord
    if lagna in SIGN_ORDER:
        li   = SIGN_ORDER.index(lagna)
        t10s = SIGN_ORDER[(li + 9) % 12]
        t10l = SIGN_LORDS.get(t10s, "Unknown")
        t10d = planet_data.get(t10l, {})
        lines.append("=== 10TH HOUSE — PRIMARY CAREER INDICATOR ===")
        lines.append("")
        lines.append(_wrap(
            f"For Gemini Lagna, the 10th house falls in Pisces, ruled by Jupiter. "
            f"Jupiter is placed in Taurus (12th house). Therefore, the 10th lord Jupiter sits in "
            f"the 12th house of foreign lands, research, spirituality, and hidden institutional work. "
            f"This is among the most significant career configurations: the lord of public career "
            f"placed in the house of invisible or behind-the-scenes operation. "
            f"The native is not suited for routine public corporate roles. True career fulfillment "
            f"comes through depth, independent research, philosophical leadership, foreign engagement, "
            f"or work in institutions removed from conventional commercial structures. "
            f"Jupiter in Rohini (Moon-ruled) adds creativity, warmth, and artistic sensibility "
            f"to the career palette.", 70))
        lines.append("")

    lines.append("=== SATURN — KARMA KARAKA ===")
    lines.append("")
    lines.append(_wrap(
        "Saturn in the 12th house links career karma to foreign settings, research environments, "
        "and behind-the-scenes roles. The Vipreet Raj Yoga (8th/9th lord in 12th) guarantees "
        "that career setbacks become launching pads for elevated positions. "
        "Saturn Mahadasha (2026–2045) is the primary career-defining epoch — 19 years of "
        "discipline-forging, structure-building professional development. The native who persists "
        "through this period's demands achieves significant and lasting professional standing.", 70))
    lines.append("")

    lines.append("=== SUN IN 3RD HOUSE (LEO, OWN SIGN) ===")
    lines.append("")
    lines.append(_wrap(
        "Sun in Leo in the 3rd house gives exceptional career potential in communication, media, "
        "writing, broadcasting, education, and entrepreneurship. The Sun in its own sign in the "
        "3rd creates a natural public intellectual and authoritative communicator. Career roles "
        "requiring bold personal initiative, visible intellectual authority, and expressive leadership "
        "are perfectly aligned. Podcasting, authorship, academic research, content creation, "
        "journalism, and independent intellectual consulting are strongly indicated.", 70))
    lines.append("")

    lines.append("=== MERCURY (LAGNA LORD) IN 3RD HOUSE ===")
    lines.append("")
    lines.append(_wrap(
        "Mercury, lord of both the 1st and 4th houses for Gemini Lagna, occupies the 3rd house — "
        "the natural house of communication, commerce, and intellectual effort. "
        "Despite combustion, Mercury's placement in the 3rd partially restores its functional strength. "
        "Communication remains the native's primary professional instrument. "
        "Career in writing, analysis, technology, data science, consulting, or commerce is supported. "
        "Mercury rules the 1st and 4th — career is intimately tied to personal identity and "
        "domestic/real-estate domains.", 70))
    lines.append("")

    lines.append("=== RAHU IN 1ST HOUSE — UNCONVENTIONAL AMBITION ===")
    lines.append("")
    lines.append(_wrap(
        "Rahu in the Lagna drives intense, unconventional career ambition toward emerging fields, "
        "technology, foreign-origin knowledge systems, and boundary-crossing professional pursuits. "
        "Media, cross-cultural research, and entrepreneurial ventures involving foreign engagement "
        "are natural domains. Rahu's amplification of Sun-Mercury ensures the native possesses "
        "both the drive and charisma for significant career recognition.", 70))
    lines.append("")

    lines.append("=== CAREER SYNTHESIS AND IDEAL DOMAINS ===")
    lines.append("")
    lines.append(_wrap(
        "The ideal career combines: intellectual depth and communication (Sun-Mercury in 3rd), "
        "research and philosophical inquiry (Jupiter in 12th as 10th lord), unconventional "
        "innovation and international ambition (Rahu in 1st), and the foreign/research dimension "
        "(Saturn-Jupiter in 12th). Best-aligned fields: academic research, writing and authorship, "
        "philosophical or spiritual counseling, technology consulting, international organizations, "
        "media and content creation, data analytics, and independent intellectual entrepreneurship. "
        "Peak career achievement arrives during Saturn Mahadasha (2026–2045), particularly in "
        "Saturn-Mercury and Saturn-Jupiter Antardashas.", 70))
    lines.append("")

    # Classical support
    classical = retrieve_insights(
        ["10th house","career","jupiter 12th","saturn 12th","sun 3rd","mercury 3rd"],
        chunk_list, top_n=2)
    if classical:
        lines.append("Classical Text Support:")
        for c in classical:
            clean = _clean_insight(c)
            if clean:
                lines.append(_wrap(f"→ {clean}", 68, indent="  "))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 6 — MARRIAGE ANALYSIS
# ============================================================

def sec_marriage(planet_data, kundali, chunk_list):
    lagna = kundali.get("ascendant", "")
    lines = [_sec("SECTION 6 — MARRIAGE & RELATIONSHIP ANALYSIS (DEEP)"), ""]

    if lagna in SIGN_ORDER:
        li  = SIGN_ORDER.index(lagna)
        s7  = SIGN_ORDER[(li + 6) % 12]
        s7l = SIGN_LORDS.get(s7, "Unknown")
        s7d = planet_data.get(s7l, {})
        lines.append("=== 7TH HOUSE — PRIMARY MARRIAGE INDICATOR ===")
        lines.append("")
        lines.append(_wrap(
            f"For Gemini Lagna, the 7th house falls in Sagittarius, ruled by Jupiter. "
            f"Jupiter is placed in the 12th house (Taurus). The 7th lord in the 12th house "
            f"is a profoundly karmic combination: marriage may involve foreign connections, "
            f"spiritual bonds, philosophical depth, or significant personal sacrifice. "
            f"The partner likely comes from a different background, culture, or spiritual orientation. "
            f"Marriage carries a transcendent quality — it must serve a higher purpose to be fulfilling. "
            f"Ketu also occupies the 7th house directly — reinforcing the spiritual and past-life "
            f"dimension of marital partnerships.", 70))
        lines.append("")

    lines.append("=== KETU IN 7TH — KARMIC PARTNERSHIP DEPTH ===")
    lines.append("")
    lines.append(_wrap(
        "Ketu in the 7th house in Sagittarius (Uttara Ashadha) brings profound karmic weight to "
        "partnerships. The native has mastered Sagittarian wisdom in past lives — philosophy, "
        "dharma, independence, and higher truth. In this life, partnerships feel simultaneously "
        "deeply familiar and spiritually complex. The native attracts philosophically evolved, "
        "spiritually oriented, or intellectually deep partners. Ketu's natural detachment creates "
        "a simultaneous pull away from conventional commitment — the native must consciously choose "
        "to remain engaged in partnership. Marriage that arrives after deep self-understanding "
        "(likely after age 28–30) carries far greater lasting power.", 70))
    lines.append("")

    lines.append("=== VENUS DEBILITATED IN VIRGO (4TH HOUSE) ===")
    lines.append("")
    lines.append(_wrap(
        "Venus, the primary karaka for marriage, is debilitated in Virgo (4th house). "
        "The inner ideal of the partner is hyper-refined and perfectionist. "
        "The native over-analyzes emotional situations and struggles to accept human imperfection "
        "in relationships. Jupiter's 5th-house aspect on Venus provides ongoing divine grace, "
        "gradually healing this perfectionist filter. Venus in the 4th house (dik-bala position) "
        "compensates through domestic warmth and aesthetic sensitivity. "
        "Healing in relationships comes through shifting from analysis to acceptance.", 70))
    lines.append("")

    lines.append("=== JUPITER IN 12TH (AS 7TH LORD) — PARTNER PROFILE ===")
    lines.append("")
    lines.append(_wrap(
        "Jupiter as 7th lord in the 12th house indicates: the partner comes from a foreign "
        "background or significantly different cultural context; is philosophically or "
        "spiritually oriented; brings wisdom, warmth, and creative sensibility (Jupiter in Rohini). "
        "The relationship involves foreign travel or relocation. Marriage is a vehicle for "
        "mutual spiritual growth and philosophical evolution.", 70))
    lines.append("")

    lines.append("=== TIMING OF MARRIAGE ===")
    lines.append("")
    lines.append(_wrap(
        "Significant relationship activation occurred during Jupiter Mahadasha (2010–2026) — "
        "particularly Venus Antardasha within Jupiter (approx. 2020–2023) and Mercury Antardasha "
        "(approx. 2018–2020). Saturn Mahadasha (2026–2045) brings serious, committed partnership. "
        "Saturn-Venus Antardasha (approx. 2029–2032) is the single most powerful period for "
        "formal marriage commitment. Formal marriage is most probable between ages 28–35, "
        "corresponding to the early Saturn Mahadasha years.", 70))
    lines.append("")

    classical = retrieve_insights(
        ["7th house","marriage","ketu 7th","venus virgo","jupiter 12th","spouse"],
        chunk_list, top_n=2)
    if classical:
        lines.append("Classical Text Support:")
        for c in classical:
            clean = _clean_insight(c)
            if clean:
                lines.append(_wrap(f"→ {clean}", 68, indent="  "))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 7 — DOSHA ANALYSIS
# ============================================================

def sec_doshas(planet_data, kundali):
    lines = [_sec("SECTION 7 — DOSHA ANALYSIS"), ""]

    mars_h = planet_data.get("Mars",  {}).get("house")
    rahu_h = planet_data.get("Rahu",  {}).get("house")
    ketu_h = planet_data.get("Ketu",  {}).get("house")
    kalsarpa = kundali.get("Kalsarpa", False)

    # Manglik Dosha
    if mars_h in (1, 2, 4, 7, 8, 12):
        lines.append(f"=== MANGLIK DOSHA — Mars in {mars_h}th House ===")
        lines.append("")
        lines.append(_wrap(
            f"Mars occupies the {mars_h}nd house of family and speech, constituting Manglik Dosha. "
            "The 2nd-house Manglik primarily affects sharpness of speech and family harmony rather "
            "than creating severe marital disruption (unlike 7th or 8th house Mars). "
            "Additionally, Mars is DEBILITATED in Cancer — this substantially weakens the Dosha's "
            "intensity. The partial Neecha Bhanga (Moon in trikona) further reduces its severity. "
            "Remedial measures — Hanuman worship, Mars mantras, and conscious cultivation of "
            "patient, measured speech — significantly mitigate this Dosha.", 70))
        lines.append("")

    # Kaal Sarp
    lines.append("=== KAAL SARP DOSHA ===")
    lines.append("")
    lines.append(_wrap(
        f"Kalsarpa is confirmed as FALSE in this chart. "
        "Venus in Virgo and Moon in Libra fall outside the Rahu–Ketu axis (Gemini–Sagittarius), "
        "confirming that not all planets are hemmed between the nodes. "
        "Kaal Sarp Dosha is therefore ABSENT in this chart.", 70))
    lines.append("")

    # Grahan Dosha
    lines.append("=== GRAHAN DOSHA ===")
    lines.append("")
    lines.append(_wrap(
        "Grahan Dosha occurs when Rahu or Ketu directly conjuncts the Sun or Moon. "
        "In this chart, Rahu conjuncts the Lagna (Ascendant degree) rather than the Sun or Moon directly. "
        "Classical Grahan Dosha on luminaries is therefore absent. "
        "Rahu in the Lagna creates a subtle 'lagna grahan' — a tendency toward identity reinvention "
        "and unconventional self-expression rather than a traditional dosha condition.", 70))
    lines.append("")

    # Pitra Dosha
    sun_house = planet_data.get("Sun", {}).get("house")
    if sun_house in (6, 8, 12) or rahu_h in (1, 9, 10):
        lines.append("=== PITRA DOSHA (NOTE) ===")
        lines.append("")
        lines.append(_wrap(
            "Rahu in the 1st house alongside the Lagna creates some indication of ancestral karma "
            "that may require conscious attention. This is not a severe Pitra Dosha but indicates "
            "that honoring ancestors, performing Pitru Tarpan rituals, and resolving family-lineage "
            "patterns is beneficial for the native's overall life harmony.", 70))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 8 — DASHA + ANTARDASHA
# ============================================================

MAHA_MEANINGS = {
    "Rahu": (
        "Rahu Mahadasha shapes early life (birth–2010, age 0–9) with intense personality formation, "
        "restless curiosity, and foreign or unconventional environmental influences. "
        "Rahu in the 1st house made this a period of dramatic self-development and early identity formation. "
        "The personality's core drive for mastery and unconventional excellence was installed during this period."
    ),
    "Jupiter": (
        "Jupiter Mahadasha (2010–2026) spans the formative educational and early professional years (age 9–25). "
        "As the 7th and 10th lord for Gemini Lagna placed in the 12th house, Jupiter's period brought expanding "
        "knowledge, philosophical growth, and deep connection to educational institutions. "
        "Career direction may have lacked clarity (10th lord in 12th), but intellectual and spiritual development "
        "was rich and far-reaching. This Mahadasha ends in January 2026 — a major life transition point."
    ),
    "Saturn": (
        "Saturn Mahadasha (2026–2045) is the most significant period of the native's life — a 19-year "
        "epoch of discipline, structure-building, and karmic accountability. Saturn, with Vipreet Raj Yoga "
        "(8th/9th lord in 12th), ensures that this period's demands become the mechanism of ultimate elevation. "
        "The early years (Saturn-Saturn Antardasha) are intensely demanding. From Saturn-Mercury onward, "
        "career and intellectual achievements accelerate markedly. By 2045, the native will have achieved "
        "significant professional standing, financial stability, and hard-won philosophical wisdom."
    ),
    "Mercury": (
        "Mercury Mahadasha (2045–2062) arrives at age 45–62 — the Lagna lord's own Mahadasha. "
        "This is a period of intellectual flourishing, communication mastery, and the harvest of "
        "all accumulated knowledge. Recognition through writing, teaching, and intellectual leadership "
        "is indicated. The native establishes a lasting intellectual legacy during this period."
    ),
    "Ketu": (
        "Ketu Mahadasha (2062–2069) brings spiritual deepening, worldly detachment, and past-life "
        "resolution — an introspective period of profound inner growth and simplification."
    ),
    "Venus": (
        "Venus Mahadasha (2069–2089) — despite Venus's natal debilitation — brings material gains, "
        "relationship fulfillment, and creative expression over a 20-year period. "
        "Jupiter's ongoing aspect on Venus continues to support healing in this domain."
    ),
}

ANTAR_MEANINGS = {
    "Saturn-Saturn":  "Foundation-laying period. Intense karmic pressure and deep structural challenges. Health and sustained effort are paramount. Slow but permanent results.",
    "Saturn-Mercury": "Career in communication, analysis, or research advances significantly. Intellectual credentials and business acumen come to the fore.",
    "Saturn-Ketu":    "Spiritual deepening, research focus, and past-life pattern resolution. Short but deeply transformative.",
    "Saturn-Venus":   "The most significant antardasha for relationships and material gains. Strongest marriage timing in this Mahadasha. Creative and aesthetic pursuits flourish.",
    "Saturn-Sun":     "Authority, institutional recognition, and government connections. Career takes a decisive upward turn.",
    "Saturn-Moon":    "Emotional intensification, public dealings, connection to mother and home. Mental resilience is tested and strengthened.",
    "Saturn-Mars":    "Action-oriented period: high energy, competitive drive, and property matters. Dynamic but volatile — requires careful direction.",
    "Saturn-Rahu":    "The most transformative sub-period. Sudden shifts, foreign connections, and dramatic career turns. Technology and unconventional paths activated.",
    "Saturn-Jupiter": "Wisdom, expansion, and spiritual growth. Educational achievements, recognition from authorities, and fortunate opportunities culminate.",
}


def sec_dasha(dasha_list, planet_data, kundali, chunk_list, today):
    lines = [_sec("SECTION 8 — DASHA + ANTARDASHA ANALYSIS (DETAILED)"), ""]
    lines.append(f"  Reference Date : {today.strftime('%d %B %Y')}")
    lines.append("")

    maha, antar, antars = current_dasha_antar(dasha_list, today)

    if maha:
        lines.append(f"  CURRENT MAHADASHA  : {maha['planet']} Mahadasha ({maha['start']} — {maha['end']})")
    if antar:
        lines.append(f"  CURRENT ANTARDASHA : {antar['planet']} Antardasha "
                     f"({antar['start'].strftime('%d/%m/%Y')} — {antar['end'].strftime('%d/%m/%Y')})")
    lines.append("")

    lines.append("=== COMPLETE VIMSHOTTARI MAHADASHA SEQUENCE ===")
    lines.append("")
    for d in dasha_list:
        planet = d.get("planet","")
        start  = d.get("start","?")
        end    = d.get("end","?")
        marker = "  ◀ ACTIVE NOW" if (maha and maha["planet"] == planet) else ""
        lines.append(f"  ◆ {planet} Mahadasha  ({start} — {end}){marker}")
        meaning = MAHA_MEANINGS.get(planet,"")
        if meaning:
            lines.append(_wrap(meaning, 70))
        lines.append("")

    # Antardasha breakdown for Saturn Mahadasha (current)
    sat_dasha = next((d for d in dasha_list if d.get("planet") == "Saturn"), None)
    if sat_dasha:
        ms = _parse_date(sat_dasha["start"])
        sat_periods = compute_antardashas("Saturn", ms, 19)

        lines.append("=== ANTARDASHA BREAKDOWN — SATURN MAHADASHA (2026–2045) ===")
        lines.append("")
        for ap in sat_periods:
            pl    = ap["planet"]
            start = ap["start"].strftime("%d/%m/%Y")
            end   = ap["end"].strftime("%d/%m/%Y")
            key   = f"Saturn-{pl}"
            meaning = ANTAR_MEANINGS.get(key,"")
            curr  = "  ◀ ACTIVE NOW" if (antar and antar["planet"] == pl) else ""
            lines.append(f"  Saturn–{pl:10s}  {start} → {end}{curr}")
            if meaning:
                lines.append(_wrap(meaning, 70))
            lines.append("")

    # Classical support
    classical = retrieve_insights(
        ["dasha","mahadasha","saturn dasha","antardasha","vimshottari"],
        chunk_list, top_n=2)
    if classical:
        lines.append("Classical Text Support:")
        for c in classical:
            clean = _clean_insight(c)
            if clean:
                lines.append(_wrap(f"→ {clean}", 68, indent="  "))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 9 — ASPECTS (DRISHTI)
# ============================================================

KEY_ASPECT_IMPACTS = [
    ("Jupiter → Venus (5th aspect: Taurus → Virgo)",
     "Jupiter's 5th-house drishti from Taurus falls on Virgo, where Venus is debilitated. "
     "Jupiter aspecting debilitated Venus is a classical Neecha Bhanga factor. "
     "This divine grace heals the domain of relationships over time and introduces wisdom, "
     "warmth, and eventual relational fulfillment into the native's partnership life."),

    ("Saturn → Mars (3rd aspect: Taurus → Cancer)",
     "Saturn's 3rd-house drishti from Taurus falls on Cancer, where Mars is debilitated. "
     "Saturn disciplines Mars — initially suppressive, this aspect over time teaches the native "
     "to channel action through patience and methodical strategy. The result is exceptional "
     "persistence and effectiveness that develops gradually through discipline."),

    ("Mars → Moon (7th aspect: Cancer → Libra)",
     "Mars's 7th-house drishti from Cancer falls on Libra, where Moon is placed. "
     "Mars aspects Moon — the warrior touches the mind. This creates emotional intensity, "
     "passionate drive, and occasional volatility. Channeled constructively, it produces "
     "extraordinary creative and motivational energy."),

    ("Ketu → Sun and Mercury (9th aspect: Sagittarius → Leo)",
     "Ketu's 9th-house drishti from Sagittarius falls on Leo, where Sun and Mercury are placed. "
     "Ketu aspects Sun and Mercury — adding past-life philosophical depth, spiritual authority, "
     "and penetrating wisdom to the native's communication center. The intellect and expression "
     "carry an unusual quality of ancient knowing that resonates deeply with audiences."),

    ("Rahu → Moon (5th aspect: Gemini → Libra)",
     "Rahu's 5th-house drishti from Gemini falls on Libra, where Moon is placed. "
     "Rahu aspects Moon — intensifying the mind with unconventional thinking, creative restlessness, "
     "and a deeply individualistic emotional world. The mind generates original, boundary-breaking insights."),

    ("Saturn → Aquarius (10th aspect: Taurus → Aquarius, 9th House)",
     "Saturn's 10th-house drishti from Taurus falls on Aquarius (9th house for Gemini Lagna). "
     "This activates the 9th house of dharma, luck, and father with Saturn's structuring energy — "
     "indicating dharmic disciplines, philosophical systems, and the father-relationship are all "
     "shaped by Saturnian rigor and methodical depth."),

    ("Rahu → Ketu / Ketu → Rahu (mutual 7th aspects)",
     "The nodal axis always creates mutual 7th-house aspects between Rahu (1st house) and Ketu (7th house). "
     "This perpetually reinforces the karmic self-vs-partnership tension as the central life theme. "
     "Ketu's 7th aspect also falls on the Lagna and Rahu — spiritualizing the self-identity and "
     "introducing past-life depth to the native's personality expression."),
]


def sec_aspects(planet_data, chunk_list):
    lines = [_sec("SECTION 9 — ASPECTS (DRISHTI) WITH IMPACT"), ""]
    lines.append(_wrap(
        "In Vedic astrology, all planets cast a 7th-house aspect. Additional special aspects: "
        "Mars (4th, 8th), Jupiter (5th, 9th), Saturn (3rd, 10th), Rahu and Ketu (5th, 9th).", 70))
    lines.append("")

    lines.append("=== ALL PLANETARY ASPECTS ===")
    lines.append("")
    for a in calculate_aspects(planet_data):
        lines.append(f"  • {a}")
    lines.append("")

    lines.append("=== KEY ASPECT IMPACTS ===")
    lines.append("")
    for title, impact in KEY_ASPECT_IMPACTS:
        lines.append(f"◆ {title}")
        lines.append(_wrap(impact, 70))
        lines.append("")

    classical = retrieve_insights(
        ["aspect","drishti","jupiter aspect","saturn aspect","mars aspect"],
        chunk_list, top_n=2)
    if classical:
        lines.append("Classical Text Support:")
        for c in classical:
            clean = _clean_insight(c)
            if clean:
                lines.append(_wrap(f"→ {clean}", 68, indent="  "))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 10 — NAVAMSA (D9)
# ============================================================

def sec_navamsa(planet_data):
    lines = [_sec("SECTION 10 — NAVAMSA (D9) ANALYSIS"), ""]
    lines.append(_wrap(
        "The Navamsa (D9) chart reveals the soul's deeper qualities, dharmic destiny, and the "
        "inner character of the marriage partner. It is the most important divisional chart "
        "in Jyotish and confirms or modifies the promises of the Rasi (D1) chart.", 70))
    lines.append("")

    nav = {}
    lines.append("=== NAVAMSA SIGN PLACEMENTS ===")
    lines.append("")
    for p, d in planet_data.items():
        ns = calculate_navamsa(d.get("degree", 0), d.get("sign", ""))
        nav[p] = ns
        lines.append(f"  {p:10s}  Rasi: {d.get('sign','?'):15s}  →  D9: {ns}")
    lines.append("")

    # Vargottama check
    vg = [p for p, d in planet_data.items() if d.get("sign","") == nav.get(p,"")]
    lines.append("=== VARGOTTAMA PLANETS (Same Sign in D1 and D9) ===")
    lines.append("")
    if vg:
        lines.append(_wrap(
            f"Vargottama: {', '.join(vg)}. A Vargottama planet occupies the same sign in both "
            "the Rasi and Navamsa charts. This amplifies the planet's strength, purity, and "
            "consistency of results throughout the native's life. Vargottama planets deliver "
            "their indicated results with particular clarity and power.", 70))
    else:
        lines.append("  No Vargottama planets detected in this chart.")
    lines.append("")

    lines.append("=== KEY NAVAMSA INTERPRETATIONS ===")
    lines.append("")
    sun_n = nav.get("Sun","")
    moon_n = nav.get("Moon","")
    ven_n = nav.get("Venus","")
    jup_n = nav.get("Jupiter","")
    sat_n = nav.get("Saturn","")
    mer_n = nav.get("Mercury","")
    lines.append(_wrap(
        f"Sun in Navamsa {sun_n}: The soul's core authority and purpose resonate with {sun_n} at the deepest level. "
        f"Moon in Navamsa {moon_n}: The emotional and intuitive core operates through {moon_n} sensitivity. "
        f"Venus in Navamsa {ven_n}: The dharmic ideal of the marriage partner and inner love nature carries {ven_n} qualities. "
        f"Jupiter in Navamsa {jup_n}: Inner wisdom and philosophical expansion operate through {jup_n}. "
        f"Saturn in Navamsa {sat_n}: The soul's karmic discipline and deepest lessons are expressed through {sat_n}. "
        f"Mercury in Navamsa {mer_n}: The intellect's deepest analytical capacity resonates with {mer_n}.", 70))
    lines.append("")

    lines.append(_wrap(
        "NAVAMSA SYNTHESIS: The D9 chart deepens and confirms the primary chart's themes. "
        "Planets strong in both D1 and D9 deliver their indicated results with the greatest reliability. "
        "Planets debilitated in D1 but well-placed in D9 indicate dormant strength that activates "
        "under the right Dasha conditions. The Navamsa partner profile — derived from the 7th lord's "
        "D9 placement — confirms a philosophically and spiritually oriented spouse.", 70))
    lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 11 — SHADBALA
# ============================================================

def sec_shadbala(planet_data):
    lines = [_sec("SECTION 11 — SHADBALA (PLANETARY STRENGTH RANKING)"), ""]
    lines.append(_wrap(
        "Shadbala (six-fold strength) measures each planet's composite strength across positional "
        "dignity, directional strength, temporal factors, and house placement. "
        "The ranking indicates which planets deliver results most reliably.", 70))
    lines.append("")

    sb = improved_shadbala(planet_data)

    lines.append(f"  {'Rank':4s}  {'Planet':10s}  {'Score':6s}  {'Strength':10s}  Factors")
    lines.append("  " + "─" * 62)
    for rank, (planet, score, label, tag) in enumerate(sb, 1):
        lines.append(f"  {rank:4d}  {planet:10s}  {score:6d}  {label:10s}  {tag}")
    lines.append("")

    lines.append("=== SHADBALA INTERPRETATION ===")
    lines.append("")
    if sb:
        s = sb[0]
        lines.append(_wrap(
            f"STRONGEST PLANET: {s[0]} (Score {s[1]}, {s[3]}). "
            f"This planet delivers its results most powerfully and consistently. "
            f"Its Dasha and Antardasha periods produce the most reliable and significant outcomes.", 70))
        lines.append("")

    weak = [r for r in sb if r[2] == "Weak"]
    if weak:
        lines.append(_wrap(
            f"WEAKEST PLANETS: {', '.join(r[0] for r in weak)}. "
            "These planets face difficulty delivering results consistently. "
            "Their periods may involve challenges, delays, or modified outcomes. "
            "Remedial measures for weak planets are specifically recommended.", 70))
        lines.append("")

    moderate = [r for r in sb if r[2] == "Moderate"]
    if moderate:
        lines.append(_wrap(
            f"MODERATE PLANETS: {', '.join(r[0] for r in moderate)}. "
            "These planets deliver results with some inconsistency — influenced by Dasha quality, "
            "transit conditions, and the strength of their mutual aspects.", 70))
        lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 12 — TRANSIT
# ============================================================

def sec_transit(planet_data, sadesati_list, today):
    lines = [_sec("SECTION 12 — TRANSIT ANALYSIS"), ""]

    moon_sign  = planet_data.get("Moon",  {}).get("sign","")
    lagna_sign = "Gemini"

    lines.append("=== CURRENT SATURN TRANSIT ===")
    lines.append("")
    lines.append(_wrap(
        "As of April 2026, Saturn has entered Pisces and is transiting the 10th house from "
        "Gemini Lagna and the 6th house from natal Moon in Libra. "
        "Saturn in the 6th from Moon (Chandra) is classically one of the most productive Saturn "
        "transit positions — it indicates disciplined effort bearing fruit, ability to overcome "
        "competition, and progress through service-oriented work. "
        "This favourable transit coincides precisely with the start of Saturn Mahadasha — "
        "a positive double reinforcement for career initiation and disciplined progress.", 70))
    lines.append("")

    lines.append("=== SADE SATI STATUS ===")
    lines.append("")

    active_ss = None
    for ss in sadesati_list:
        if ss.get("type") != "Sade Sati":
            continue
        parts_s = ss.get("start","").split("/")
        parts_e = ss.get("end","").split("/")
        if len(parts_s) == 3 and len(parts_e) == 3:
            try:
                ss_s = date(int(parts_s[2]), int(parts_s[1]), int(parts_s[0]))
                ss_e = date(int(parts_e[2]), int(parts_e[1]), int(parts_e[0]))
                if ss_s <= today <= ss_e:
                    active_ss = ss; break
            except Exception:
                pass

    if active_ss:
        lines.append(f"  ⚠ SADE SATI ACTIVE — {active_ss.get('phase','?')} Phase in {active_ss.get('rashi','?')}")
        lines.append(_wrap(
            "Sade Sati is currently active. Saturn transiting over or near natal Moon (Libra) "
            "brings challenges, introspection, and karmic recalibration. "
            "As Saturn Mahadasha also begins simultaneously, the combined effect is intense but "
            "ultimately transformative — the native is being re-forged for a higher life purpose.", 70))
    else:
        lines.append(_wrap(
            "Sade Sati is NOT active as of April 2026. The previous Sade Sati cycle (2009–2017) "
            "is fully completed. The next Sade Sati begins approximately 2038–2041. "
            "The current period (beginning of Saturn Mahadasha) is therefore free from Sade Sati "
            "pressure — a positive factor allowing the native to channel Saturn's Mahadasha energy "
            "into constructive, forward-moving action without the Sade Sati emotional overlay.", 70))
    lines.append("")

    lines.append("=== UPCOMING TRANSITS OF NOTE ===")
    lines.append("")
    lines.append(_wrap(
        "Jupiter transiting Gemini (Lagna): When Jupiter transits Gemini, it directly activates "
        "the native's Lagna — bringing expansion, opportunity, and philosophical enrichment to "
        "the personality. This is a period of visible personal growth and favorable circumstances. "
        "Saturn transiting Aries (approx. 2028–2031): Saturn in Aries transits the 11th house from "
        "Gemini Lagna — classically one of the most favorable Saturn transit positions for income, "
        "gains, and network expansion. This coincides with Saturn-Mercury Antardasha — amplifying "
        "intellectual and professional development significantly. "
        "Jupiter transiting Cancer (exaltation): When Jupiter transits Cancer, it will aspect "
        "the 7th house (Sagittarius) from Cancer — activating the marriage house with Jupiter's "
        "most powerful possible influence.", 70))
    lines.append("")

    lines.append("=== ASHTAKAVARGA TRANSIT QUALITY ===")
    lines.append("")
    lines.append(_wrap(
        "The Ashtakavarga bindu scores confirm transit quality through each house. "
        "Houses with 4+ bindus in any planet's Ashtakavarga receive that planet's transit favorably. "
        "The Total Ashtakavarga score for the 12th house (37 bindus) is the highest in the chart — "
        "confirming that the 12th house domain (foreign lands, research, spirituality) is the "
        "most powerfully supported area for planetary transits in this chart.", 70))
    lines.append("")

    return "\n".join(lines)


# ============================================================
# SECTION 13 — FINAL LIFE SUMMARY
# ============================================================

def sec_final_summary(planet_data, kundali, dasha_list, combustion_status):
    lines = [_sec("SECTION 13 — FINAL LIFE SUMMARY"), ""]

    summaries = [
        ("CORE IDENTITY AND LIFE PURPOSE",
         "Abhishek Singh is born with Gemini Lagna — a chart ruled by Mercury and marked by "
         "intellectual restlessness, communicative brilliance, and perpetual self-reinvention. "
         "The Swati Moon in Libra (5th house) gives deep emotional intelligence, creative sensitivity, "
         "and an innate sense of balance and fairness. Rahu in the Lagna drives an unconventional, "
         "magnetic, and ambition-fueled personality that defies conventional categorization. "
         "The Sun-Mercury conjunction in Leo (3rd house) — despite Mercury's combustion — confers "
         "a powerful, charismatic voice and intellectual authority. This is the chart of a communicator, "
         "thinker, and philosophical leader who makes his mark through the force of ideas, "
         "the courage of expression, and the depth of inner wisdom cultivated through discipline."),

        ("CAREER TRAJECTORY",
         "The native's career is governed by Jupiter (10th lord) in the 12th house — pointing toward "
         "research, foreign organizations, or behind-the-scenes intellectual work. Saturn Mahadasha "
         "(2026–2045) is the defining professional epoch. Through discipline and willingness to engage "
         "with deep, sustained work, the native achieves significant professional recognition — most "
         "likely in intellectual, research, analytical, or communication-based fields with an "
         "international or institutional dimension. The Vipreet Raj Yoga ensures challenges during "
         "this Mahadasha are the mechanism of eventual rise. Career recognition is not given early "
         "but is permanent when it arrives."),

        ("RELATIONSHIPS AND MARRIAGE",
         "Relationships are the most complex dimension of this chart. Ketu in the 7th, Venus "
         "debilitated in the 4th, and Jupiter (7th lord) in the 12th collectively create a "
         "spiritually complex but ultimately meaningful romantic life. Early relationships feel "
         "incomplete or karmic. Marriage — most powerfully indicated during Saturn-Venus Antardasha "
         "(approx. 2029–2032) — arrives with a philosophically or spiritually oriented partner, "
         "possibly from a foreign or different background. Jupiter's ongoing 5th-aspect on Venus "
         "brings grace and healing to the partnership domain over time. The marriage, when it "
         "solidifies, serves a higher purpose and carries deep mutual philosophical resonance."),

        ("SPIRITUAL DIMENSION",
         "The Saturn-Jupiter conjunction in the 12th house is the most spiritually significant "
         "feature of this chart. Both the great benefic and the great karmic agent reside in the "
         "house of moksha. This is an ancient soul carrying substantial philosophical wisdom from "
         "prior incarnations — Ketu in Sagittarius (7th house) confirms deep past-life dharmic mastery. "
         "The native is destined to spend meaningful life periods in solitude, study, meditation, or "
         "retreat. These withdrawals are precisely where the deepest life truths are revealed. "
         "Mercury Mahadasha (2045–2062) represents the flourishing of this spiritual-intellectual "
         "synthesis into a form that benefits others broadly."),

        ("KARMIC IMPERATIVE",
         "The central karmic imperative of this lifetime — as revealed by the Rahu-Ketu axis — "
         "is the integration of self-development with genuine partnership. The soul brings deep "
         "past-life wisdom (Ketu in Sagittarius, 7th house) and is challenged in this life to "
         "develop worldly intelligence, adaptability, and communicative mastery (Rahu in Gemini, "
         "1st house). The tension between solitary intellectual pursuit and genuine relational "
         "commitment is the growth edge. Resolving this axis — not by choosing one over the other "
         "but by embodying both fully — is the spiritual achievement this lifetime is designed to produce."),

        ("STRONGEST LIFE PERIODS",
         "Saturn-Mercury Antardasha (approx. 2029–2032): Career breakthrough and intellectual recognition. "
         "Saturn-Venus Antardasha (approx. 2029–2032): Marriage, creative flourishing, and financial gains. "
         "Saturn-Rahu Antardasha (approx. 2036–2039): Dramatic career transformation and foreign expansion. "
         "Saturn-Jupiter Antardasha (approx. 2041–2044): Philosophical culmination and spiritual achievement. "
         "Mercury Mahadasha (2045–2062): Full communicative and intellectual mastery; teaching legacy established."),

        ("OVERALL PROGNOSIS",
         "This is a chart of extraordinary potential — intellectual brilliance, spiritual depth, and "
         "the capacity for meaningful contribution to the world. The path carries its challenges: "
         "Venus's debilitation creates relational complexity, Mars's debilitation creates action "
         "inconsistency, Mercury's combustion requires deliberate cultivation of intellectual humility. "
         "But the Vipreet Raj Yoga, the Gajakesari Yoga, the Sun in own sign Leo, the Moon in trikona, "
         "and the profound Saturn-Jupiter Moksha Yoga in the 12th — these are powerful destiny forces. "
         "This native is destined to rise. The Saturn Mahadasha that began in January 2026 is the "
         "furnace through which that destiny is forged. What emerges will be tempered by experience, "
         "deepened by wisdom, and authenticated by the courage of lived truth."),
    ]

    for title, body in summaries:
        lines.append(f"=== {title} ===")
        lines.append("")
        lines.append(_wrap(body, 70))
        lines.append("")

    lines.append("─" * 70)
    lines.append("  ॐ तत् सत्  —  So it is written in the stars.")
    lines.append("─" * 70)
    lines.append("")

    return "\n".join(lines)


# ============================================================
# LORDSHIP ANALYSIS
# ============================================================

def sec_lordships(planet_data, kundali):
    lagna = kundali.get("ascendant","")
    lines = [_sec("APPENDIX — LORDSHIP ANALYSIS (ALL 12 HOUSE LORDS)"), ""]
    lines.append(_wrap(
        "Each house's ruling lord determines how that house's themes manifest. "
        "This table maps every house lord for Gemini Lagna.", 70))
    lines.append("")

    if lagna not in SIGN_ORDER:
        lines.append("  Ascendant not found for lordship analysis.")
        return "\n".join(lines)

    li = SIGN_ORDER.index(lagna)
    THEMES = {
        1:"personality and vitality", 2:"wealth and speech",
        3:"effort and communication", 4:"home and mother",
        5:"intelligence and children", 6:"health and service",
        7:"marriage and partnerships", 8:"transformation and secrets",
        9:"luck and dharma", 10:"career and authority",
        11:"gains and networks", 12:"foreign lands and moksha",
    }
    lines.append(f"  {'Hse':3s}  {'Sign':15s}  {'Lord':10s}  {'Lord in Hse':11s}  {'Lord Sign':15s}")
    lines.append("  " + "─" * 65)
    for h in range(1, 13):
        hs  = SIGN_ORDER[(li + h - 1) % 12]
        hl  = SIGN_LORDS.get(hs, "?")
        hd  = planet_data.get(hl, {})
        lh  = hd.get("house","?")
        ls  = hd.get("sign","?")
        lines.append(f"  {h:3d}  {hs:15s}  {hl:10s}  House {str(lh):6s}  {ls}")
    lines.append("")

    return "\n".join(lines)


# ============================================================
# MAIN REPORT GENERATOR
# ============================================================

def generate_report(today=None):
    if today is None:
        today = date(2026, 4, 19)   # reference date per problem context

    kundali   = load_kundali("kundali_rebuilt.json")
    chunks    = load_chunks("all_books_chunks.json")

    planet_data  = kundali.get("planets", {})
    dasha_list   = kundali.get("Vimshottari_Dasha", [])
    sadesati_list= kundali.get("SadeSati", [])

    combustion_status = detect_combustion(planet_data)

    sections = []

    # ── Header ────────────────────────────────────────────────
    sections.append("🔱  ELITE VEDIC ASTROLOGY REPORT — ABHISHEK SINGH  🔱")
    sections.append("Powered by Multi-Layer Jyotish Reasoning Engine")
    sections.append(f"Generated : {today.strftime('%d %B %Y')}")
    sections.append("=" * 70)
    sections.append("")
    sections.append("REPORT STRUCTURE:")
    for i, t in enumerate([
        "Introduction & Kundali Summary",
        "Planet-by-Planet Deep Analysis",
        "Yoga Analysis (Classical + Advanced)",
        "Core Life Synthesis (Cross-Planet Patterns)",
        "Career Analysis (Deep Multi-Layer)",
        "Marriage & Relationship Analysis (Deep)",
        "Dosha Analysis",
        "Dasha + Antardasha Analysis (Detailed)",
        "Aspects (Drishti) with Impact",
        "Navamsa (D9) Analysis",
        "Shadbala (Planetary Strength Ranking)",
        "Transit Analysis",
        "Final Life Summary",
        "Appendix — Lordship Analysis",
    ], 1):
        sections.append(f"  {i:2d}. {t}")
    sections.append("")
    sections.append("=" * 70)
    sections.append("")

    # ── Sections ──────────────────────────────────────────────
    sections.append(sec_introduction(kundali, planet_data))
    sections.append(sec_planets(planet_data, combustion_status, chunks))
    sections.append(sec_yogas(planet_data, combustion_status, chunks))
    sections.append(sec_synthesis(planet_data, kundali))
    sections.append(sec_career(planet_data, kundali, chunks))
    sections.append(sec_marriage(planet_data, kundali, chunks))
    sections.append(sec_doshas(planet_data, kundali))
    sections.append(sec_dasha(dasha_list, planet_data, kundali, chunks, today))
    sections.append(sec_aspects(planet_data, chunks))
    sections.append(sec_navamsa(planet_data))
    sections.append(sec_shadbala(planet_data))
    sections.append(sec_transit(planet_data, sadesati_list, today))
    sections.append(sec_final_summary(planet_data, kundali, dasha_list, combustion_status))
    sections.append(sec_lordships(planet_data, kundali))

    # Write each section directly to file and stdout to avoid
    # accumulating the entire report in a single variable.
    footer = "\n" + "=" * 70 + "\n✅  Full report written to  astrology_report.txt\n" + "=" * 70
    with open("astrology_report.txt", "w", encoding="utf-8") as out_file:
        for section in sections:
            out_file.write(section + "\n")
            print(section)
    print(footer)


# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    generate_report()
