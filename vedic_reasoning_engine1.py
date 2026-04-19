from collections import defaultdict
import itertools
import json
import sys

# -------------------------------
# LOAD FILES
# -------------------------------
with open("kundali_rebuilt.json", "r") as f:
    kundali = json.load(f)

# filtered_chunks.json — list of {"id": ..., "text": ...}
with open("filtered_chunks.json", "r") as f:
    chunks = json.load(f)

# all_books_chunked.json — dict keyed by book title; each value has
# {"total_chunks": N, "chunks": [str, str, ...]}
# Normalize to {"text": ..., "book": ...} dicts so all sources share one format.
with open("all_books_chunked.json", "r") as f:
    _all_books_chunked_raw = json.load(f)

_all_books_chunked_normalized = []
for _book_title, _book_data in _all_books_chunked_raw.items():
    for _chunk_item in _book_data.get("chunks", []):
        if isinstance(_chunk_item, str):
            _all_books_chunked_normalized.append({"text": _chunk_item, "book": _book_title})
        elif isinstance(_chunk_item, dict):
            # Ensure the book field is present for consistency across all sources.
            entry = dict(_chunk_item)
            entry.setdefault("book", _book_title)
            _all_books_chunked_normalized.append(entry)

# all_books_chunks.json — list of {"text": ..., "page": ..., "book": ...}
with open("all_books_chunks.json", "r") as f:
    _all_books_chunks = json.load(f)

# Combined pool used by retrieve_insights for full-dataset coverage.
# itertools.chain avoids creating intermediate list copies for large datasets.
# Order: filtered_chunks first (highest quality), then both large datasets.
all_chunks = list(itertools.chain(chunks, _all_books_chunked_normalized, _all_books_chunks))

planets = kundali["planets"]
dasha = kundali.get("Vimshottari_Dasha", [])
sadesati = kundali.get("SadeSati", [])

# -------------------------------
# SMART RETRIEVAL (UPGRADED)
# -------------------------------
_LOGIC_WORDS = ["house", "effect", "result", "indicates", "gives", "causes"]

_RETRIEVAL_BATCH_SIZE = 500  # process chunks in batches to stay memory-efficient


def retrieve_insights(keywords, chunk_list=None):
    """Search *every* chunk in chunk_list for keyword relevance.

    Processes the full dataset in batches of _RETRIEVAL_BATCH_SIZE so that no
    chunk is ever skipped, even for very large inputs.  Defaults to all_chunks
    which combines all four loaded datasets for complete coverage.
    """
    if chunk_list is None:
        chunk_list = all_chunks

    scored = []
    total = len(chunk_list)

    # Iterate the entire dataset in fixed-size batches — no skipping, no sampling.
    for batch_start in range(0, total, _RETRIEVAL_BATCH_SIZE):
        batch = chunk_list[batch_start: batch_start + _RETRIEVAL_BATCH_SIZE]

        for chunk in batch:
            text = chunk.get("text", "").lower()

            score = 0

            # strong keyword match
            for k in keywords:
                if k in text:
                    score += 2

            # logic words bonus
            for lw in _LOGIC_WORDS:
                if lw in text:
                    score += 1

            if score >= 3:
                scored.append((score, chunk["text"]))

    scored.sort(reverse=True, key=lambda x: x[0])

    return [text for _, text in scored[:5]]


# -------------------------------
# INTERPRETATION LAYER
# -------------------------------
def interpret_text(raw_text):
    text = raw_text.strip()

    # Remove noisy OCR patterns
    if any(x in text.lower() for x in [
        "page", "chapter", "pdf", "copyright",
        "vedic remedies in astrology", "brihat parasara"
    ]):
        return None

    # Clean formatting
    text = text.replace("\n", " ").strip()

    # Skip too short or broken text
    if len(text) < 40:
        return None

    # Convert into Jyotish-style interpretation
    return f"This indicates that {text.lower().capitalize()}."


# -------------------------------
# PLANET ANALYSIS
# -------------------------------
def analyze_planets():
    output = []

    for planet, data in planets.items():
        keywords = [
            planet.lower(),
            data.get("sign", "").lower(),
            data.get("nakshatra", "").lower(),
            f"house {data.get('house', '')}"
        ]

        insights = retrieve_insights(keywords)

        section = f"\n=== {planet.upper()} ===\n"
        section += f"{planet} in {data.get('sign', 'N/A')} (House {data.get('house', 'N/A')}, {data.get('nakshatra', 'N/A')})\n"

        for insight in insights:
            interpreted = interpret_text(insight)
            if interpreted:
                section += f"- {interpreted}\n"

        output.append(section)

    return "\n".join(output)


# -------------------------------
# MAHADASHA ANALYSIS
# -------------------------------
def analyze_dasha():
    output = "\n=== MAHADASHA ANALYSIS ===\n"

    for period in dasha[:3]:
        planet = period.get("planet", "Unknown")

        keywords = [
            planet.lower(),
            "dasha",
            "mahadasa",
            "effects"
        ]

        insights = retrieve_insights(keywords)

        output += f"\n{planet} Mahadasha ({period.get('start', '?')} - {period.get('end', '?')}):\n"

        for insight in insights:
            interpreted = interpret_text(insight)
            if interpreted:
                output += f"- {interpreted}\n"

    return output


# -------------------------------
# SADE SATI ANALYSIS
# -------------------------------
def analyze_sadesati():
    output = "\n=== SADE SATI ANALYSIS ===\n"
    output += "Sade Sati is a transit-based influence and should be interpreted along with Dasha and Yogas.\n\n"

    for period in sadesati:
        if period.get("type") == "Sade Sati":
            keywords = [
                "saturn",
                "sade sati",
                period.get("rashi", "").lower()
            ]

            insights = retrieve_insights(keywords)

            output += f"\n{period.get('phase', '?')} Phase ({period.get('start', '?')} - {period.get('end', '?')}):\n"

            for insight in insights:
                interpreted = interpret_text(insight)
                if interpreted:
                    output += f"- {interpreted}\n"

    return output


# -------------------------------
# YOGA DETECTION
# -------------------------------
def detect_yogas():
    output = "\n=== YOGA INDICATIONS ===\n"

    keywords = [
        "yoga",
        "raj yoga",
        "dhan yoga",
        "vipreet",
        "lakshmi yoga"
    ]

    insights = retrieve_insights(keywords)

    for insight in insights:
        interpreted = interpret_text(insight)
        if interpreted:
            output += f"- {interpreted}\n"

    return output


# -------------------------------
# DRISHTI (ASPECTS)
# -------------------------------
SIGN_ORDER = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"
]

# Special additional aspects (houses counted from planet's house, 1-based)
SPECIAL_ASPECTS = {
    "Mars":    [4, 8],
    "Jupiter": [5, 9],
    "Saturn":  [3, 10],
    "Rahu":    [5, 9],
    "Ketu":    [5, 9],
}


def _sign_index(sign):
    try:
        return SIGN_ORDER.index(sign)
    except ValueError:
        return -1


def _house_to_sign(from_sign, houses_away):
    idx = _sign_index(from_sign)
    if idx == -1:
        return None
    return SIGN_ORDER[(idx + houses_away - 1) % 12]


def calculate_aspects(planet_data):
    results = []
    for planet, data in planet_data.items():
        sign = data.get("sign", "")
        aspected_signs = []

        # All planets aspect the 7th sign (opposition)
        seventh = _house_to_sign(sign, 7)
        if seventh:
            aspected_signs.append((7, seventh))

        # Special aspects
        for houses_away in SPECIAL_ASPECTS.get(planet, []):
            asp_sign = _house_to_sign(sign, houses_away)
            if asp_sign:
                aspected_signs.append((houses_away, asp_sign))

        for h, asp_sign in aspected_signs:
            aspected_planets = [
                p for p, d in planet_data.items()
                if d.get("sign") == asp_sign and p != planet
            ]
            desc = f"{planet} ({sign}) aspects {asp_sign} (house-{h} drishti)"
            if aspected_planets:
                desc += f" — aspecting {', '.join(aspected_planets)}"
            results.append(desc)

    return results


# -------------------------------
# NAVAMSA (D9)
# -------------------------------
# First navamsa sign for each element group
_NAVAMSA_START = {
    "fire":  0,   # Aries
    "earth": 9,   # Capricorn
    "air":   6,   # Libra
    "water": 3,   # Cancer
}

_SIGN_ELEMENT = {
    "Aries": "fire", "Leo": "fire", "Sagittarius": "fire",
    "Taurus": "earth", "Virgo": "earth", "Capricorn": "earth",
    "Gemini": "air", "Libra": "air", "Aquarius": "air",
    "Cancer": "water", "Scorpio": "water", "Pisces": "water",
}


def calculate_navamsa(degree, sign):
    """Return the Navamsa (D9) sign for a planet at *degree* in *sign*."""
    element = _SIGN_ELEMENT.get(sign)
    if element is None:
        return "Unknown"

    # Which 3°20' segment within the sign? (0-8)
    segment = int(degree / (30.0 / 9))
    segment = min(segment, 8)

    start_idx = _NAVAMSA_START[element]
    navamsa_idx = (start_idx + segment) % 12
    return SIGN_ORDER[navamsa_idx]


# -------------------------------
# IMPROVED SHADBALA
# -------------------------------
_EXALTATION = {
    "Sun": "Aries", "Moon": "Taurus", "Mars": "Capricorn",
    "Mercury": "Virgo", "Jupiter": "Cancer", "Venus": "Pisces",
    "Saturn": "Libra",
}
_DEBILITATION = {
    "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
    "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo",
    "Saturn": "Aries",
}
_OWN_SIGN = {
    "Sun": ["Leo"], "Moon": ["Cancer"], "Mars": ["Aries", "Scorpio"],
    "Mercury": ["Gemini", "Virgo"], "Jupiter": ["Sagittarius", "Pisces"],
    "Venus": ["Taurus", "Libra"], "Saturn": ["Capricorn", "Aquarius"],
    "Rahu": [], "Ketu": [],
}
# Directional strength: planet → house of max dik-bala
_DIK_BALA_HOUSE = {
    "Sun": 10, "Mars": 10, "Jupiter": 1, "Mercury": 1,
    "Moon": 4, "Venus": 4, "Saturn": 7,
}


def improved_shadbala(planet_data):
    results = []
    for planet, data in planet_data.items():
        sign = data.get("sign", "")
        house = data.get("house", 0)
        score = 0
        notes = []

        # Sthana bala (positional strength)
        if _EXALTATION.get(planet) == sign:
            score += 60
            notes.append("exalted")
        elif _DEBILITATION.get(planet) == sign:
            score -= 30
            notes.append("debilitated")
        elif sign in _OWN_SIGN.get(planet, []):
            score += 45
            notes.append("own sign")

        # Dik bala (directional strength)
        best_house = _DIK_BALA_HOUSE.get(planet)
        if best_house is not None and house == best_house:
            score += 30
            notes.append("full dik-bala")
        elif best_house is not None and abs(house - best_house) <= 2:
            score += 15
            notes.append("partial dik-bala")

        # Kendra/trikona bonus (angular/trine houses strengthen planets)
        if house in (1, 4, 7, 10):
            score += 20
            notes.append("kendra")
        elif house in (5, 9):
            score += 15
            notes.append("trikona")

        # Dusthana penalty (6, 8, 12)
        if house in (6, 8, 12):
            score -= 20
            notes.append("dusthana")

        label = "Strong" if score >= 60 else ("Moderate" if score >= 20 else "Weak")
        tag = ", ".join(notes) if notes else "neutral"
        results.append(f"{planet}: Shadbala score = {score} [{label}] ({tag})")

    return results


# -------------------------------
# TRANSIT LOGIC (SATURN)
# -------------------------------
def saturn_transit_effect(planet_data):
    moon_sign = planet_data.get("Moon", {}).get("sign", "")
    saturn_sign = planet_data.get("Saturn", {}).get("sign", "")

    if not moon_sign or not saturn_sign:
        return "Transit data unavailable (Moon or Saturn sign missing)."

    moon_idx = _sign_index(moon_sign)
    saturn_idx = _sign_index(saturn_sign)

    if moon_idx == -1 or saturn_idx == -1:
        return "Transit data unavailable (unrecognized sign)."

    # Difference in signs (forward count from Moon)
    diff = (saturn_idx - moon_idx) % 12 + 1  # 1-based house from Moon

    output = f"Saturn transiting {saturn_sign}, natal Moon in {moon_sign} (Saturn in {diff}th from Moon).\n"

    sade_sati_houses = {12, 1, 2}
    ashtama_houses = {8}
    if diff in sade_sati_houses:
        output += "⚠  SADE SATI active — period of challenges, introspection, and karmic lessons."
    elif diff in ashtama_houses:
        output += "⚠  ASHTAMA SHANI active — pressure on health, finances, and stability."
    elif diff in {4, 7, 10}:
        output += "⚡ Kantaka Shani (square transit) — obstacles in career and relationships possible."
    elif diff in {3, 6, 11}:
        output += "✅ Favourable Saturn transit — discipline and hard work bring rewards."
    else:
        output += "Saturn transit effect is neutral for the current period."

    return output


# -------------------------------
# DOSHA DETECTION
# -------------------------------
def detect_doshas(planet_data):
    output = "\n=== DOSHA ANALYSIS ===\n"
    output += "This section identifies key karmic patterns in your chart that may influence important life areas.\n\n"

    mars_house = planet_data.get("Mars", {}).get("house")
    if mars_house in [1, 4, 7, 8, 12]:
        output += "Your chart carries Manglik Dosha — this placement of Mars may influence your marriage dynamics and should be considered when making partnership decisions.\n"

    rahu_house = planet_data.get("Rahu", {}).get("house")
    ketu_house = planet_data.get("Ketu", {}).get("house")
    if rahu_house == 1 and ketu_house == 7:
        output += "A Kaal Sarp pattern is detected in your chart — your life journey may feature dramatic highs and lows, but these experiences ultimately shape profound inner growth.\n"

    return output


# -------------------------------
# FINAL SYNTHESIS / PREDICTION
# -------------------------------
def generate_final_prediction(planet_data, dasha_list, transit_text):
    output = "\n=== FINAL PREDICTION ===\n"
    output += "Based on the full analysis of your chart, here is a synthesised view of what lies ahead.\n\n"

    # Career logic based on Saturn placement
    if "Saturn" in planet_data:
        house = planet_data["Saturn"].get("house")
        if house in [10, 11]:
            output += "Your chart indicates that your career is likely to grow steadily over time, especially through discipline and persistence.\n"
        elif house in [6, 8, 12]:
            output += "Your chart suggests possible career delays and struggles, but long-term stability is very much attainable with patience and consistent effort.\n"

    # Moon sign emotional nature
    moon_sign = planet_data.get("Moon", {}).get("sign")
    if moon_sign:
        output += f"Your emotional tendencies are influenced by {moon_sign}, shaping the way you process and make important decisions in life.\n"

    # Dasha influence
    if len(dasha_list) > 0:
        current = dasha_list[0].get("planet", "")
        output += f"The current Mahadasha of {current} is set to dominate key life events and themes during this period.\n"

    # Transit influence
    output += transit_text + "\n"

    return output


# -------------------------------
# COMBINED SYNTHESIS ANALYSIS
# -------------------------------
def combined_analysis(planet_data):
    output = "\n=== COMBINED ANALYSIS ===\n"
    output += "This section looks at planetary combinations in your chart that produce distinctive life themes and outcomes.\n\n"

    if (planet_data.get("Saturn", {}).get("house") == 12
            and planet_data.get("Jupiter", {}).get("house") == 12):
        output += "With both Saturn and Jupiter placed in the 12th house, your chart carries strong spiritual potential along with meaningful connections to foreign lands or institutions.\n"

    if planet_data.get("Sun", {}).get("house") == 3:
        output += "Your Sun in the 3rd house bestows courage and a natural ability to lead through communication, writing, and self-driven effort.\n"

    return output


# -------------------------------
# ANTARDASHA ANALYSIS
# -------------------------------
def analyze_antardasha(dasha_list):
    output = "\n=== ANTARDASHA ANALYSIS ===\n"
    output += "This section analyzes sub-period influences within the current Mahadasha with detailed period-specific interpretations.\n\n"

    if len(dasha_list) < 2:
        return output + "Insufficient data for Antardasha analysis.\n"

    main = dasha_list[0].get("planet", "")
    sub = dasha_list[1].get("planet", "")

    output += f"You are currently running the {main}-{sub} Dasha-Antardasha period.\n\n"

    main_meanings = {
        "Sun":     "authority, government, father, leadership, and ego expression. Career advancement and clarity of purpose.",
        "Moon":    "emotions, mind, mother, travel, the public, and social dealings. Heightened sensitivity and intuitive perception.",
        "Mars":    "energy, action, courage, property, siblings, and competition. A time for bold, decisive moves.",
        "Mercury": "intellect, communication, business, education, and analytical thinking. Learning and trade flourish.",
        "Jupiter": "wisdom, expansion, spirituality, children, good fortune, and higher knowledge. Major life blessings often occur.",
        "Venus":   "relationships, beauty, luxury, arts, pleasure, and material comfort. Love life and creativity are highlighted.",
        "Saturn":  "discipline, karmic lessons, delays, hard work, and long-term building. Patience and perseverance are essential.",
        "Rahu":    "ambition, unconventional paths, foreign connections, illusion, and obsession. Dramatic and unexpected life shifts possible.",
        "Ketu":    "detachment, spirituality, past-life resolution, psychic sensitivity, and inner search. Worldly disengagement is common.",
    }

    sub_meanings = {
        "Sun":     "focus on identity, authority, and leadership. Health and career clarity are emphasized.",
        "Moon":    "emotional events, mother, home, and mental states come to the fore. Travel or relocation is possible.",
        "Mars":    "action, conflict, property matters, and sibling relationships. Energy and physical drive are heightened.",
        "Mercury": "communication, business deals, education, and analytical decisions are favored.",
        "Jupiter": "expansion, wisdom, blessings, and opportunity emerge. Auspicious events and growth are likely.",
        "Venus":   "relationships, social life, creative projects, and financial gains are activated.",
        "Saturn":  "discipline, hard work, health caution, and karmic accountability are required.",
        "Rahu":    "sudden events, foreign connections, ambition surges, and unpredictability are heightened.",
        "Ketu":    "spiritual seeking, sense of loss, detachment, and deep introspection mark this sub-period.",
    }

    if main in main_meanings:
        output += f"Main Period Themes — {main} Mahadasha:\n{main_meanings[main]}\n\n"

    if main == "Saturn":
        output += "The Saturn Mahadasha is a long, often challenging but profoundly productive period. It emphasizes discipline, responsibility, and karmic accountability. Results are slow but lasting and serve as the foundation for future achievements.\n"
    elif main == "Jupiter":
        output += "The Jupiter Mahadasha is typically a period of significant personal growth, expansion, and spiritual enrichment. Blessings, children, higher education, and fortunate connections often mark this period.\n"
    elif main == "Rahu":
        output += "The Rahu Mahadasha brings sudden changes, foreign influences, and unconventional opportunities. Material ambition is high but maintaining groundedness is essential to avoid illusion and scattered focus.\n"
    elif main == "Ketu":
        output += "The Ketu Mahadasha brings a period of spiritual deepening and worldly detachment. Past-life patterns surface for resolution. This is a powerful time for inner growth, though external circumstances may feel uncertain.\n"
    elif main == "Venus":
        output += "The Venus Mahadasha is often a period of joy, relationships, material gains, and creative expression. Marriage, artistic pursuits, and financial growth are commonly experienced.\n"
    elif main == "Mars":
        output += "The Mars Mahadasha brings dynamic energy, action, and ambition. Property, siblings, and competitive endeavors are activated. Decisions made now have lasting consequences.\n"
    elif main == "Moon":
        output += "The Moon Mahadasha heightens emotional sensitivity, public dealings, and connection to mother and home. Travel, career in public life, and social growth are common themes.\n"
    elif main == "Mercury":
        output += "The Mercury Mahadasha favors intellectual pursuits, communication, business, and education. A time for learning, networking, and analytical career growth.\n"
    elif main == "Sun":
        output += "The Sun Mahadasha brings focus on identity, authority, and professional recognition. Leadership opportunities, government connections, and clarity of life purpose emerge.\n"

    if sub in sub_meanings:
        output += f"\nSub-Period Themes — {sub} Antardasha:\n{sub_meanings[sub]}\n"

    if sub == "Mercury":
        output += "The Mercury Antardasha within this main period brings focus on communication, learning, business negotiations, and analytical decision-making. Intellectual clarity improves.\n"
    elif sub == "Saturn":
        output += "The Saturn Antardasha within this main period demands discipline, health awareness, and karmic accountability. Delays may occur but consistent effort is rewarded.\n"
    elif sub == "Jupiter":
        output += "The Jupiter Antardasha within this main period brings blessings, expansion, and fortunate opportunities. New doors in education, spirituality, or career may open.\n"
    elif sub == "Venus":
        output += "The Venus Antardasha brings comfort, relationship harmony, and creative or financial opportunities within this main period.\n"
    elif sub == "Rahu":
        output += "The Rahu Antardasha introduces sudden changes, ambition, and unpredictable events. Careful decision-making and staying grounded are important.\n"

    output += f"\n=== {main}-{sub} Compatibility Assessment ===\n"
    benefics = {"Jupiter", "Venus", "Mercury", "Moon"}
    malefics = {"Saturn", "Mars", "Rahu", "Ketu", "Sun"}

    if main in benefics and sub in benefics:
        output += "Both main and sub periods are natural benefics — this is generally a favorable combination bringing growth, harmony, creativity, and positive opportunities across most life areas.\n"
    elif main in malefics and sub in malefics:
        output += "Both main and sub periods are natural malefics — this period requires extra patience, resilience, and conscious effort. Karmic clearing is active and hard work pays dividends in the long run.\n"
    else:
        output += "A blend of benefic and malefic energies characterizes this period — some life areas will flourish while others require careful navigation and patience. Balance and awareness are the key.\n"

    return output


# -------------------------------
# CLASSICAL YOGA DETECTION
# -------------------------------
def detect_real_yogas(planet_data):
    output = "\n=== CLASSICAL YOGA ANALYSIS ===\n"
    output += "This section identifies important yogas formed in your chart.\n\n"

    yoga_found = False

    # Raj Yoga: Kendra + Trikona lord interaction (simplified)
    if (planet_data.get("Jupiter", {}).get("house") in [1, 5, 9]
            and planet_data.get("Saturn", {}).get("house") in [1, 4, 7, 10]):
        output += "⚡ Raj Yoga indicates potential rise in status, authority, and recognition.\n"
        yoga_found = True

    # Dhan Yoga
    if (planet_data.get("Venus", {}).get("house") in [2, 11]
            or planet_data.get("Jupiter", {}).get("house") in [2, 11]):
        output += "💰 Dhan Yoga indicates financial growth and wealth accumulation potential.\n"
        yoga_found = True

    # Vipreet Raj Yoga
    if planet_data.get("Saturn", {}).get("house") in [6, 8, 12]:
        output += "🔥 Vipreet Raj Yoga indicates success through adversity and unexpected rise.\n"
        yoga_found = True

    if not yoga_found:
        output += "No major classical yogas detected, but planetary combinations still influence destiny.\n"

    return output


# -------------------------------
# CAREER ANALYSIS
# -------------------------------
def analyze_career(planet_data):
    output = "\n=== CAREER DEEP ANALYSIS ===\n"
    output += "This section evaluates professional direction, growth, and challenges using multi-layer planetary reasoning.\n\n"

    saturn = planet_data.get("Saturn", {})
    sun = planet_data.get("Sun", {})
    mars = planet_data.get("Mars", {})
    mercury = planet_data.get("Mercury", {})
    jupiter = planet_data.get("Jupiter", {})
    rahu = planet_data.get("Rahu", {})

    saturn_house = saturn.get("house")
    sun_house = sun.get("house")
    mars_house = mars.get("house")
    mercury_house = mercury.get("house")
    jupiter_house = jupiter.get("house")
    rahu_house = rahu.get("house")

    output += "=== 10th House Lord (Primary Career Indicator) ===\n"
    lagna = kundali.get("ascendant", "")
    if lagna in SIGN_ORDER:
        lagna_idx = SIGN_ORDER.index(lagna)
        tenth_sign = SIGN_ORDER[(lagna_idx + 9) % 12]
        tenth_lord = SIGN_LORDS.get(tenth_sign, "Unknown")
        tenth_lord_data = planet_data.get(tenth_lord, {})
        output += f"10th house falls in {tenth_sign}, ruled by {tenth_lord}.\n"
        output += f"{tenth_lord} is placed in House {tenth_lord_data.get('house', '?')} ({tenth_lord_data.get('sign', '?')}).\n"
        tlh = tenth_lord_data.get("house")
        if tlh in [10, 11]:
            output += f"Strong placement — {tenth_lord} in House {tlh} supports professional success, recognition, and career gains.\n"
        elif tlh in [1, 5, 9]:
            output += f"{tenth_lord} in House {tlh} (trikona) — career connected to personal dharma, intelligence, and fortunate opportunities.\n"
        elif tlh in [6, 8, 12]:
            output += f"{tenth_lord} in House {tlh} (dusthana) — career may involve transformation, hidden work, service, or foreign sectors. Challenges become stepping stones.\n"
        if tlh == 12:
            output += "Career linked to foreign lands, research, spirituality, hidden sectors, or institutional environments.\n"

    output += "\n=== Saturn — Karma Karaka (Lord of Career Karma) ===\n"
    output += "Saturn determines long-term career stability and the karmic lessons embedded in professional life. Whatever Saturn touches matures slowly but solidly.\n"
    if saturn_house in [10, 11]:
        output += "Saturn in 10th or 11th: Strong long-term career growth with discipline and persistence. Authority and status come with time and are built to last.\n"
    elif saturn_house in [6, 8, 12]:
        output += "Saturn in 6/8/12: Career may involve delays, struggles, or unconventional paths — but Vipreet Raj Yoga potential creates unexpected rise through adversity.\n"
    elif saturn_house == 3:
        output += "Saturn in 3rd house: Career success through consistent effort, writing, communication, and self-driven entrepreneurial initiative.\n"
    elif saturn_house == 7:
        output += "Saturn in 7th: Partnerships and business collaborations play a major role in career. Disciplined business approach is recommended.\n"
    elif saturn_house in [1, 4]:
        output += "Saturn in 1st or 4th: Career success comes through perseverance and real estate, construction, or foundational industries may be favored.\n"

    output += "\n=== Sun — Authority and Status ===\n"
    if sun_house == 10:
        output += "Sun in 10th house: Natural authority, leadership ability, and strong career in government, management, or any field requiring status and public recognition.\n"
    elif sun_house == 3:
        output += "Sun in 3rd house: Career driven by courage, self-expression, and communication. Media, writing, entrepreneurship, or broadcasting is indicated.\n"
    elif sun_house == 1:
        output += "Sun in 1st house: Strong identity tied to career and public image. Leadership roles and roles with clear authority are most fulfilling.\n"
    elif sun_house in [6, 8, 12]:
        output += "Sun in dusthana (6/8/12): Career may face ego-related challenges and authority conflicts, but provides deep service orientation and hidden strength that emerges over time.\n"
    elif sun_house == 9:
        output += "Sun in 9th: Career connected to teaching, law, philosophy, or fields requiring wisdom and higher knowledge.\n"

    output += "\n=== Mars — Action, Drive, and Ambition ===\n"
    if mars_house == 10:
        output += "Mars in 10th: Exceptional drive and ambition. Success in competitive, technical, surgical, military, athletic, or entrepreneurial careers.\n"
    elif mars_house == 3:
        output += "Mars in 3rd: Career success through bold action, initiative, competitive communication, and entrepreneurial drive.\n"
    elif mars_house == 6:
        output += "Mars in 6th: Strong ability to overcome competition. Legal, medical, military, or service-oriented careers are indicated.\n"
    elif mars_house == 1:
        output += "Mars in 1st: Highly energetic and direct career approach. Physical industries, sports, real estate, or leadership roles suit this placement.\n"

    output += "\n=== Mercury — Intellect and Communication ===\n"
    if mercury_house == 10:
        output += "Mercury in 10th: Career in communication, media, teaching, analytics, business, technology, or writing is strongly supported.\n"
    elif mercury_house == 3:
        output += "Mercury in 3rd (its natural house): Exceptional analytical, writing, and communication skills that significantly accelerate career growth.\n"
    elif mercury_house == 1:
        output += "Mercury in 1st: Intellectual and communicative personality that brings career advantages in any knowledge-based field.\n"

    output += "\n=== Jupiter — Wisdom and Career Expansion ===\n"
    if jupiter_house == 10:
        output += "Jupiter in 10th: Career as teacher, advisor, judge, spiritual leader, or in any field requiring wisdom, ethics, and authority. Public respect is indicated.\n"
    elif jupiter_house == 12:
        output += "Jupiter in 12th: Career linked to foreign lands, research, spirituality, counseling, healing, or behind-the-scenes advisory roles.\n"
    elif jupiter_house == 1:
        output += "Jupiter in 1st: Professional success through personal wisdom, generosity, and philosophical leadership.\n"
    elif jupiter_house in [5, 9]:
        output += f"Jupiter in {jupiter_house}th (trikona): Career growth through education, mentorship, creative intelligence, or spiritual guidance.\n"

    output += "\n=== Rahu — Unconventional Career Ambition ===\n"
    if rahu_house == 10:
        output += "Rahu in 10th: Intense career ambition and unconventional or technological career paths. Strong drive for public recognition — career may be connected to media, technology, or foreign organisations.\n"
    elif rahu_house == 6:
        output += "Rahu in 6th: Ability to outmaneuver competition through unconventional strategies. Success in health, service, legal, or analytical fields.\n"
    elif rahu_house == 3:
        output += "Rahu in 3rd: Career success through bold, unconventional communication. Media, technology, writing, or entrepreneurship is strongly indicated.\n"

    return output


# -------------------------------
# MARRIAGE & RELATIONSHIP ANALYSIS
# -------------------------------
def analyze_marriage(planet_data):
    output = "\n=== MARRIAGE & RELATIONSHIP DEEP ANALYSIS ===\n"
    output += "This section evaluates partnership dynamics, marital prospects, and relationship karma through multi-layer planetary reasoning.\n\n"

    venus = planet_data.get("Venus", {})
    mars = planet_data.get("Mars", {})
    saturn = planet_data.get("Saturn", {})
    jupiter = planet_data.get("Jupiter", {})
    ketu = planet_data.get("Ketu", {})
    rahu = planet_data.get("Rahu", {})

    venus_house = venus.get("house")
    mars_house = mars.get("house")
    saturn_house = saturn.get("house")
    jupiter_house = jupiter.get("house")
    ketu_house = ketu.get("house")
    rahu_house = rahu.get("house")

    output += "=== 7th House Lord (Primary Marriage Indicator) ===\n"
    lagna = kundali.get("ascendant", "")
    if lagna in SIGN_ORDER:
        lagna_idx = SIGN_ORDER.index(lagna)
        seventh_sign = SIGN_ORDER[(lagna_idx + 6) % 12]
        seventh_lord = SIGN_LORDS.get(seventh_sign, "Unknown")
        seventh_lord_data = planet_data.get(seventh_lord, {})
        output += f"7th house falls in {seventh_sign}, ruled by {seventh_lord}.\n"
        output += f"{seventh_lord} is placed in House {seventh_lord_data.get('house', '?')} ({seventh_lord_data.get('sign', '?')}).\n"
        slh = seventh_lord_data.get("house")
        if slh in [1, 5, 7]:
            output += f"Favorable placement — {seventh_lord} in House {slh} supports relationship harmony and partnership success.\n"
        elif slh in [6, 8, 12]:
            output += f"{seventh_lord} in House {slh} (dusthana) — karmic lessons in partnerships; relationships require patience and significant personal growth.\n"

    output += "\n=== Venus — Love, Relationships, and Harmony ===\n"
    if venus_house in [7, 1]:
        output += "Venus in 7th or 1st: Strong indications for attraction, romance, and meaningful partnerships. This placement supports a loving and harmonious marital life.\n"
    elif venus_house == 2:
        output += "Venus in 2nd house: Wealth through relationships; spouse may be from a family with good values and resources. Strong appreciation for family life.\n"
    elif venus_house in [6, 8, 12]:
        output += "Venus in dusthana: Relationships may require sacrifice, healing, or transformation. Deep soul connections are possible despite surface challenges.\n"
    elif venus_house == 5:
        output += "Venus in 5th: Romantic, creative, and joyful approach to love. Children and creative pursuits may be closely intertwined with the relationship.\n"
    elif venus_house == 11:
        output += "Venus in 11th: Social connections lead to romantic opportunities. Spouse may be met through friends, networks, or social events.\n"
    elif venus_house == 9:
        output += "Venus in 9th: Partner may be foreign, philosophical, or spiritually oriented. Relationship brings higher learning and fortunate travel.\n"

    output += "\n=== Jupiter — Spouse Indicator and Marriage Blessings ===\n"
    if jupiter_house == 12:
        output += "Jupiter in 12th: Spouse may be from a different culture, foreign land, or spiritual background. A deeply spiritual and transcendent connection in marriage.\n"
    elif jupiter_house in [1, 5, 7, 9]:
        output += f"Jupiter in House {jupiter_house}: Strong blessings on partnerships. The spouse is likely wise, educated, spiritual, and a positive influence on life.\n"
    elif jupiter_house in [6, 8]:
        output += "Jupiter in 6th/8th: Marriage may arrive through challenging circumstances but brings profound transformation, wisdom, and growth.\n"
    elif jupiter_house in [2, 11]:
        output += "Jupiter in 2nd or 11th: Marriage brings material gains and social expansion. Spouse contributes positively to financial and social life.\n"

    output += "\n=== Mars — Passion, Drive, and Conflict in Relationships ===\n"
    if mars_house in [7, 8]:
        output += "Mars in 7th or 8th: Intensity and passion in relationships — this is classic Manglik placement. Careful handling of conflicts, power dynamics, and emotional intensity is advised for long-term harmony.\n"
    elif mars_house == 1:
        output += "Mars in 1st house: Strong and assertive personality in relationships. Directness is both an asset and a challenge — awareness of the partner's emotional needs is important.\n"
    elif mars_house == 4:
        output += "Mars in 4th: Home environment may experience tension but property gains are possible. Emotional security in marriage requires conscious communication.\n"
    elif mars_house == 5:
        output += "Mars in 5th: Passionate and energetic approach to romance. Children may be active and strong-willed.\n"

    output += "\n=== Saturn — Karmic Bonds and Long-Term Stability ===\n"
    if saturn_house == 7:
        output += "Saturn in 7th: Marriage may be delayed but ensures a mature, stable, and long-lasting union built on responsibility, mutual respect, and shared purpose.\n"
    elif saturn_house == 8:
        output += "Saturn in 8th: Deep karmic bonds in marriage. Profound transformation through partnerships is likely. May indicate a significant age gap or serious, sober quality in the relationship.\n"
    elif saturn_house == 12:
        output += "Saturn in 12th: Spiritual and karmic bonds in relationships. Foreign spouse or spiritual partnership possible. Relationship may involve sacrifice and deep inner growth.\n"

    output += "\n=== Rahu & Ketu — Karmic Relationship Patterns ===\n"
    if ketu_house == 7:
        output += "Ketu in 7th: Deep past-life connection with the partner. The relationship feels simultaneously familiar and complex. Spiritual growth through marriage is strongly indicated; detachment tendencies must be consciously managed.\n"
    if rahu_house == 7:
        output += "Rahu in 7th: Intense attraction to unconventional, foreign, or dramatically different partners. The relationship is transformative, all-consuming, and often carries lessons about obsession and healthy boundaries.\n"
    if ketu_house in [1, 5]:
        output += f"Ketu in House {ketu_house}: Past-life traits strong here — may need conscious effort to remain engaged in relationships rather than retreating inward.\n"

    return output


# -------------------------------
# CLASSICAL REMEDIES
# -------------------------------
def suggest_remedies(planet_data):
    output = "\n=== CLASSICAL REMEDIES ===\n"
    output += "These remedies are based on traditional Vedic practices.\n\n"

    if planet_data.get("Saturn"):
        output += "- Chant 'Om Sham Shanicharaya Namah' on Saturdays.\n"
        output += "- Donate black sesame seeds or mustard oil.\n"

    if planet_data.get("Mars"):
        output += "- Chant 'Om Angarakaya Namah'.\n"
        output += "- Visit Hanuman temple on Tuesdays.\n"

    if planet_data.get("Rahu"):
        output += "- Chant 'Om Rahave Namah'.\n"

    if planet_data.get("Ketu"):
        output += "- Chant 'Om Ketave Namah'.\n"

    return output


# -------------------------------
# SIGN CONSTANTS (shared by lord analysis functions)
# -------------------------------
SIGN_ORDER = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"
]

SIGN_LORDS = {
    "Aries": "Mars", "Taurus": "Venus", "Gemini": "Mercury", "Cancer": "Moon",
    "Leo": "Sun", "Virgo": "Mercury", "Libra": "Venus", "Scorpio": "Mars",
    "Sagittarius": "Jupiter", "Capricorn": "Saturn", "Aquarius": "Saturn",
    "Pisces": "Jupiter"
}


# -------------------------------
# ANTARDASHA TIMELINE
# -------------------------------
def antardasha_timeline(dasha_list):
    output = "\n=== ANTARDASHA TIMELINE ===\n"
    output += "Detailed breakdown of sub-periods within the current Mahadasha.\n\n"

    for d in dasha_list[:5]:
        planet = d.get("planet", "")
        start = d.get("start", "?")
        end = d.get("end", "?")

        output += f"{planet} Antardasha: {start} → {end}\n"

    return output


# -------------------------------
# LAGNA LORD ANALYSIS
# -------------------------------
def analyze_lagna_lord(kundali_data, planet_data):
    output = "\n=== LAGNA LORD ANALYSIS ===\n\n"

    lagna = kundali_data.get("ascendant", "")
    lord = SIGN_LORDS.get(lagna)

    if not lord:
        return output + "Lagna lord not found.\n"

    data = planet_data.get(lord, {})

    output += f"Your ascendant is {lagna}, ruled by {lord}.\n"
    output += f"{lord} is placed in {data.get('sign')} (House {data.get('house')}).\n"

    if data.get("house") in [1, 5, 9]:
        output += "This strengthens personality, confidence, and life direction.\n"
    elif data.get("house") in [6, 8, 12]:
        output += "This indicates challenges and karmic lessons shaping your life path.\n"

    return output


# -------------------------------
# 10TH LORD (CAREER DEEP ANALYSIS)
# -------------------------------
def analyze_10th_lord(planet_data, kundali_data):
    output = "\n=== 10TH LORD (CAREER DEEP ANALYSIS) ===\n\n"

    lagna = kundali_data.get("ascendant", "")
    if lagna not in SIGN_ORDER:
        return output + "Ascendant not found for 10th lord analysis.\n"

    lagna_index = SIGN_ORDER.index(lagna)
    tenth_sign = SIGN_ORDER[(lagna_index + 9) % 12]
    lord = SIGN_LORDS.get(tenth_sign)

    data = planet_data.get(lord, {})

    output += f"Your 10th house falls in {tenth_sign}, ruled by {lord}.\n"
    output += f"{lord} is placed in {data.get('sign')} (House {data.get('house')}).\n"

    if data.get("house") in [10, 11]:
        output += "Indicates strong professional success and recognition.\n"
    elif data.get("house") in [6, 8, 12]:
        output += "Career path may involve obstacles, transformation, or unconventional routes.\n"

    return output


# -------------------------------
# 7TH LORD (MARRIAGE DEEP ANALYSIS)
# -------------------------------
def analyze_7th_lord(planet_data, kundali_data):
    output = "\n=== 7TH LORD (MARRIAGE DEEP ANALYSIS) ===\n\n"

    lagna = kundali_data.get("ascendant", "")
    if lagna not in SIGN_ORDER:
        return output + "Ascendant not found for 7th lord analysis.\n"

    lagna_index = SIGN_ORDER.index(lagna)
    seventh_sign = SIGN_ORDER[(lagna_index + 6) % 12]
    lord = SIGN_LORDS.get(seventh_sign)

    data = planet_data.get(lord, {})

    output += f"Your 7th house falls in {seventh_sign}, ruled by {lord}.\n"
    output += f"{lord} is placed in {data.get('sign')} (House {data.get('house')}).\n"

    if data.get("house") in [1, 5, 7]:
        output += "Favorable for relationships and partnership harmony.\n"
    elif data.get("house") in [6, 8, 12]:
        output += "May indicate delays, challenges, or karmic patterns in relationships.\n"

    return output


# ============================================================
# ELITE SYNTHESIS ENGINE — PLANET DEEP ANALYSIS
# ============================================================

HOUSE_MEANINGS = {
    1:  "self, personality, and physical body",
    2:  "wealth, speech, and family values",
    3:  "effort, courage, siblings, and communication",
    4:  "home, mother, emotional security, and comfort",
    5:  "intelligence, creativity, children, and past-life merit",
    6:  "enemies, disease, debts, and competition",
    7:  "marriage, business partnerships, and open enemies",
    8:  "transformation, secrets, occult, and sudden events",
    9:  "luck, dharma, father, and higher knowledge",
    10: "career, karma, status, and public recognition",
    11: "gains, networks, aspirations, and elder siblings",
    12: "loss, foreign lands, moksha, and spirituality",
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

PLANET_DEEP_LOGIC = {
    "Saturn": {
        "general": (
            "Saturn delays but never denies. Results come through persistence, "
            "discipline, and patience. Saturn-ruled periods test character but "
            "ultimately reward integrity and long-term effort."
        ),
        6:  "Saturn in the 6th house gives formidable ability to overcome enemies, disease, and competition. Work in service industries, legal fields, or healthcare is often indicated.",
        8:  "Saturn in the 8th house brings deep transformation and research orientation. Insurance, occult, inheritance, or investigative careers may feature. Longevity is often indicated.",
        10: "Saturn in the 10th house is a powerful career placement. Authority, government, administration, and long-term professional reputation are strongly supported.",
        12: "Saturn in the 12th house carries strong karmic isolation energy. Foreign settlement, deep spiritual evolution, or work in hidden institutions (hospitals, ashrams, research) is indicated.",
    },
    "Jupiter": {
        "general": (
            "Jupiter expands whatever it touches and bestows wisdom, optimism, and higher "
            "knowledge. Jupiter periods often bring blessings, growth, children, and connection "
            "to spiritual truths."
        ),
        1:  "Jupiter in the 1st house blesses the native with wisdom, optimism, and a generous, philosophical personality. Natural teachers and advisors emerge from this placement.",
        5:  "Jupiter in the 5th house supports exceptional intelligence, higher education, and blessings through children and creative endeavours.",
        9:  "Jupiter in the 9th house — its natural house of higher knowledge — gives deep dharmic orientation, blessings from teachers, and fortunate grace in life.",
        12: "Jupiter in the 12th house indicates that knowledge comes through isolation, foreign lands, or deep inner exploration. Strong moksha and spiritual liberation potential.",
    },
    "Rahu": {
        "general": (
            "Rahu amplifies desire, ambition, and unconventional thinking. It breaks social "
            "norms and drives the native toward obsessive pursuit of chosen goals. Rahu areas "
            "bring both rapid rise and potential illusion."
        ),
        1:  "Rahu in the 1st house creates a magnetic, unconventional personality. The native often follows an unusual life path and may have a powerful, transformative public presence.",
        7:  "Rahu in the 7th can bring attraction to foreign, unconventional, or dramatically different partners. Relationships are transformative and all-consuming.",
        10: "Rahu in the 10th house creates intense career ambition and often leads to prominence, notoriety, or success through technology and unconventional paths.",
    },
    "Ketu": {
        "general": (
            "Ketu detaches and spiritualizes. It represents past-life mastery and present-life "
            "disinterest in those areas. Ketu placements show where the native has innate skill "
            "but little worldly attachment, making liberation possible."
        ),
        7:  "Ketu in the 7th house indicates karmic relationships with deep soul connections but potential detachment, unusual dynamics, or spiritual orientation in partnerships.",
        12: "Ketu in the 12th house — its natural domain — strongly supports moksha, spiritual liberation, psychic sensitivity, and connection to foreign or hidden realms.",
    },
    "Mars": {
        "general": (
            "Mars provides energy, courage, and drive. It governs action, competition, and "
            "physical vitality. Mars periods are dynamic and conflict-prone but also highly "
            "productive when the native channels the energy constructively."
        ),
        1:  "Mars in the 1st house gives a courageous, direct, and energetic personality. Natural athletes, leaders, and entrepreneurs emerge from this placement.",
        4:  "Mars in the 4th house can create tension in the domestic sphere but also supports real estate gains and strong protective instincts over the home.",
        10: "Mars in the 10th house supports leadership, engineering, military, sports, surgery, or any competitive and results-driven profession.",
    },
    "Venus": {
        "general": (
            "Venus governs beauty, love, arts, and material comfort. Venus periods bring social "
            "harmony, creative expression, financial gains, and relationship opportunities. "
            "Venus strong charts indicate talent in aesthetics and a refined quality of life."
        ),
        7:  "Venus in the 7th house is a classic indicator of a beautiful, harmonious marriage and strong partnership energy. Relationships are a source of joy.",
        2:  "Venus in the 2nd house supports wealth accumulation through beauty, arts, luxury goods, or finance. The voice and speech are often pleasing.",
        11: "Venus in the 11th house brings gains through social networks, arts, and creative collaborations. Social life is rich and rewarding.",
    },
    "Mercury": {
        "general": (
            "Mercury rules intellect, communication, commerce, and analytical ability. "
            "Mercury-strong charts often indicate writers, traders, analysts, teachers, or "
            "speakers. Quick thinking and adaptability are hallmarks of strong Mercury."
        ),
        1:  "Mercury in the 1st house gives a quick, analytical mind and strong communication skills. The personality is witty, curious, and intellectually driven.",
        3:  "Mercury in the 3rd house — its natural house of communication — strongly supports writing, speaking, trading, and intellectual pursuits.",
        10: "Mercury in the 10th house supports careers in communication, media, teaching, business, or technology.",
    },
    "Moon": {
        "general": (
            "The Moon governs the mind, emotions, intuition, and the relationship with the "
            "mother. A strong Moon supports emotional stability, empathy, and social popularity. "
            "The Moon sign is as important as the rising sign in Vedic astrology."
        ),
        1:  "Moon in the 1st house makes the personality highly emotionally expressive, empathetic, and sensitive to environmental influences.",
        4:  "Moon in the 4th house — its natural domain — gives strong emotional security, love of home, a nourishing nature, and a close relationship with the mother.",
        10: "Moon in the 10th house indicates a career in public life, caregiving, hospitality, or any work that involves direct engagement with the public.",
    },
    "Sun": {
        "general": (
            "The Sun governs authority, ego, the soul, and the relationship with the father. "
            "A strong Sun supports leadership, clarity of purpose, and public recognition. "
            "The Sun represents the core identity and life purpose."
        ),
        1:  "Sun in the 1st house gives a strong, confident, and commanding personality. Leadership and authority come naturally.",
        10: "Sun in the 10th house — its natural house of authority — strongly supports leadership, government, and professional recognition throughout life.",
        3:  "Sun in the 3rd house gives exceptional courage, self-expression, and the ability to lead through communication, writing, and entrepreneurial initiative.",
    },
}


def synthesize_planet(planet, data, all_planets):
    """Return a deeply reasoned analysis string for one planet."""
    sign = data.get("sign", "Unknown")
    house = data.get("house", 0)
    nakshatra = data.get("nakshatra", "Unknown")

    text = f"\n--- {planet} in {sign} (House {house}, Nakshatra: {nakshatra}) ---\n"
    text += f"{planet} is placed in {sign} in the {house}th house of your chart. "
    text += f"This house governs {HOUSE_MEANINGS.get(house, 'various life areas')}. "
    text += f"{planet} represents {PLANET_NATURE.get(planet, 'cosmic influence')}.\n\n"

    logic = PLANET_DEEP_LOGIC.get(planet, {})
    if "general" in logic:
        text += logic["general"] + "\n"
    if house in logic:
        text += logic[house] + "\n"

    # Dignity commentary
    exaltation = {
        "Sun": "Aries", "Moon": "Taurus", "Mars": "Capricorn",
        "Mercury": "Virgo", "Jupiter": "Cancer", "Venus": "Pisces", "Saturn": "Libra",
    }
    debilitation = {
        "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
        "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries",
    }
    own_signs = {
        "Sun": ["Leo"], "Moon": ["Cancer"], "Mars": ["Aries", "Scorpio"],
        "Mercury": ["Gemini", "Virgo"], "Jupiter": ["Sagittarius", "Pisces"],
        "Venus": ["Taurus", "Libra"], "Saturn": ["Capricorn", "Aquarius"],
        "Rahu": [], "Ketu": [],
    }

    if exaltation.get(planet) == sign:
        text += f"{planet} is EXALTED in {sign} — this is a position of exceptional strength. Results of this planet are amplified positively throughout life.\n"
    elif debilitation.get(planet) == sign:
        text += f"{planet} is DEBILITATED in {sign} — this placement creates challenges but also potential for Neecha Bhanga (cancellation of debilitation) which can produce remarkable results.\n"
    elif sign in own_signs.get(planet, []):
        text += f"{planet} is in its OWN SIGN {sign} — well-established, confident, and able to fully express its natural qualities.\n"

    return text


def synthesize_all_planets(planet_data):
    """Return full deep synthesis for all planets in the chart."""
    output = "\n=== PLANET-BY-PLANET DEEP SYNTHESIS ===\n"
    output += "Each planet is analyzed through its sign, house, nakshatra, and dignity to reveal the full multi-dimensional picture of your chart.\n"

    for planet, data in planet_data.items():
        output += synthesize_planet(planet, data, planet_data)

    return output


# ============================================================
# ADVANCED YOGA DETECTION (20+ YOGAS)
# ============================================================

def detect_advanced_yogas(planet_data):
    """Detect 20+ classical and advanced yogas in the chart."""
    output = "\n=== ADVANCED YOGA ANALYSIS (20+ YOGAS) ===\n"
    output += "This section identifies all major classical and advanced yogas present in your chart. Yogas are specific planetary combinations that produce distinctive life outcomes.\n\n"

    yogas_found = []

    jup_h   = planet_data.get("Jupiter", {}).get("house", 0)
    moon_h  = planet_data.get("Moon", {}).get("house", 0)
    sun_h   = planet_data.get("Sun", {}).get("house", 0)
    mer_h   = planet_data.get("Mercury", {}).get("house", 0)
    mars_h  = planet_data.get("Mars", {}).get("house", 0)
    ven_h   = planet_data.get("Venus", {}).get("house", 0)
    sat_h   = planet_data.get("Saturn", {}).get("house", 0)
    rahu_h  = planet_data.get("Rahu", {}).get("house", 0)
    ketu_h  = planet_data.get("Ketu", {}).get("house", 0)
    jup_s   = planet_data.get("Jupiter", {}).get("sign", "")
    ven_s   = planet_data.get("Venus", {}).get("sign", "")
    mars_s  = planet_data.get("Mars", {}).get("sign", "")
    mer_s   = planet_data.get("Mercury", {}).get("sign", "")
    sat_s   = planet_data.get("Saturn", {}).get("sign", "")

    # 1. Gajakesari Yoga
    if jup_h and moon_h and (jup_h - moon_h) % 3 == 0:
        yogas_found.append(
            "🐘 Gajakesari Yoga — Jupiter and Moon in mutual kendra (quadrant) relationship. "
            "Indicates intelligence, fame, and divine protection. This yoga brings recognition, "
            "wisdom, and the ability to overcome obstacles through righteousness. One of the most "
            "auspicious yogas in Vedic astrology."
        )

    # 2. Budh-Aditya Yoga
    if sun_h and mer_h and sun_h == mer_h:
        yogas_found.append(
            "☀️ Budh-Aditya Yoga — Sun and Mercury conjunct in the same house. Strong intelligence, "
            "articulate communication, and intellectual brilliance. Excellent for writers, speakers, "
            "analysts, and diplomats. The combination of soul (Sun) and intellect (Mercury) in one "
            "house creates a razor-sharp communicator."
        )

    # 3. Chandra-Mangal Yoga
    if moon_h and mars_h and moon_h == mars_h:
        yogas_found.append(
            "🌙 Chandra-Mangal Yoga — Moon and Mars conjunct. Indicates wealth generation through "
            "boldness, initiative, and enterprising action. Strong financial drive combined with "
            "emotional courage. A powerful combination for business and real estate."
        )

    # 4. Raj Yoga (Kendra-Trikona lord connection)
    if jup_h in [1, 5, 9] and sat_h in [1, 4, 7, 10]:
        yogas_found.append(
            "⚡ Raj Yoga — Kendra-Trikona lord connection (Jupiter in trikona, Saturn in kendra). "
            "Strong potential for rise in status, authority, and recognition. This yoga activates "
            "ambition, discipline, and long-term achievement. Authority and leadership are strongly indicated."
        )

    # 5. Dhan Yoga
    if ven_h in [2, 11] or jup_h in [2, 11]:
        yogas_found.append(
            "💰 Dhan Yoga — Wealth lords (Venus or Jupiter) positioned in houses of wealth (2nd) or "
            "gains (11th). Financial growth, wealth accumulation, and material prosperity are powerfully "
            "indicated. The native is likely to build significant assets over their lifetime."
        )

    # 6. Vipreet Raj Yoga
    if sat_h in [6, 8, 12]:
        yogas_found.append(
            "🔥 Vipreet Raj Yoga — Saturn in dusthana (6th, 8th, or 12th house). Success through "
            "adversity, unexpected rise, and triumph after hardship. The greater the challenge faced, "
            "the greater the eventual reward. This is the yoga of the phoenix — rising from difficulties."
        )

    # 7. Dharma-Karma Adhipati Yoga
    if jup_h == 9 and sat_h == 10:
        yogas_found.append(
            "🙏 Dharma-Karma Adhipati Yoga — Jupiter in the 9th house of dharma and Saturn in the "
            "10th house of karma. Strong destiny combined with dedicated career purpose. This native "
            "is destined to fulfill a significant social, dharmic, or leadership role in the world."
        )

    # 8. Hamsa Yoga (Pancha Mahapurusha)
    if jup_h in [1, 4, 7, 10] and jup_s in ["Sagittarius", "Pisces", "Cancer"]:
        yogas_found.append(
            "🕊️ Hamsa Yoga — Jupiter in kendra (1/4/7/10) in own or exalted sign. One of the five "
            "Pancha Mahapurusha Yogas. Indicates a wise, virtuous, and deeply respected personality. "
            "Often found in charts of teachers, scholars, judges, and spiritual leaders."
        )

    # 9. Malavya Yoga (Pancha Mahapurusha)
    if ven_h in [1, 4, 7, 10] and ven_s in ["Taurus", "Libra", "Pisces"]:
        yogas_found.append(
            "💫 Malavya Yoga — Venus in kendra in own or exalted sign. One of the Pancha Mahapurusha "
            "Yogas. Indicates beauty, artistic talent, refined tastes, luxury, and strong romantic and "
            "marital happiness. The native often possesses great charm and aesthetic sensitivity."
        )

    # 10. Ruchaka Yoga (Pancha Mahapurusha)
    if mars_h in [1, 4, 7, 10] and mars_s in ["Aries", "Scorpio", "Capricorn"]:
        yogas_found.append(
            "⚔️ Ruchaka Yoga — Mars in kendra in own or exalted sign. One of the Pancha Mahapurusha "
            "Yogas. Indicates a strong, courageous, and leadership-oriented personality. Success in "
            "physical, military, athletic, engineering, or competitive professional fields is strongly indicated."
        )

    # 11. Bhadra Yoga (Pancha Mahapurusha)
    if mer_h in [1, 4, 7, 10] and mer_s in ["Gemini", "Virgo"]:
        yogas_found.append(
            "📚 Bhadra Yoga — Mercury in kendra in own sign. One of the Pancha Mahapurusha Yogas. "
            "Exceptional intelligence, communication mastery, business acumen, and analytical genius. "
            "The native excels in any field requiring sharp thinking and expressive ability."
        )

    # 12. Sasa Yoga (Pancha Mahapurusha)
    if sat_h in [1, 4, 7, 10] and sat_s in ["Capricorn", "Aquarius", "Libra"]:
        yogas_found.append(
            "🪐 Sasa Yoga — Saturn in kendra in own or exalted sign. One of the Pancha Mahapurusha "
            "Yogas. Discipline, authority, and long-term achievement in governance, administration, law, "
            "or service fields. The native builds a lasting legacy through sustained effort."
        )

    # 13. Neecha Bhanga Raja Yoga
    debil_map = {
        "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
        "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries",
    }
    for pname, debil_sign in debil_map.items():
        if planet_data.get(pname, {}).get("sign") == debil_sign:
            yogas_found.append(
                f"♻️ Neecha Bhanga Raja Yoga potential — {pname} is debilitated in {debil_sign}. "
                f"If the debilitation is cancelled by supporting factors (lord of debilitation sign "
                f"in kendra, or exaltation lord in kendra), this produces extraordinary strength and "
                f"unexpected rise in life, especially during the {pname} Mahadasha."
            )

    # 14. Kemadruma Yoga
    adjacent = {(moon_h - 2) % 12 + 1, moon_h % 12 + 1}
    other_houses = {v.get("house") for k, v in planet_data.items() if k not in ["Moon", "Rahu", "Ketu"] and v.get("house")}
    if moon_h and not adjacent.intersection(other_houses):
        yogas_found.append(
            "⚠️ Kemadruma Yoga — Moon is isolated with no planets in the 2nd or 12th house from it. "
            "This can indicate emotional isolation, mental restlessness, or lack of consistent support. "
            "It is partially or fully cancelled if Moon is in a kendra or aspected by benefics — check "
            "your aspects section for cancellation."
        )

    # 15. Vasumati Yoga
    benefic_list = ["Jupiter", "Venus", "Mercury", "Moon"]
    upachaya_houses = [3, 6, 10, 11]
    bens_in_upachaya = [p for p in benefic_list if planet_data.get(p, {}).get("house") in upachaya_houses]
    if len(bens_in_upachaya) >= 3:
        yogas_found.append(
            f"🌸 Vasumati Yoga — Multiple benefics ({', '.join(bens_in_upachaya)}) in upachaya "
            f"(growth) houses (3/6/10/11). Indicates growing wealth, social influence, and material "
            f"success that compounds and increases with age and effort."
        )

    # 16. Amala Yoga
    for p in ["Jupiter", "Venus", "Mercury"]:
        if planet_data.get(p, {}).get("house") == 10:
            yogas_found.append(
                f"✨ Amala Yoga — {p} (a natural benefic) in the 10th house of career and status. "
                f"Indicates a spotless reputation, ethical conduct in professional life, and lasting "
                f"fame built on noble and virtuous deeds."
            )

    # 17. Chandra-Adhi Yoga (benefics in 6th, 7th, 8th from Moon)
    if moon_h:
        sixth_from_moon  = (moon_h + 4) % 12 + 1
        seventh_from_moon = moon_h % 12 + 1
        eighth_from_moon  = (moon_h + 6) % 12 + 1
        adhi_houses = {sixth_from_moon, seventh_from_moon, eighth_from_moon}
        adhi_bens = [p for p in ["Jupiter", "Venus", "Mercury"]
                     if planet_data.get(p, {}).get("house") in adhi_houses]
        if len(adhi_bens) >= 2:
            yogas_found.append(
                f"🌟 Adhi Yoga — Benefics ({', '.join(adhi_bens)}) in 6th, 7th, and/or 8th from Moon. "
                f"This yoga produces ministers, commanders, and leaders. The native rises to authority "
                f"through intelligence, diplomacy, and inner moral strength."
            )

    # 18. Parivartana Yoga (mutual exchange)
    sign_lord_map = {
        "Aries": "Mars", "Taurus": "Venus", "Gemini": "Mercury", "Cancer": "Moon",
        "Leo": "Sun", "Virgo": "Mercury", "Libra": "Venus", "Scorpio": "Mars",
        "Sagittarius": "Jupiter", "Capricorn": "Saturn", "Aquarius": "Saturn", "Pisces": "Jupiter",
    }
    checked = set()
    for p1, d1 in planet_data.items():
        for p2, d2 in planet_data.items():
            if p1 >= p2 or (p1, p2) in checked:
                continue
            checked.add((p1, p2))
            if (sign_lord_map.get(d1.get("sign", "")) == p2
                    and sign_lord_map.get(d2.get("sign", "")) == p1):
                yogas_found.append(
                    f"🔄 Parivartana Yoga — {p1} and {p2} are in mutual exchange of signs. "
                    f"This creates a powerful bond between the two planets and the houses they occupy, "
                    f"activating both houses simultaneously and often producing unexpected but significant life results."
                )

    # 19. Shubha Kartari Yoga (planet hemmed by benefics)
    for planet_name, pdata in planet_data.items():
        ph = pdata.get("house", 0)
        if not ph:
            continue
        prev_h = (ph - 2) % 12 + 1
        next_h = ph % 12 + 1
        bens_adjacent = [p for p in ["Jupiter", "Venus", "Mercury"]
                         if planet_data.get(p, {}).get("house") in [prev_h, next_h]]
        if len(bens_adjacent) == 2:
            yogas_found.append(
                f"🌈 Shubha Kartari Yoga — {planet_name} is hemmed between two natural benefics "
                f"({', '.join(bens_adjacent)}). This protects and enhances the results of {planet_name}, "
                f"adding grace and good fortune to its house themes."
            )

    # 20. Graha Malika Yoga (planetary chain)
    occupied = sorted(set(v.get("house") for v in planet_data.values() if v.get("house")))
    chain_len = 1
    max_chain = 1
    for i in range(1, len(occupied)):
        if occupied[i] == occupied[i - 1] + 1:
            chain_len += 1
            max_chain = max(max_chain, chain_len)
        else:
            chain_len = 1
    if max_chain >= 5:
        yogas_found.append(
            f"🔗 Graha Malika Yoga — {max_chain} consecutive houses are occupied by planets, forming "
            f"a garland of planetary energy. This yoga indicates a life of intense activity, varied "
            f"experiences, and wide-ranging impact across multiple life domains."
        )

    if yogas_found:
        for i, yoga in enumerate(yogas_found, 1):
            output += f"{i}. {yoga}\n\n"
    else:
        output += "No major advanced yogas detected in this chart, though the unique planetary combinations create distinctive life patterns worthy of careful study.\n"

    return output


# ============================================================
# LORDSHIP ANALYSIS — ALL 12 HOUSE LORDS
# ============================================================

def analyze_lordships(planet_data):
    """Analyze all 12 house lords and their placements for complete lordship mapping."""
    output = "\n=== LORDSHIP ANALYSIS — ALL 12 HOUSE RULERS ===\n"
    output += (
        "Each house has a ruling lord whose placement in the chart determines how that house's "
        "themes manifest. This section maps every house lord to decode the full life blueprint.\n\n"
    )

    lagna = kundali.get("ascendant", "")
    if lagna not in SIGN_ORDER:
        return output + "Ascendant not found for lordship analysis.\n"

    lagna_idx = SIGN_ORDER.index(lagna)

    house_themes = {
        1: "personality, self, and physical vitality",
        2: "wealth, family, and speech",
        3: "effort, courage, siblings, and communication",
        4: "home, mother, education, and emotional peace",
        5: "intelligence, creativity, and children",
        6: "health, enemies, debts, and service",
        7: "marriage, partnerships, and business",
        8: "transformation, secrets, and hidden events",
        9: "luck, dharma, father, and higher knowledge",
        10: "career, status, authority, and public life",
        11: "gains, income, aspirations, and social networks",
        12: "foreign lands, spirituality, loss, and liberation",
    }

    for house_num in range(1, 13):
        house_sign = SIGN_ORDER[(lagna_idx + house_num - 1) % 12]
        lord = SIGN_LORDS.get(house_sign, "Unknown")
        lord_data = planet_data.get(lord, {})
        lord_house = lord_data.get("house", "?")
        lord_sign = lord_data.get("sign", "?")

        output += f"House {house_num:2d} ({house_sign}) — Lord: {lord:8s} → placed in House {lord_house} ({lord_sign})\n"
        output += f"  Domain: {house_themes.get(house_num, '')}\n"
        output += f"  Interpretation: The themes of House {house_num} are channelled through {lord}'s qualities.\n"

        if isinstance(lord_house, int):
            # Key combinations
            if house_num == 10 and lord_house in [10, 11]:
                output += "  ★ 10th lord in 10th/11th: Exceptional career strength and professional gains.\n"
            elif house_num == 10 and lord_house in [6, 8, 12]:
                output += "  ⚑ 10th lord in dusthana: Career may face obstacles but transformation is possible.\n"

            if house_num == 7 and lord_house in [7, 1, 5]:
                output += "  ★ 7th lord well-placed: Good partnership and marriage prospects.\n"
            elif house_num == 7 and lord_house in [6, 8, 12]:
                output += "  ⚑ 7th lord in dusthana: Relationships require patience and karmic work.\n"

            if house_num == 5 and lord_house in [5, 9, 1]:
                output += "  ★ 5th lord well-placed: Strong intelligence, creativity, and past-life merit active.\n"

            if house_num == 9 and lord_house in [9, 1, 5]:
                output += "  ★ 9th lord well-placed: Exceptional luck, dharmic grace, and spiritual alignment.\n"

            if house_num == 2 and lord_house in [2, 11, 5]:
                output += "  ★ 2nd lord well-placed: Wealth accumulation and financial stability are supported.\n"

            if house_num == 11 and lord_house in [11, 2, 10]:
                output += "  ★ 11th lord well-placed: Strong income, gains, and social network benefits.\n"

        output += "\n"

    return output


# ============================================================
# COMBUSTION (ASTA) ANALYSIS
# ============================================================

# Classical combustion orbs (degrees of separation from Sun within same sign)
_COMBUSTION_ORBS = {
    "Moon":    12,
    "Mars":    17,
    "Mercury": 14,
    "Jupiter": 11,
    "Venus":   10,
    "Saturn":  15,
}


def detect_combustion(planet_data):
    """Detect planets combust (Asta) by proximity to the Sun."""
    output = "\n=== COMBUSTION (ASTA) ANALYSIS ===\n"
    output += (
        "A planet is combust (Asta) when it falls within a critical angular distance "
        "of the Sun in the same sign, weakening its independent significations.\n\n"
    )

    sun = planet_data.get("Sun", {})
    sun_sign = sun.get("sign", "")
    sun_deg  = sun.get("degree", 0)

    if not sun_sign:
        return output + "Sun data unavailable for combustion analysis.\n"

    combust_found = False
    for planet, data in planet_data.items():
        if planet in ("Sun", "ASC", "Rahu", "Ketu"):
            continue
        orb = _COMBUSTION_ORBS.get(planet)
        if orb is None:
            continue
        if data.get("sign") == sun_sign:
            diff = abs(data.get("degree", 0) - sun_deg)
            if diff < orb:
                combust_found = True
                output += (
                    f"⚠  {planet} is COMBUST — {diff:.2f}° from Sun "
                    f"(orb limit: {orb}°) in {sun_sign}.\n"
                    f"   Effect: {planet}'s significations are weakened and "
                    f"overshadowed by solar energy. Independent results of {planet} "
                    f"may be muted or delayed; however, the native gains solar clarity "
                    f"in {planet}-ruled areas when consciously directing willpower.\n\n"
                )

    if not combust_found:
        output += "No planets are combust in this chart.\n"

    return output


# ============================================================
# CONJUNCTION ANALYSIS
# ============================================================

# Named conjunctions and their classical interpretations
_NAMED_CONJUNCTIONS = {
    frozenset({"Sun", "Mercury"}): (
        "Budha-Aditya Yoga — Sun and Mercury together bestow heightened intellect, "
        "communication mastery, and analytical brilliance. The native excels in writing, "
        "speaking, teaching, and advisory roles."
    ),
    frozenset({"Sun", "Venus"}): (
        "Sun-Venus conjunction — Artistic talent, charisma, and a refined aesthetic sense. "
        "Venus may be partially combust; check the Combustion section for degree orb."
    ),
    frozenset({"Sun", "Mars"}): (
        "Sun-Mars conjunction — Exceptional courage, leadership drive, and competitive energy. "
        "Aggression must be consciously channelled to avoid conflict and impulsiveness."
    ),
    frozenset({"Sun", "Moon"}): (
        "Amavasya (New Moon) conjunction — Strong willpower but potential emotional volatility; "
        "the mind and soul are deeply interlinked, creating intense focus."
    ),
    frozenset({"Jupiter", "Saturn"}): (
        "Jupiter-Saturn conjunction — A rare and weighty combination: disciplined wisdom, "
        "spiritual ambition, and long-term achievement through patient, principled effort. "
        "In a dusthana house this activates Vipreet Raj Yoga potential."
    ),
    frozenset({"Jupiter", "Venus"}): (
        "Jupiter-Venus conjunction — Wealth, beauty, and spiritual grace combined. "
        "Strong indications for prosperity, higher learning, and harmonious relationships."
    ),
    frozenset({"Jupiter", "Mercury"}): (
        "Jupiter-Mercury conjunction — Philosophical intellect and commercial acumen. "
        "Teaching, writing, law, and advisory careers are strongly supported."
    ),
    frozenset({"Mars", "Rahu"}): (
        "Angarak Yoga — Mars-Rahu conjunction creates intense, explosive drive. "
        "Success through bold unconventional action; impulsiveness and temper require management."
    ),
    frozenset({"Moon", "Rahu"}): (
        "Grahan Yoga — Moon-Rahu conjunction heightens imagination, ambition, and psychic "
        "sensitivity, but can bring emotional instability and obsessive mental patterns."
    ),
    frozenset({"Moon", "Ketu"}): (
        "Moon-Ketu conjunction — Deep intuition and past-life sensitivity. Emotional "
        "detachment and spiritual seeking are pronounced; worldly emotions feel unfamiliar."
    ),
    frozenset({"Venus", "Mars"}): (
        "Venus-Mars conjunction — Passionate, creative, and romantically charged energy. "
        "Strong artistic talent combined with bold, decisive ambition."
    ),
    frozenset({"Saturn", "Rahu"}): (
        "Saturn-Rahu conjunction (Shrapit Yoga) — Karmic burdens and unconventional "
        "discipline. Hard-earned but potentially dramatic rise after sustained effort."
    ),
    frozenset({"Saturn", "Mars"}): (
        "Saturn-Mars conjunction — Disciplined aggression; methodical execution of bold plans. "
        "Potential for engineering, construction, or rigorous physical/technical achievement."
    ),
}


def detect_conjunctions(planet_data):
    """Detect planets sharing a house and apply classical conjunction interpretations."""
    output = "\n=== CONJUNCTION ANALYSIS ===\n"
    output += (
        "Conjunctions occur when two or more planets occupy the same house, merging their "
        "energies and producing distinctive life themes in that house's domain.\n\n"
    )

    house_map = defaultdict(list)
    for planet, data in planet_data.items():
        if planet == "ASC":
            continue
        h = data.get("house")
        if h:
            house_map[h].append(planet)

    found = False
    for house_num, plist in sorted(house_map.items()):
        if len(plist) < 2:
            continue
        found = True
        sign = next((planet_data[p].get("sign", "") for p in plist), "")
        output += f"House {house_num} ({sign}): {' + '.join(plist)}\n"

        pset = set(plist)
        # Check every pair for named conjunctions
        named_shown = set()
        for p1 in plist:
            for p2 in plist:
                if p1 >= p2:
                    continue
                key = frozenset({p1, p2})
                if key in _NAMED_CONJUNCTIONS and key not in named_shown:
                    named_shown.add(key)
                    output += f"  → {_NAMED_CONJUNCTIONS[key]}\n"

        # Validate with classical text chunks
        keywords = [p.lower() for p in plist] + ["conjunction", "conjunct", "together"]
        insights = retrieve_insights(keywords)
        for insight in insights[:2]:
            interpreted = interpret_text(insight)
            if interpreted:
                output += f"  📖 Classical: {interpreted}\n"

        output += "\n"

    if not found:
        output += "No multi-planet conjunctions found in this chart.\n"

    return output


# ============================================================
# RETROGRADE PLANET ANALYSIS
# ============================================================

_RETROGRADE_PRINCIPLES = {
    "Saturn": (
        "If Saturn is retrograde: karmic lessons become more internalized. Discipline and "
        "delays are felt more keenly but the eventual rewards are exceptionally durable. "
        "The native revisits past-life karmic debts with greater intensity and thoroughness."
    ),
    "Jupiter": (
        "If Jupiter is retrograde: wisdom develops through introspection rather than "
        "external mentors. Blessings arrive after re-evaluation and inner growth. "
        "Spiritual or philosophical values may be unconventional and self-developed."
    ),
    "Mars": (
        "If Mars is retrograde: energy and drive turn inward — the native is a 'strategic "
        "warrior' who plans before acting. Assertiveness can be bottled up and released "
        "suddenly; physical vitality strengthens through disciplined internal practice."
    ),
    "Mercury": (
        "If Mercury is retrograde: communication and analysis deepen through review and "
        "revision. The intellect is thorough and non-linear. Writing, research, and "
        "behind-the-scenes analytical work often produce outstanding results."
    ),
    "Venus": (
        "If Venus is retrograde: relationship values are reassessed through deep inner "
        "reflection. Artistic ability deepens by revisiting and refining creative works. "
        "Past-life romantic patterns may resurface for resolution."
    ),
}


def detect_retrograde(planet_data):
    """Provide retrograde interpretation framework for the chart."""
    output = "\n=== RETROGRADE PLANET ANALYSIS ===\n"
    output += (
        "Retrograde planets (Vakri Graha) redirect their energy inward, deepening internal "
        "development while delaying external results. Retrograde flags are not encoded in "
        "the source kundali JSON; the following principles apply if any of these planets "
        "were retrograde at birth — verify against an ephemeris for the birth date.\n\n"
    )
    for planet, principle in _RETROGRADE_PRINCIPLES.items():
        if planet in planet_data:
            output += f"{planet}:\n  {principle}\n\n"
    return output


# ============================================================
# DASHA TIMELINE — TIME + EVENT + REASON
# ============================================================

_DASHA_PROFILES = {
    "Sun": {
        "theme":       "Authority, identity, and professional recognition",
        "events":      "Career advancement, government connections, clarity of life purpose, recognition from superiors",
        "challenges":  "Ego conflicts, authority clashes, health issues (heart/eyes), father-related events",
        "reason":      "Sun governs the soul, ego, authority, and the father — its period activates these themes centrally",
    },
    "Moon": {
        "theme":       "Mind, emotions, public life, and the mother",
        "events":      "Social popularity, career in public-facing roles, emotional growth, travel, mother-related events",
        "challenges":  "Emotional instability, mental anxiety, wavering decisions, sensitivity to criticism",
        "reason":      "Moon governs the mind and emotional field — its period heightens sensitivity, intuition, and public dealings",
    },
    "Mars": {
        "theme":       "Energy, action, property, and ambition",
        "events":      "Property transactions, competitive success, sibling-related events, physical ventures, bold career moves",
        "challenges":  "Impulsiveness, accidents, conflicts, short-temper, disputes over land or siblings",
        "reason":      "Mars governs drive, courage, and real estate — its period is dynamic, action-oriented, and competitive",
    },
    "Mercury": {
        "theme":       "Intellect, communication, business, and education",
        "events":      "Career in communication/technology/business, educational achievements, writing, networking successes",
        "challenges":  "Indecisiveness, nervous energy, information overload, contractual disputes",
        "reason":      "Mercury governs the analytical mind and commerce — its period favours intellectual and business pursuits",
    },
    "Jupiter": {
        "theme":       "Wisdom, expansion, spirituality, and fortune",
        "events":      "Marriage, children, higher education, career expansion, spiritual initiation, financial growth",
        "challenges":  "Overconfidence, excessive optimism, weight gain, legal matters",
        "reason":      "Jupiter governs dharma, wisdom, and abundance — its period is among the most auspicious in Vedic astrology",
    },
    "Venus": {
        "theme":       "Relationships, beauty, luxury, and creative expression",
        "events":      "Marriage/romantic union, artistic career growth, financial gains, social expansion, comforts and pleasures",
        "challenges":  "Overindulgence, relationship complications, material attachment",
        "reason":      "Venus governs love, beauty, and material comfort — its period activates relationships and creative life",
    },
    "Saturn": {
        "theme":       "Discipline, karma, hard work, and long-term building",
        "events":      "Career restructuring, relocation, karmic relationship events, professional authority, property through hard work",
        "challenges":  "Delays, health burdens, loneliness, isolation, losses, heavy responsibilities",
        "reason":      "Saturn governs karma and discipline — its long 19-year period is the most transformative and character-building",
    },
    "Rahu": {
        "theme":       "Ambition, foreign connections, and unconventional breakthroughs",
        "events":      "Rapid material rise, foreign travel or settlement, unconventional career success, sudden fame or notoriety",
        "challenges":  "Illusion, obsession, deception, scattered focus, sudden reversals",
        "reason":      "Rahu amplifies desire and drives the native toward obsessive ambition — results are dramatic but volatile",
    },
    "Ketu": {
        "theme":       "Spirituality, detachment, and past-life resolution",
        "events":      "Spiritual awakening, withdrawal from worldly pursuits, psychic experiences, losses that lead to liberation",
        "challenges":  "Isolation, purposelessness, health issues, worldly disorientation",
        "reason":      "Ketu governs liberation and past-life karma — its period dissolves worldly attachments in favour of inner growth",
    },
}


_NATURAL_MALEFICS = {"Sun", "Mars", "Saturn", "Rahu", "Ketu"}


def _house_lord_context(planet, planet_data):
    """Return a brief house-placement context string for a planet."""
    data = planet_data.get(planet, {})
    house = data.get("house")
    sign  = data.get("sign", "?")
    if house is None:
        return ""
    return f"{planet} is placed in House {house} ({sign})"


def analyze_dasha_timeline(dasha_list, planet_data):
    """Generate structured Time + Event + Reason predictions for every Mahadasha."""
    output = "\n=== DASHA TIMELINE — TIME | EVENT | REASON ===\n"
    output += (
        "Each Mahadasha period is analysed with its time window, predicted life events, "
        "and the astrological reason drawn from the chart's unique planetary placements.\n\n"
    )

    for idx, period in enumerate(dasha_list):
        planet = period.get("planet", "Unknown")
        start  = period.get("start", "?")
        end    = period.get("end", "?")
        years  = period.get("years", "?")

        profile = _DASHA_PROFILES.get(planet, {})
        placement = _house_lord_context(planet, planet_data)

        output += f"{'=' * 55}\n"
        output += f"  {planet.upper()} MAHADASHA   {start} → {end}   ({years} years)\n"
        output += f"{'=' * 55}\n"

        if profile:
            output += f"  Theme:      {profile.get('theme', '')}\n"
            output += f"  Time:       {start} to {end}\n"
            output += f"  Events:     {profile.get('events', '')}\n"
            output += f"  Challenges: {profile.get('challenges', '')}\n"
            output += f"  Reason:     {profile.get('reason', '')}\n"
            if placement:
                output += f"  Placement:  {placement}\n"

        # House-specific refinement
        pdata = planet_data.get(planet, {})
        house = pdata.get("house")
        sign  = pdata.get("sign", "")

        if house == 12:
            if planet in _NATURAL_MALEFICS:
                output += (
                    f"  Chart note: {planet} in 12th house — events lean toward foreign lands, "
                    f"spiritual pursuits, research, or behind-the-scenes institutional work. "
                    f"Vipreet Raj Yoga is active: {planet} is a natural malefic in a dusthana, "
                    f"giving unexpected rise and success through adversity.\n"
                )
            else:
                output += (
                    f"  Chart note: {planet} in 12th house — events lean toward foreign lands, "
                    f"spiritual pursuits, research, or behind-the-scenes institutional work. "
                    f"As a natural benefic in the 12th, {planet} supports spiritual and foreign "
                    f"connections rather than material accumulation.\n"
                )
        elif house in (1, 5, 9):
            output += (
                f"  Chart note: {planet} in House {house} (trikona) — dharmic grace and "
                f"fortunate results from this Mahadasha are strongly supported.\n"
            )
        elif house in (4, 7, 10):
            output += (
                f"  Chart note: {planet} in House {house} (kendra) — this Mahadasha brings "
                f"decisive, tangible results in career, relationships, or domestic life.\n"
            )
        elif house in (6, 8):
            output += (
                f"  Chart note: {planet} in House {house} (dusthana) — transformation and "
                f"struggle characterise this period, but hard-won gains are lasting.\n"
            )

        # Classical text validation
        keywords = [planet.lower(), "dasha", "mahadasa", sign.lower(), "period"]
        insights = retrieve_insights(keywords)
        classical_added = 0
        for insight in insights:
            interpreted = interpret_text(insight)
            if interpreted and classical_added < 2:
                output += f"  📖 Classical: {interpreted}\n"
                classical_added += 1

        output += "\n"

    return output


# ============================================================
# HOUSE LORD EFFECTS SUMMARY
# ============================================================

_HOUSE_LORD_EFFECT_RULES = {
    # (house_of_lord, placed_in_house) -> effect description
    # Capture most meaningful placements
    (1, 1):  "Lagna lord in Lagna — strong self, vitality, and personal authority; life is self-directed.",
    (1, 5):  "Lagna lord in 5th — intelligence, creativity, and children are key life themes.",
    (1, 9):  "Lagna lord in 9th — dharmic life path; fortune through higher knowledge and spiritual pursuit.",
    (1, 10): "Lagna lord in 10th — career is the primary life focus; professional recognition is prominent.",
    (1, 12): "Lagna lord in 12th — spiritual orientation, possible foreign settlement; inner journey dominates.",
    (2, 2):  "2nd lord in 2nd — stable family wealth and strong speech.",
    (2, 11): "2nd lord in 11th — financial gains multiply through networks and elder siblings.",
    (5, 9):  "5th lord in 9th — exceptional intelligence linked to dharma, teaching, and higher philosophy.",
    (5, 5):  "5th lord in 5th — strong past-life merit; creativity, children, and intelligence all flourish.",
    (7, 7):  "7th lord in 7th — powerful partnership energy; marriage is a central and fulfilling life theme.",
    (7, 1):  "7th lord in Lagna — partnerships and self-identity are deeply intertwined.",
    (9, 1):  "9th lord in Lagna — dharma permeates personality; natural teacher or philosopher.",
    (9, 9):  "9th lord in 9th — exceptional luck, spiritual grace, and blessings from lineage.",
    (10, 10):"10th lord in 10th — peak career potential; authority, recognition, and professional legacy.",
    (10, 11):"10th lord in 11th — career directly translates to income and social network gains.",
    (11, 11):"11th lord in 11th — abundant income, fulfilled aspirations, and strong social influence.",
}


def analyze_house_lord_effects(planet_data):
    """Map every house lord's placement to its primary effect on life themes."""
    output = "\n=== HOUSE LORD EFFECTS ANALYSIS ===\n"
    output += (
        "This section maps each house lord to its placement to decode how the 12 life "
        "domains actually express in the chart.\n\n"
    )

    lagna = kundali.get("ascendant", "")
    if lagna not in SIGN_ORDER:
        return output + "Ascendant not found.\n"

    lagna_idx = SIGN_ORDER.index(lagna)

    for house_num in range(1, 13):
        house_sign = SIGN_ORDER[(lagna_idx + house_num - 1) % 12]
        lord = SIGN_LORDS.get(house_sign, "Unknown")
        lord_data = planet_data.get(lord, {})
        lord_house = lord_data.get("house")
        lord_sign  = lord_data.get("sign", "?")

        placed = f"House {lord_house} ({lord_sign})" if lord_house else "unknown"
        output += f"House {house_num:2d} lord ({lord:8s}) placed in {placed}\n"

        # Lookup specific rule
        rule = _HOUSE_LORD_EFFECT_RULES.get((house_num, lord_house))
        if rule:
            output += f"  ★ {rule}\n"
        elif lord_house in (6, 8, 12):
            output += (
                f"  ⚑ Lord of House {house_num} in dusthana (House {lord_house}) — "
                f"the themes of House {house_num} undergo transformation, hidden development, "
                f"or face karmic tests before manifesting fully.\n"
            )
        elif lord_house in (1, 4, 7, 10):
            output += (
                f"  ✓ Lord of House {house_num} in kendra (House {lord_house}) — "
                f"themes of House {house_num} are supported by angular strength and tangible results.\n"
            )
        elif lord_house in (5, 9):
            output += (
                f"  ✓ Lord of House {house_num} in trikona (House {lord_house}) — "
                f"themes of House {house_num} benefit from dharmic grace and fortunate circumstances.\n"
            )
        output += "\n"

    return output


# -------------------------------
# FINAL REPORT
# -------------------------------
def generate_report():
    print("\n🔱 ELITE VEDIC ASTROLOGY REPORT 🔱")
    print("Powered by Multi-Layer Jyotish Reasoning Engine\n")

    # --- Report Navigation ---
    print("\n=== REPORT STRUCTURE ===")
    print("1.  Introduction & Kundali Summary")
    print("2.  Planet-by-Planet Deep Synthesis")
    print("3.  Combustion (Asta) Analysis")
    print("4.  Conjunction Analysis")
    print("5.  Retrograde Planet Analysis")
    print("6.  Combination Analysis")
    print("7.  Classical Yogas + Advanced Yogas (20+)")
    print("8.  Lordship Analysis — All 12 House Rulers")
    print("9.  House Lord Effects Analysis")
    print("10. Lagna Lord | 10th Lord | 7th Lord")
    print("11. Career Deep Analysis")
    print("12. Marriage Deep Analysis")
    print("13. Dasha Timeline — Time | Event | Reason")
    print("14. Mahadasha + Antardasha + Timeline")
    print("15. Aspects (Drishti)")
    print("16. Navamsa (D9)")
    print("17. Shadbala (Planetary Strength)")
    print("18. Shastra Insights (Classical Texts)")
    print("19. Dosha Analysis")
    print("20. Transit Analysis + Sade Sati")
    print("21. Remedies")
    print("22. Final Prediction")
    print("23. Overall Summary\n")

    # --------------------------------------------------------
    # 1. INTRODUCTION & KUNDALI SUMMARY
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 1 — INTRODUCTION & KUNDALI SUMMARY")
    print("=" * 60)
    bd = kundali.get("basic_details", kundali)
    for key in ("name", "date_of_birth", "time_of_birth", "place_of_birth",
                "place", "lagna", "ascendant", "rasi", "nakshatra"):
        val = bd.get(key) or kundali.get(key)
        if val:
            print(f"  {key.replace('_', ' ').title()}: {val}")
    print()

    print("=== PLANETARY PLACEMENTS AT A GLANCE ===")
    for planet, data in planets.items():
        print(f"  {planet:10s} | Sign: {data.get('sign', 'N/A'):15s} | House: {data.get('house', 'N/A'):2} | Nakshatra: {data.get('nakshatra', 'N/A')}")
    print()

    # --------------------------------------------------------
    # 2. PLANET-BY-PLANET DEEP SYNTHESIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 2 — PLANET-BY-PLANET DEEP SYNTHESIS")
    print("=" * 60)
    print(synthesize_all_planets(planets))

    # --------------------------------------------------------
    # 3. COMBUSTION (ASTA) ANALYSIS  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 3 — COMBUSTION (ASTA) ANALYSIS")
    print("=" * 60)
    print(detect_combustion(planets))

    # --------------------------------------------------------
    # 4. CONJUNCTION ANALYSIS  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 4 — CONJUNCTION ANALYSIS")
    print("=" * 60)
    print(detect_conjunctions(planets))

    # --------------------------------------------------------
    # 5. RETROGRADE PLANET ANALYSIS  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 5 — RETROGRADE PLANET ANALYSIS")
    print("=" * 60)
    print(detect_retrograde(planets))

    # --------------------------------------------------------
    # 6. COMBINATION ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 6 — COMBINATION ANALYSIS")
    print("=" * 60)
    print(combined_analysis(planets))

    # --------------------------------------------------------
    # 7. YOGAS — CLASSICAL + ADVANCED
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 7 — YOGA ANALYSIS")
    print("=" * 60)
    print("\n🔥 CORE DESTINY FACTORS (YOGAS) 🔥\n")
    print(detect_real_yogas(planets))
    print(detect_advanced_yogas(planets))

    # --------------------------------------------------------
    # 8. LORDSHIP ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 8 — LORDSHIP ANALYSIS")
    print("=" * 60)
    print(analyze_lordships(planets))

    # --------------------------------------------------------
    # 9. HOUSE LORD EFFECTS  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 9 — HOUSE LORD EFFECTS ANALYSIS")
    print("=" * 60)
    print(analyze_house_lord_effects(planets))

    # --------------------------------------------------------
    # 10. LAGNA LORD | 10TH LORD | 7TH LORD
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 10 — LAGNA LORD | 10TH LORD | 7TH LORD")
    print("=" * 60)
    print(analyze_lagna_lord(kundali, planets))
    print(analyze_10th_lord(planets, kundali))
    print(analyze_7th_lord(planets, kundali))

    # --------------------------------------------------------
    # 11. CAREER DEEP ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 11 — CAREER DEEP ANALYSIS")
    print("=" * 60)
    print(analyze_career(planets))

    # --------------------------------------------------------
    # 12. MARRIAGE DEEP ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 12 — MARRIAGE DEEP ANALYSIS")
    print("=" * 60)
    print(analyze_marriage(planets))

    # --------------------------------------------------------
    # 13. DASHA TIMELINE — TIME | EVENT | REASON  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 13 — DASHA TIMELINE (TIME | EVENT | REASON)")
    print("=" * 60)
    print(analyze_dasha_timeline(dasha, planets))

    # --------------------------------------------------------
    # 14. DASHA + ANTARDASHA + TIMELINE
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 14 — TIMING ANALYSIS (DASHA SYSTEM)")
    print("=" * 60)
    print("\n⏳ TIMING ANALYSIS (DASHA SYSTEM) ⏳\n")
    print(analyze_dasha())
    print(analyze_antardasha(dasha))
    print(antardasha_timeline(dasha))

    # --------------------------------------------------------
    # 15. ASPECTS (DRISHTI)
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 15 — ASPECTS (DRISHTI)")
    print("=" * 60)
    print("\n=== ASPECTS (DRISHTI) ===")
    for a in calculate_aspects(planets):
        print(" ", a)

    # --------------------------------------------------------
    # 16. NAVAMSA (D9)
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 16 — NAVAMSA (D9)")
    print("=" * 60)
    print("\n=== NAVAMSA (D9) ===")
    for p, d in planets.items():
        nav = calculate_navamsa(d.get("degree", 0), d.get("sign", ""))
        print(f"  {p:10s} → Navamsa sign: {nav}")

    # --------------------------------------------------------
    # 17. SHADBALA
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 17 — SHADBALA (PLANETARY STRENGTH)")
    print("=" * 60)
    print("\n=== SHADBALA (PLANETARY STRENGTH) ===")
    print("This section evaluates the strength of planets in your chart and their ability to deliver results.\n")
    for s in improved_shadbala(planets):
        print(" ", s)

    # --------------------------------------------------------
    # 18. SHASTRA INSIGHTS (CLASSICAL TEXTS)
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 18 — SHASTRA INSIGHTS (CLASSICAL TEXTS)")
    print("=" * 60)
    print("\n=== SHASTRA INSIGHTS ===")
    print("The following insights are drawn from classical Vedic texts and are relevant to the planetary placements in your chart.\n")
    keywords = []
    for p, d in planets.items():
        keywords.append(p.lower())
        keywords.append(d.get("sign", "").lower())
        keywords.append(d.get("nakshatra", "").lower())

    insights = retrieve_insights(keywords)
    for i in insights:
        interpreted = interpret_text(i[:300])
        if interpreted:
            print("-", interpreted)

    # --------------------------------------------------------
    # 19. DOSHA ANALYSIS
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 19 — DOSHA ANALYSIS")
    print("=" * 60)
    print(detect_doshas(planets))

    # --------------------------------------------------------
    # 20. TRANSIT ANALYSIS + SADE SATI
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 20 — TRANSIT ANALYSIS + SADE SATI")
    print("=" * 60)
    print("\n=== TRANSIT ANALYSIS ===")
    print("This section examines how current planetary transits are interacting with your natal chart and what shifts they may bring.\n")
    transit_text = saturn_transit_effect(planets)
    print(" ", transit_text)
    print("\n🪐 TRANSIT ANALYSIS (INCLUDING SADE SATI) 🪐\n")
    print(analyze_sadesati())

    # --------------------------------------------------------
    # 21. REMEDIES
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 21 — REMEDIES")
    print("=" * 60)
    print(suggest_remedies(planets))

    # --------------------------------------------------------
    # 22. FINAL PREDICTION
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 22 — FINAL PREDICTION")
    print("=" * 60)
    final_pred = generate_final_prediction(planets, dasha, transit_text)
    print(final_pred)

    # --------------------------------------------------------
    # 23. OVERALL SUMMARY
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 23 — OVERALL SUMMARY")
    print("=" * 60)
    print("\n=== OVERALL SUMMARY ===")
    print("Your chart shows a blend of karmic challenges and growth opportunities. With the right effort and awareness, strong progress is indicated in key areas of life.")


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    REPORT_OUTPUT_FILE = "astrology_report.txt"

    class _Tee:
        """Write to both the report file and the real stdout simultaneously."""
        def __init__(self, file_obj):
            self._file = file_obj
            self._stdout = sys.__stdout__

        def write(self, data):
            self._stdout.write(data)
            self._file.write(data)

        def flush(self):
            self._stdout.flush()
            self._file.flush()

    with open(REPORT_OUTPUT_FILE, "w", encoding="utf-8") as _report_file:
        sys.stdout = _Tee(_report_file)
        try:
            generate_report()
        finally:
            sys.stdout = sys.__stdout__

    print(f"\n✅ Report saved to {REPORT_OUTPUT_FILE}")

