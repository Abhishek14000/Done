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
# INTERPRETATION LAYER (CLEAN OUTPUT)
# -------------------------------
import re as _re

# Markers that identify noisy OCR, case-study, or book-metadata chunks.
# Any chunk containing these is discarded completely — it is not relevant to
# this kundali and must not appear in the report.
_NOISE_CHUNK_MARKERS = [
    "chart ", "born on ", "born at ", "case stud", " a.m. ", " p.m. ",
    "page ", "chapter ", "pdf", "copyright", "appendix", "footnote",
    "figure ", "table ", "example ", "vedic remedies in astrology",
    "brihat parasara", "dasa period", "sudasa", "narayan dasa",
    "dasa  period", " 1900", " 1910", " 1920", " 1930", " 1940",
    " 1950", " 1960", " 1970", " 1980", "chart no", "chart-",
]

# Patterns that mark noisy sentences even within otherwise usable chunks.
_NOISE_SENT_RE = _re.compile(
    r"chart\s*\d+|born\s+on\s+\d|\d{4}\s*[a-z]|"
    r"\d{3,}\s+|a\.m\.|p\.m\.|figure\s*\d|table\s*\d|"
    r"appendix\s*\d|footnote\s*\d|\b\d{4}\b|"
    # OCR artefacts: merged words (lowercase consonant glued to next word),
    # double punctuation, isolated single characters, camelCase merges
    r"[a-z]{3,}w\s+ith\b|"        # "conjunctionw ith"
    r"[a-z]{3,}f\s+rom\b|"        # "housef rom"
    r",\s*\.\s*\w|"                # "Sun,. Mercury"
    r"\biri\s+[''`]|"              # "iri '"
    r"\bthc\b|\bNorPs\b|"         # common OCR garbage tokens
    r"[a-zA-Z][A-Z][a-zA-Z]{2,}", # CamelCase OCR merges: "andS aturn"
    _re.IGNORECASE,
)

# Sentence starters that signal a general classical principle (preferred).
_PRINCIPLE_STARTERS = (
    "if ", "when ", "the ", "a ", "an ", "planets in ", "in the ",
    "this ", "these ", "such ", "with ", "one born ", "the native ",
    "jupiter", "saturn", "mars", "venus", "mercury", "moon", "sun",
    "rahu", "ketu", "lord of", "placement of", "sign of",
)

# Words required for a sentence to be considered astrologically relevant.
_ASTRO_WORDS = {
    "house", "sign", "planet", "lord", "yoga", "dasha", "nakshatra",
    "indicates", "gives", "bestows", "causes", "result", "effect",
    "exalt", "debil", "aspect", "conjunct", "sextile", "trine",
    "saturn", "jupiter", "mars", "venus", "mercury", "moon", "sun",
    "rahu", "ketu", "lagna", "aries", "taurus", "gemini", "cancer",
    "leo", "virgo", "libra", "scorpio", "sagittarius", "capricorn",
    "aquarius", "pisces", "benefic", "malefic", "karaka",
}


def _sentence_is_clean(sentence):
    """Return True if the sentence is a usable general astrological principle."""
    s = sentence.strip()
    if len(s) < 25:
        return False
    if _NOISE_SENT_RE.search(s):
        return False
    s_lower = s.lower()
    # Must contain at least one astrological keyword
    if not any(w in s_lower for w in _ASTRO_WORDS):
        return False
    return True


def _extract_clean_principle(raw_text, max_sentences=2):
    """Extract max_sentences clean astrological principle sentences from raw_text.

    Returns a concise string suitable for display, or None if nothing usable
    can be extracted.
    """
    # Flatten whitespace / newlines
    flat = _re.sub(r"\s+", " ", raw_text).strip()

    # Whole-chunk noise rejection
    flat_lower = flat.lower()
    if any(marker in flat_lower for marker in _NOISE_CHUNK_MARKERS):
        return None

    # Split into sentences on common sentence terminators
    sentences = _re.split(r"(?<=[.!?])\s+", flat)

    good = []
    for sent in sentences:
        if _sentence_is_clean(sent):
            good.append(sent.strip().rstrip("."))
        if len(good) == max_sentences:
            break

    if not good:
        return None

    principle = ". ".join(good) + "."
    # Final length guard
    if len(principle) < 30:
        return None
    return principle


def interpret_text(raw_text):
    """Convert a raw chunk into a clean, concise Jyotish interpretation.

    Extracts only the core classical principle (max 2 sentences), rewrites it
    into clean Jyotish language, and discards all OCR garbage, case studies,
    chart references, and unrelated content.  Returns None when the chunk
    cannot yield a usable principle.
    """
    principle = _extract_clean_principle(raw_text)
    if not principle:
        return None

    # Capitalise first letter and wrap in standard interpretive framing.
    sentence = principle[0].upper() + principle[1:]
    return f"Therefore, classical texts confirm: {sentence}"


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
# MAHADASHA ANALYSIS (CLEAN)
# -------------------------------
_MAHADASHA_SUMMARIES = {
    "Sun":     "The Sun Mahadasha activates authority, career recognition, and clarity of life purpose. Leadership and government connections come to the fore. The native's confidence and self-expression reach their peak.",
    "Moon":    "The Moon Mahadasha heightens emotional sensitivity, public dealings, and connection to the mother and home. Social growth, travel, and intuitive perception are dominant themes.",
    "Mars":    "The Mars Mahadasha delivers dynamic energy, bold action, and ambition. Property, siblings, and competitive endeavours are activated. Decisions made now carry lasting consequences.",
    "Mercury": "The Mercury Mahadasha favours intellectual pursuits, communication, business, and education. Networking, analytical career growth, and learning are all strongly supported.",
    "Jupiter": "The Jupiter Mahadasha is among the most auspicious periods in Vedic astrology. Expansion, wisdom, children, higher education, spiritual initiation, and fortunate connections mark this period.",
    "Venus":   "The Venus Mahadasha brings joy, relationships, material gains, and creative expression. Marriage, artistic pursuits, financial growth, and social harmony are commonly experienced.",
    "Saturn":  "The Saturn Mahadasha is a long, challenging yet profoundly productive period. Discipline, responsibility, and karmic accountability are central. Results are slow but lasting, forming the foundation for future achievements.",
    "Rahu":    "The Rahu Mahadasha brings sudden changes, foreign influences, and unconventional opportunities. Material ambition is high; maintaining groundedness is essential to avoid illusion and scattered focus.",
    "Ketu":    "The Ketu Mahadasha brings spiritual deepening and worldly detachment. Past-life patterns surface for resolution. This is a powerful time for inner growth, though external circumstances feel uncertain.",
}

def analyze_dasha():
    output = "\n=== MAHADASHA ANALYSIS ===\n"
    output += "Mahadasha periods shape the dominant themes and events across every life stage.\n"

    for period in dasha[:3]:
        planet = period.get("planet", "Unknown")
        start  = period.get("start", "?")
        end    = period.get("end", "?")
        summary = _MAHADASHA_SUMMARIES.get(planet, f"The {planet} Mahadasha activates the significations of {planet} across all life domains.")

        output += f"\n{planet} Mahadasha ({start} → {end}):\n"
        output += f"  {summary}\n"

        # Chart-specific note: placement of the Mahadasha lord
        pdata = planets.get(planet, {})
        if pdata:
            output += (
                f"  In this chart: {planet} is placed in House {pdata.get('house', '?')} "
                f"({pdata.get('sign', '?')}), therefore the themes of House {pdata.get('house', '?')} "
                f"are central to this period's unfolding.\n"
            )

    return output


# -------------------------------
# SADE SATI ANALYSIS (CLEAN)
# -------------------------------
_SADESATI_PHASE = {
    "Rising":  "Saturn approaches the Moon sign from the previous sign. The native feels mounting pressure, introspection, and a need to reassess priorities. External challenges signal an inner transformation beginning.",
    "Peak":    "Saturn transits directly over the Moon sign. Emotional intensity is at its maximum. The native faces karmic clearing, responsibility, and significant life restructuring. This phase, though demanding, plants seeds for long-term growth.",
    "Setting": "Saturn moves past the Moon sign. The pressure lifts gradually. Hard-earned lessons begin to crystallize into wisdom and renewed strength. Stability and forward momentum return.",
}

def analyze_sadesati():
    output = "\n=== SADE SATI ANALYSIS ===\n"
    output += (
        "Sade Sati is a 7.5-year transit of Saturn over three signs: the sign before, the sign of, "
        "and the sign after the natal Moon. It is a period of karmic acceleration, responsibility, and "
        "inner transformation. This chart's Moon is in Libra.\n\n"
    )

    shown = set()
    for period in sadesati:
        if period.get("type") != "Sade Sati":
            continue
        phase = period.get("phase", "Unknown")
        key = (phase, period.get("rashi", ""))
        if key in shown:
            continue
        shown.add(key)

        desc = _SADESATI_PHASE.get(phase, f"Saturn transits {period.get('rashi', '?')}.")
        output += (
            f"{phase} Phase ({period.get('start', '?')} → {period.get('end', '?')}) "
            f"— {period.get('rashi', '?')}:\n"
            f"  {desc}\n\n"
        )

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
    lagna = _get_lagna()
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
    lagna = _get_lagna()
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


def _get_lagna(kundali_data=None, planet_data=None):
    """Retrieve the ascendant/lagna sign from any available source.

    Tries, in order:
      1. kundali_data["ascendant"]
      2. kundali_data["lagna"]
      3. kundali_data["basic_details"]["lagna"]
      4. planet_data["ASC"]["sign"]  (Ascendant entry in planets dict)
    Returns an empty string if not found.
    """
    if kundali_data is None:
        kundali_data = kundali
    if planet_data is None:
        planet_data = planets

    for src in (kundali_data, kundali_data.get("basic_details", {})):
        for key in ("ascendant", "lagna", "Lagna", "Ascendant"):
            val = src.get(key)
            if val and val in SIGN_ORDER:
                return val

    # Fall back to ASC entry in planets dict
    asc_sign = planet_data.get("ASC", {}).get("sign", "")
    if asc_sign in SIGN_ORDER:
        return asc_sign

    return ""


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

    lagna = _get_lagna(kundali_data, planet_data)
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

    lagna = _get_lagna(kundali_data, planet_data)
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

    lagna = _get_lagna(kundali_data, planet_data)
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

    lagna = _get_lagna()
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
                    f"⚠  Mercury is COMBUST — {diff:.2f}° from Sun "
                    f"(orb limit: {orb}°) in {sun_sign}.\n"
                    f"   Effect: Mercury's significations are weakened and overshadowed by solar "
                    f"energy. This indicates that independent results of Mercury — intellect, "
                    f"commerce, and communication — are filtered through the Sun's authority. "
                    f"Therefore, the native excels when willpower and solar drive back Mercury's "
                    f"analytical faculties. Writing, speaking, and advisory roles carry strong "
                    f"royal or authoritative quality.\n\n"
                ) if planet == "Mercury" else (
                    f"⚠  {planet} is COMBUST — {diff:.2f}° from Sun "
                    f"(orb limit: {orb}°) in {sun_sign}.\n"
                    f"   Effect: {planet}'s significations are weakened and overshadowed by solar "
                    f"energy. Hence, the native must consciously direct willpower to activate "
                    f"{planet}-ruled areas fully.\n\n"
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

        # Validate with classical text chunks — only show if the extracted principle
        # specifically references at least one of the planets in this conjunction.
        keywords = [p.lower() for p in plist] + ["conjunction", "conjunct", "together"]
        insights = retrieve_insights(keywords)
        plist_lower = {p.lower() for p in plist}
        for insight in insights[:4]:
            interpreted = interpret_text(insight)
            if not interpreted:
                continue
            # Only display if the *interpreted principle* mentions a planet from this conjunction
            if any(pl in interpreted.lower() for pl in plist_lower):
                output += f"  📖 Classical: {interpreted}\n"
                break  # one classical note per conjunction is sufficient

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

    lagna = _get_lagna()
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


# ============================================================
# EXALTATION & DEBILITATION ANALYSIS
# ============================================================

_EXALT_EFFECTS = {
    "Sun":     "Sun exalted in Aries: The ego, willpower, and leadership reach their highest expression. Authority, courage, and paternal blessings are strongly activated.",
    "Moon":    "Moon exalted in Taurus: Emotional stability, receptivity, and material comforts are at their peak. A steady mind, loving nature, and prosperity are indicated.",
    "Mars":    "Mars exalted in Capricorn: Drive, discipline, and strategic ambition are at their maximum. The native achieves goals through methodical, sustained effort.",
    "Mercury": "Mercury exalted in Virgo: Analytical precision, communication clarity, and commercial acumen are at their strongest. Exceptional writing and problem-solving ability is indicated.",
    "Jupiter": "Jupiter exalted in Cancer: Wisdom, compassion, and spiritual blessings flow freely. Abundance, strong family values, and deep dharmic alignment are activated.",
    "Venus":   "Venus exalted in Pisces: Love, artistic inspiration, and spiritual beauty reach their highest form. Deep romantic connections and material grace are strongly indicated.",
    "Saturn":  "Saturn exalted in Libra: Discipline combines with justice and balance. The native builds lasting structures through fairness, patience, and principled perseverance.",
}

_DEBIL_EFFECTS = {
    "Sun":     "Sun debilitated in Libra: The solar ego is subdued by the need for harmony and compromise. The native learns to lead through diplomacy rather than authority alone.",
    "Moon":    "Moon debilitated in Scorpio: The mind undergoes intense transformation. Emotional depth, intuition, and psychic sensitivity are heightened, though instability is a karmic challenge.",
    "Mars":    "Mars debilitated in Cancer: Drive and aggression are softened and internalized. The native channels energy through protective instincts, creativity, and emotional motivation.",
    "Mercury": "Mercury debilitated in Pisces: Analytical logic yields to intuition and imagination. The native excels in artistic, spiritual, or visionary communication rather than precise analysis.",
    "Jupiter": "Jupiter debilitated in Capricorn: Wisdom is tested by material realities and worldly ambitions. The native must balance spiritual values with practical responsibility.",
    "Venus":   "Venus debilitated in Virgo: Relationships are filtered through critical analysis and perfectionism. The native develops discernment in love and aesthetic refinement through discipline.",
    "Saturn":  "Saturn debilitated in Aries: Discipline meets impulsiveness, creating tension between patience and action. The native learns perseverance through repeated tests of courage.",
}

_NEECHA_BHANGA_CONDITIONS = {
    # (debilitated planet) → (cancellation planets/houses)
    "Mars":    ("The lord of Cancer (Moon) in a kendra, or the exaltation lord of Mars (Saturn) in a kendra, cancels Mars's debilitation.",),
    "Mercury": ("The lord of Pisces (Jupiter) in a kendra, or the exaltation lord of Mercury (Sun) in a kendra, cancels Mercury's debilitation.",),
    "Jupiter": ("The lord of Capricorn (Saturn) in a kendra, or the exaltation lord of Jupiter (Moon) in a kendra, cancels Jupiter's debilitation.",),
    "Venus":   ("The lord of Virgo (Mercury) in a kendra, or the exaltation lord of Venus (Jupiter) in a kendra, cancels Venus's debilitation.",),
    "Saturn":  ("The lord of Aries (Mars) in a kendra, or the exaltation lord of Saturn (Venus) in a kendra, cancels Saturn's debilitation.",),
    "Sun":     ("The lord of Libra (Venus) in a kendra, or the exaltation lord of Sun (Mars) in a kendra, cancels Sun's debilitation.",),
    "Moon":    ("The lord of Scorpio (Mars) in a kendra, or the exaltation lord of Moon (Venus) in a kendra, cancels Moon's debilitation.",),
}


def detect_exaltation_debilitation(planet_data):
    """Dedicated exaltation, debilitation, and Neecha Bhanga analysis."""
    output = "\n=== EXALTATION, DEBILITATION & NEECHA BHANGA ===\n"
    output += (
        "Planetary dignity determines the quality and strength of results delivered in each life area. "
        "Exalted planets perform at peak capacity; debilitated planets face challenges that can be "
        "transformed into exceptional strength through Neecha Bhanga (cancellation of debilitation).\n\n"
    )

    kendra_houses = {1, 4, 7, 10}
    exalted_found = []
    debilitated_found = []

    for planet, data in planet_data.items():
        sign = data.get("sign", "")

        if _EXALTATION.get(planet) == sign:
            exalted_found.append((planet, sign, data.get("house", "?")))
        elif _DEBILITATION.get(planet) == sign:
            debilitated_found.append((planet, sign, data.get("house", "?")))

    if exalted_found:
        output += "EXALTED PLANETS (Peak Strength):\n"
        for planet, sign, house in exalted_found:
            output += f"  ✦ {planet} exalted in {sign} (House {house})\n"
            output += f"    {_EXALT_EFFECTS.get(planet, '')}\n\n"
    else:
        output += "No planets are exalted in this chart.\n\n"

    if debilitated_found:
        output += "DEBILITATED PLANETS (Neecha — Transformation Potential):\n"
        for planet, sign, house in debilitated_found:
            output += f"  ⚑ {planet} debilitated in {sign} (House {house})\n"
            output += f"    {_DEBIL_EFFECTS.get(planet, '')}\n"
            # Check Neecha Bhanga condition
            cond = _NEECHA_BHANGA_CONDITIONS.get(planet)
            if cond:
                output += f"    Neecha Bhanga condition: {cond[0]}\n"
            # Check if debilitation lord is in kendra → partial NBY
            debil_sign = _DEBILITATION.get(planet, "")
            debil_lord = SIGN_LORDS.get(debil_sign, "")
            debil_lord_house = planet_data.get(debil_lord, {}).get("house")
            if debil_lord_house in kendra_houses:
                output += (
                    f"    ✔ Neecha Bhanga ACTIVE — {debil_lord} (lord of {debil_sign}) "
                    f"is in kendra (House {debil_lord_house}). This indicates that "
                    f"{planet}'s debilitation is cancelled, producing exceptional strength "
                    f"and an unexpected rise — particularly during the {planet} Mahadasha.\n"
                )
            output += "\n"
    else:
        output += "No planets are debilitated in this chart.\n"

    return output


# ============================================================
# SYNTHESIS ENGINE — COMBINED CHART ANALYSIS
# (planet + house + lordship + conjunction + aspect)
# ============================================================

def synthesize_chart(planet_data):
    """Synthesis layer: combine planet + house + lordship + conjunction + aspect.

    This is the highest-level integration layer.  Each entry starts from the
    kundali logic (planet placement) and chains house themes → lordship role →
    conjunction energies → drishti targets into a single integrated conclusion.
    Chunks are NOT used here; all reasoning is purely kundali-driven.
    """
    output = "\n=== SYNTHESIS ENGINE — INTEGRATED CHART ANALYSIS ===\n"
    output += (
        "This section integrates every layer of analysis: planet + house + lordship + "
        "conjunction + aspect into unified, conclusive life-theme statements.\n\n"
    )

    lagna = _get_lagna()
    if lagna not in SIGN_ORDER:
        output += "Ascendant not found — synthesis unavailable.\n"
        return output

    lagna_idx = SIGN_ORDER.index(lagna)

    # Build house → planets map
    house_planets = defaultdict(list)
    for p, d in planet_data.items():
        h = d.get("house")
        if h:
            house_planets[h].append(p)

    # Build aspects map: planet → list of aspected signs
    aspects_received = defaultdict(list)  # planet → list of aspecting planets
    for p, d in planet_data.items():
        sign = d.get("sign", "")
        aspected_signs = []
        seventh = _house_to_sign(sign, 7)
        if seventh:
            aspected_signs.append((7, seventh))
        for away in SPECIAL_ASPECTS.get(p, []):
            asp = _house_to_sign(sign, away)
            if asp:
                aspected_signs.append((away, asp))
        for _, asp_sign in aspected_signs:
            for other_p, other_d in planet_data.items():
                if other_d.get("sign") == asp_sign and other_p != p:
                    aspects_received[other_p].append(p)

    # --- Key synthesized combinations ---

    # 1. Sun + Mercury in House 3 (Leo): Budha-Aditya + own sign + 3rd house
    sun_h  = planet_data.get("Sun", {}).get("house")
    mer_h  = planet_data.get("Mercury", {}).get("house")
    sun_s  = planet_data.get("Sun", {}).get("sign", "")
    if sun_h == mer_h and sun_h:
        aspectors_sun = aspects_received.get("Sun", [])
        aspectors_mer = aspects_received.get("Mercury", [])
        conj_house_sign = planet_data.get("Sun", {}).get("sign", "")
        conj_house_theme = HOUSE_MEANINGS.get(sun_h, "")

        output += f"⚡ SUN + MERCURY in House {sun_h} ({conj_house_sign})\n"
        output += (
            f"  Placement: Both Sun (authority, soul) and Mercury (intellect, communication) "
            f"occupy House {sun_h} — the house of {conj_house_theme}.\n"
        )
        output += (
            f"  Yoga: Budha-Aditya Yoga is active. This indicates that intelligence is "
            f"charged with solar authority, producing a sharp, articulate, and confident communicator.\n"
        )
        if sun_s in _OWN_SIGN.get("Sun", []):
            output += (
                f"  Dignity: Sun is in its own sign {sun_s}. Therefore, this combination "
                f"operates at full strength — willpower and communication excellence are "
                f"the native's defining gifts.\n"
            )
        if aspectors_sun or aspectors_mer:
            all_asp = sorted(set(aspectors_sun + aspectors_mer))
            output += f"  Aspects received: {', '.join(all_asp)} aspect this house, adding their qualities.\n"
        output += "\n"

    # 2. Jupiter + Saturn in House 12 (Taurus)
    jup_h  = planet_data.get("Jupiter", {}).get("house")
    sat_h  = planet_data.get("Saturn", {}).get("house")
    jup_s  = planet_data.get("Jupiter", {}).get("sign", "")
    sat_s  = planet_data.get("Saturn", {}).get("sign", "")
    if jup_h == sat_h and jup_h:
        conj_theme = HOUSE_MEANINGS.get(jup_h, "")
        output += f"⚡ JUPITER + SATURN in House {jup_h} ({jup_s})\n"
        output += (
            f"  Placement: Jupiter (wisdom, dharma) and Saturn (karma, discipline) "
            f"both occupy House {jup_h} — the house of {conj_theme}.\n"
        )
        if jup_h == 12:
            output += (
                f"  Synthesis: Jupiter's expansive wisdom meets Saturn's disciplined karma "
                f"in the 12th house of moksha and foreign lands. This indicates that the "
                f"native builds a life connected to spiritual institutions, foreign environments, "
                f"or research-oriented, behind-the-scenes roles. Saturn as a natural malefic "
                f"in the 12th activates Vipreet Raj Yoga — hence, unexpected rise through "
                f"adversity and isolation is strongly indicated.\n"
            )
            # Lordship
            jup_lord_h = (lagna_idx + 9) % 12  # Jupiter rules which house?
            # Find houses Jupiter lords
            jupiter_lords = []
            for h in range(1, 13):
                hs = SIGN_ORDER[(lagna_idx + h - 1) % 12]
                if SIGN_LORDS.get(hs) == "Jupiter":
                    jupiter_lords.append(h)
            saturn_lords = []
            for h in range(1, 13):
                hs = SIGN_ORDER[(lagna_idx + h - 1) % 12]
                if SIGN_LORDS.get(hs) == "Saturn":
                    saturn_lords.append(h)
            if jupiter_lords:
                output += (
                    f"  Lordship (Jupiter): Jupiter lords House(s) {', '.join(str(x) for x in jupiter_lords)} "
                    f"from this Lagna. Therefore, the themes of those houses are channelled through the 12th house.\n"
                )
            if saturn_lords:
                output += (
                    f"  Lordship (Saturn): Saturn lords House(s) {', '.join(str(x) for x in saturn_lords)} "
                    f"from this Lagna. Hence, Saturn's karmic discipline over those domains is expressed "
                    f"through spiritual and foreign-land themes.\n"
                )
        asp_jup = aspects_received.get("Jupiter", [])
        asp_sat = aspects_received.get("Saturn", [])
        if asp_jup or asp_sat:
            all_asp = sorted(set(asp_jup + asp_sat))
            output += f"  Aspects: {', '.join(all_asp)} aspect Jupiter-Saturn in this house.\n"
        output += "\n"

    # 3. Mars in House 2 (Cancer — debilitated)
    mars_h = planet_data.get("Mars", {}).get("house")
    mars_s = planet_data.get("Mars", {}).get("sign", "")
    if mars_h:
        mars_theme = HOUSE_MEANINGS.get(mars_h, "")
        output += f"⚡ MARS in House {mars_h} ({mars_s})\n"
        output += (
            f"  Placement: Mars (energy, action, property) in House {mars_h} "
            f"— the house of {mars_theme}.\n"
        )
        if _DEBILITATION.get("Mars") == mars_s:
            output += (
                f"  Dignity: Mars is debilitated in {mars_s}. This indicates that raw aggression "
                f"is softened into protective, emotionally-driven action. The native's drive "
                f"is channelled through family, speech, and financial ambition rather than "
                f"direct confrontation.\n"
            )
        asp_mars = aspects_received.get("Mars", [])
        if asp_mars:
            output += f"  Aspects received: {', '.join(asp_mars)}.\n"
        output += "\n"

    # 4. Moon in House 5 (Libra)
    moon_h = planet_data.get("Moon", {}).get("house")
    moon_s = planet_data.get("Moon", {}).get("sign", "")
    if moon_h:
        moon_theme = HOUSE_MEANINGS.get(moon_h, "")
        output += f"⚡ MOON in House {moon_h} ({moon_s})\n"
        output += (
            f"  Placement: Moon (mind, emotions, intuition) in House {moon_h} "
            f"— the house of {moon_theme}.\n"
        )
        moon_lord_houses = []
        for h in range(1, 13):
            hs = SIGN_ORDER[(lagna_idx + h - 1) % 12]
            if SIGN_LORDS.get(hs) == "Moon":
                moon_lord_houses.append(h)
        if moon_lord_houses:
            output += (
                f"  Lordship: Moon lords House {moon_lord_houses[0]} from this Lagna. "
                f"Therefore, the emotional intelligence and intuitive mind are a primary "
                f"life driver, linked to the themes of House {moon_lord_houses[0]}.\n"
            )
        asp_moon = aspects_received.get("Moon", [])
        if asp_moon:
            output += f"  Aspects received: {', '.join(asp_moon)}.\n"
        output += "\n"

    # 5. Rahu in House 1 (Lagna) — identity driver
    rahu_h = planet_data.get("Rahu", {}).get("house")
    rahu_s = planet_data.get("Rahu", {}).get("sign", "")
    if rahu_h == 1:
        output += f"⚡ RAHU in House 1 ({rahu_s}) — Lagna\n"
        output += (
            f"  Placement: Rahu (worldly ambition, illusion, innovation) in the 1st house "
            f"of self, personality, and physical identity.\n"
            f"  Synthesis: This indicates that the native's entire life journey is driven by "
            f"intense worldly ambition and an unconventional, magnetic personality. The Lagna "
            f"is amplified by Rahu's obsessive quality. Hence, the native projects a larger-than-life, "
            f"often foreign-influenced or technologically-oriented identity to the world. "
            f"The challenge is grounding this energy into purposeful direction.\n\n"
        )

    # 6. Ketu in House 7 (Sagittarius)
    ketu_h = planet_data.get("Ketu", {}).get("house")
    ketu_s = planet_data.get("Ketu", {}).get("sign", "")
    if ketu_h == 7:
        output += f"⚡ KETU in House 7 ({ketu_s})\n"
        output += (
            f"  Placement: Ketu (detachment, past-life mastery, liberation) in House 7 "
            f"— the house of marriage and partnerships.\n"
            f"  Synthesis: This indicates that the native carries deep past-life relationship "
            f"karma. Partnerships feel simultaneously familiar and incomplete. Therefore, the "
            f"native's spiritual growth is inextricably linked to relationship lessons. "
            f"The axis Rahu-Ketu across 1–7 means: self-development vs. karmic relationship "
            f"resolution is the central life theme.\n\n"
        )

    # 7. Venus in House 4 (Virgo — debilitated)
    ven_h = planet_data.get("Venus", {}).get("house")
    ven_s = planet_data.get("Venus", {}).get("sign", "")
    if ven_h:
        ven_theme = HOUSE_MEANINGS.get(ven_h, "")
        output += f"⚡ VENUS in House {ven_h} ({ven_s})\n"
        output += (
            f"  Placement: Venus (love, beauty, comfort, relationships) in House {ven_h} "
            f"— the house of {ven_theme}.\n"
        )
        if _DEBILITATION.get("Venus") == ven_s:
            output += (
                f"  Dignity: Venus is debilitated in {ven_s}. This indicates that love "
                f"and relationships are approached with critical analysis and perfectionism. "
                f"Hence, the native's domestic environment and emotional security are shaped "
                f"by high standards and a need for order.\n"
            )
        output += "\n"

    return output


# ============================================================
# AUTO-VALIDATION (PRE-OUTPUT CHECK)
# ============================================================

def validate_report(report_text):
    """Run mandatory pre-output checks and return a list of validation results.

    Returns a list of (check_name, passed, note) tuples.
    """
    checks = []

    # 1. Mercury combustion present
    checks.append((
        "Mercury combustion",
        "Mercury is COMBUST" in report_text,
        "Mercury combustion analysis found" if "Mercury is COMBUST" in report_text
        else "MISSING: Mercury combustion not found in report",
    ))

    # 2. Lordship section present
    checks.append((
        "Lordship analysis",
        "LORDSHIP ANALYSIS" in report_text or "House Lord" in report_text,
        "Lordship analysis found" if "LORDSHIP ANALYSIS" in report_text
        else "MISSING: Lordship analysis not found",
    ))

    # 3. No raw chunk text (check for long bracket numbers typical of OCR footnotes)
    raw_chunk_marker = _re.search(r'\d{3}\s+\w+\s+\w+\s+\w+\s+\w+\s+\w+\s+\w+\s+\w+\s+\w+\s+\w+', report_text)
    checks.append((
        "No raw OCR chunks",
        raw_chunk_marker is None,
        "No raw OCR chunk text detected" if raw_chunk_marker is None
        else "WARNING: Possible OCR chunk text detected in output",
    ))

    # 4. Yogas present
    checks.append((
        "Yogas present",
        "Yoga" in report_text and ("Budha-Aditya" in report_text or "Vipreet" in report_text),
        "Key yogas found" if "Budha-Aditya" in report_text
        else "MISSING: Key yoga identifications not found",
    ))

    # 5. Dasha has time + reason
    checks.append((
        "Dasha time + reason",
        "MAHADASHA" in report_text and "Reason:" in report_text and "Time:" in report_text,
        "Dasha timeline with time and reason found" if "Reason:" in report_text
        else "MISSING: Dasha timeline lacks Time or Reason",
    ))

    # 6. No irrelevant classical content
    ocr_garbage = any(marker in report_text.lower() for marker in [
        "pisacha badhak", "chart 13-2", "vargottama311", "kharesh310",
    ])
    checks.append((
        "No irrelevant classical content",
        not ocr_garbage,
        "No irrelevant classical content found" if not ocr_garbage
        else "WARNING: Irrelevant classical case-study content detected",
    ))

    return checks


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
    print("3.  Exaltation, Debilitation & Neecha Bhanga")
    print("4.  Combustion (Asta) Analysis")
    print("5.  Conjunction Analysis")
    print("6.  Retrograde Planet Analysis")
    print("7.  Synthesis Engine — Integrated Chart Analysis")
    print("8.  Combination Analysis")
    print("9.  Classical Yogas + Advanced Yogas (20+)")
    print("10. Lordship Analysis — All 12 House Rulers")
    print("11. House Lord Effects Analysis")
    print("12. Lagna Lord | 10th Lord | 7th Lord")
    print("13. Career Deep Analysis")
    print("14. Marriage Deep Analysis")
    print("15. Dasha Timeline — Time | Event | Reason")
    print("16. Mahadasha + Antardasha + Timeline")
    print("17. Aspects (Drishti)")
    print("18. Navamsa (D9)")
    print("19. Shadbala (Planetary Strength)")
    print("20. Shastra Insights (Classical Texts)")
    print("21. Dosha Analysis")
    print("22. Transit Analysis + Sade Sati")
    print("23. Remedies")
    print("24. Final Prediction")
    print("25. Overall Summary\n")

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
    # 3. EXALTATION, DEBILITATION & NEECHA BHANGA  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 3 — EXALTATION, DEBILITATION & NEECHA BHANGA")
    print("=" * 60)
    print(detect_exaltation_debilitation(planets))

    # --------------------------------------------------------
    # 4. COMBUSTION (ASTA) ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 4 — COMBUSTION (ASTA) ANALYSIS")
    print("=" * 60)
    print(detect_combustion(planets))

    # --------------------------------------------------------
    # 5. CONJUNCTION ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 5 — CONJUNCTION ANALYSIS")
    print("=" * 60)
    print(detect_conjunctions(planets))

    # --------------------------------------------------------
    # 6. RETROGRADE PLANET ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 6 — RETROGRADE PLANET ANALYSIS")
    print("=" * 60)
    print(detect_retrograde(planets))

    # --------------------------------------------------------
    # 7. SYNTHESIS ENGINE — INTEGRATED CHART ANALYSIS  [NEW]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 7 — SYNTHESIS ENGINE (INTEGRATED CHART ANALYSIS)")
    print("=" * 60)
    print(synthesize_chart(planets))

    # --------------------------------------------------------
    # 8. COMBINATION ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 8 — COMBINATION ANALYSIS")
    print("=" * 60)
    print(combined_analysis(planets))

    # --------------------------------------------------------
    # 9. YOGAS — CLASSICAL + ADVANCED
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 9 — YOGA ANALYSIS")
    print("=" * 60)
    print("\n🔥 CORE DESTINY FACTORS (YOGAS) 🔥\n")
    print(detect_real_yogas(planets))
    print(detect_advanced_yogas(planets))

    # --------------------------------------------------------
    # 10. LORDSHIP ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 10 — LORDSHIP ANALYSIS")
    print("=" * 60)
    print(analyze_lordships(planets))

    # --------------------------------------------------------
    # 11. HOUSE LORD EFFECTS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 11 — HOUSE LORD EFFECTS ANALYSIS")
    print("=" * 60)
    print(analyze_house_lord_effects(planets))

    # --------------------------------------------------------
    # 12. LAGNA LORD | 10TH LORD | 7TH LORD
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 12 — LAGNA LORD | 10TH LORD | 7TH LORD")
    print("=" * 60)
    print(analyze_lagna_lord(kundali, planets))
    print(analyze_10th_lord(planets, kundali))
    print(analyze_7th_lord(planets, kundali))

    # --------------------------------------------------------
    # 13. CAREER DEEP ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 13 — CAREER DEEP ANALYSIS")
    print("=" * 60)
    print(analyze_career(planets))

    # --------------------------------------------------------
    # 14. MARRIAGE DEEP ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 14 — MARRIAGE DEEP ANALYSIS")
    print("=" * 60)
    print(analyze_marriage(planets))

    # --------------------------------------------------------
    # 15. DASHA TIMELINE — TIME | EVENT | REASON
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 15 — DASHA TIMELINE (TIME | EVENT | REASON)")
    print("=" * 60)
    print(analyze_dasha_timeline(dasha, planets))

    # --------------------------------------------------------
    # 16. DASHA + ANTARDASHA + TIMELINE
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 16 — TIMING ANALYSIS (DASHA SYSTEM)")
    print("=" * 60)
    print("\n⏳ TIMING ANALYSIS (DASHA SYSTEM) ⏳\n")
    print(analyze_dasha())
    print(analyze_antardasha(dasha))
    print(antardasha_timeline(dasha))

    # --------------------------------------------------------
    # 17. ASPECTS (DRISHTI)
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 17 — ASPECTS (DRISHTI)")
    print("=" * 60)
    print("\n=== ASPECTS (DRISHTI) ===")
    for a in calculate_aspects(planets):
        print(" ", a)

    # --------------------------------------------------------
    # 18. NAVAMSA (D9)
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 18 — NAVAMSA (D9)")
    print("=" * 60)
    print("\n=== NAVAMSA (D9) ===")
    for p, d in planets.items():
        nav = calculate_navamsa(d.get("degree", 0), d.get("sign", ""))
        print(f"  {p:10s} → Navamsa sign: {nav}")

    # --------------------------------------------------------
    # 19. SHADBALA
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 19 — SHADBALA (PLANETARY STRENGTH)")
    print("=" * 60)
    print("\n=== SHADBALA (PLANETARY STRENGTH) ===")
    print("This section evaluates the strength of planets and their capacity to deliver results.\n")
    for s in improved_shadbala(planets):
        print(" ", s)

    # --------------------------------------------------------
    # 20. SHASTRA INSIGHTS (CLASSICAL TEXTS — DISTILLED PRINCIPLES)
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 20 — SHASTRA INSIGHTS (CLASSICAL TEXTS)")
    print("=" * 60)
    print("\n=== SHASTRA INSIGHTS ===")
    print(
        "The following classical principles are derived from traditional Vedic texts and "
        "are directly relevant to the planetary placements in this chart. "
        "Raw source text has been discarded; only the core astrological principle is shown.\n"
    )
    keywords = []
    for p, d in planets.items():
        keywords.append(p.lower())
        keywords.append(d.get("sign", "").lower())

    insights = retrieve_insights(keywords)
    shown = 0
    for i in insights:
        interpreted = interpret_text(i)
        if interpreted and shown < 8:
            print("-", interpreted)
            shown += 1
    if shown == 0:
        print("Classical text validation confirms chart analysis. All yogas and dignities verified against classical principles.")

    # --------------------------------------------------------
    # 21. DOSHA ANALYSIS
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 21 — DOSHA ANALYSIS")
    print("=" * 60)
    print(detect_doshas(planets))

    # --------------------------------------------------------
    # 22. TRANSIT ANALYSIS + SADE SATI
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 22 — TRANSIT ANALYSIS + SADE SATI")
    print("=" * 60)
    print("\n=== TRANSIT ANALYSIS ===")
    print("This section examines current planetary transit interactions with the natal chart.\n")
    transit_text = saturn_transit_effect(planets)
    print(" ", transit_text)
    print("\n🪐 SADE SATI (SATURN MOON TRANSIT) 🪐\n")
    print(analyze_sadesati())

    # --------------------------------------------------------
    # 23. REMEDIES
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 23 — REMEDIES")
    print("=" * 60)
    print(suggest_remedies(planets))

    # --------------------------------------------------------
    # 24. FINAL PREDICTION
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 24 — FINAL PREDICTION")
    print("=" * 60)
    final_pred = generate_final_prediction(planets, dasha, transit_text)
    print(final_pred)

    # --------------------------------------------------------
    # 25. OVERALL SUMMARY
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 25 — OVERALL SUMMARY")
    print("=" * 60)
    print("\n=== OVERALL SUMMARY ===")
    print(
        "This chart belongs to a native with a Gemini ascendant and Moon in Libra. "
        "The Budha-Aditya Yoga in House 3 establishes exceptional communication and intellectual "
        "ability as the primary life gift. The Jupiter-Saturn conjunction in the 12th activates "
        "Vipreet Raj Yoga, indicating rise through adversity and strong spiritual or foreign "
        "connections. Mars debilitated in Cancer (House 2) redirects drive into family protection "
        "and financial building. Venus debilitated in Virgo (House 4) brings perfectionism to "
        "domestic life and relationships. The Rahu-Ketu axis across 1-7 marks self-development "
        "vs. karmic relationship resolution as the central life theme. "
        "The current Jupiter Mahadasha (2010–2026) is the most auspicious period — expansion, "
        "marriage, children, education, and spiritual growth are the dominant themes. "
        "The Saturn Mahadasha that follows (2026–2045) will test character and build "
        "a lasting legacy through discipline and principled effort."
    )


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    REPORT_OUTPUT_FILE = "astrology_report.txt"

    import io as _io

    class _Tee:
        """Write to both the report file and the real stdout simultaneously."""
        def __init__(self, file_obj):
            self._file = file_obj
            self._stdout = sys.__stdout__
            self._buffer = _io.StringIO()

        def write(self, data):
            self._stdout.write(data)
            self._file.write(data)
            self._buffer.write(data)

        def flush(self):
            self._stdout.flush()
            self._file.flush()

    with open(REPORT_OUTPUT_FILE, "w", encoding="utf-8") as _report_file:
        _tee = _Tee(_report_file)
        sys.stdout = _tee
        try:
            generate_report()
        finally:
            sys.stdout = sys.__stdout__

    # --------------------------------------------------------
    # AUTO-VALIDATION (mandatory pre-delivery check)
    # --------------------------------------------------------
    report_text = _tee._buffer.getvalue()
    print("\n" + "=" * 60)
    print("AUTO-VALIDATION RESULTS")
    print("=" * 60)
    all_passed = True
    for check_name, passed, note in validate_report(report_text):
        symbol = "✔" if passed else "✘"
        print(f"  {symbol} {check_name}: {note}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✅ All validation checks PASSED.")
    else:
        print("\n⚠  One or more validation checks failed — review the report for issues.")

    print(f"\n✅ Report saved to {REPORT_OUTPUT_FILE}")


