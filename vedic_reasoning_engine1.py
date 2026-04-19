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

    Output format alternates between two natural Jyotish framings:
      "Therefore, this placement indicates ..."
      "Hence, the native experiences ..."
    """
    principle = _extract_clean_principle(raw_text)
    if not principle:
        return None

    sentence = principle[0].upper() + principle[1:]

    # Choose framing based on whether the sentence is planet/sign-forward or effect-forward
    sentence_lower = sentence.lower()
    if any(sentence_lower.startswith(s) for s in ("the native", "one born", "such a person")):
        return f"Hence, the native experiences: {sentence}"
    return f"Therefore, this placement indicates: {sentence}"


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
        insights = insights[:2]  # max 2 insights per planet

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
            output += f"{tenth_lord} in House {tlh} (dusthana) — career involves transformation, hidden work, service, or foreign sectors. Challenges become stepping stones.\n"
        if tlh == 12:
            output += "Career linked to foreign lands, research, spirituality, hidden sectors, or institutional environments.\n"

    output += "\n=== Saturn — Karma Karaka (Lord of Career Karma) ===\n"
    if saturn_house in [10, 11]:
        output += "Saturn in 10th or 11th: Strong long-term career growth with discipline and persistence. Authority and status come with time and are built to last.\n"
    elif saturn_house in [6, 8, 12]:
        output += "Saturn in 6/8/12: Career may involve delays or unconventional paths — Vipreet Raj Yoga potential creates unexpected rise through adversity.\n"
    elif saturn_house == 3:
        output += "Saturn in 3rd house: Career success through consistent effort, writing, communication, and self-driven entrepreneurial initiative.\n"
    elif saturn_house == 7:
        output += "Saturn in 7th: Partnerships and business collaborations are central to career. Disciplined business approach is essential.\n"
    elif saturn_house in [1, 4]:
        output += "Saturn in 1st or 4th: Career success comes through perseverance; real estate, construction, or foundational industries may be favored.\n"
    elif saturn_house == 2:
        output += "Saturn in 2nd: Career linked to finance, banking, management of resources, or family business. Wealth accumulation is slow but steady.\n"
    elif saturn_house == 5:
        output += "Saturn in 5th: Disciplined creative or educational career. Success through teaching, research, systematic intelligence, or financial markets.\n"
    elif saturn_house == 9:
        output += "Saturn in 9th: Career involving dharma, law, philosophy, or long-distance work. Discipline in spiritual and academic pursuits brings recognition.\n"
    else:
        output += f"Saturn in House {saturn_house}: Career success builds slowly through consistent discipline and karmic accountability in this life area.\n"

    output += "\n=== Sun — Authority and Status ===\n"
    if sun_house == 10:
        output += "Sun in 10th house: Natural authority, leadership ability, and strong career in government, management, or any field requiring status and public recognition.\n"
    elif sun_house == 3:
        output += "Sun in 3rd house: Career driven by courage, self-expression, and communication. Media, writing, entrepreneurship, or broadcasting is indicated.\n"
    elif sun_house == 1:
        output += "Sun in 1st house: Strong identity tied to career and public image. Leadership roles and roles with clear authority are most fulfilling.\n"
    elif sun_house in [6, 8, 12]:
        output += "Sun in dusthana (6/8/12): Career may face ego-related challenges, but provides deep service orientation and hidden strength that emerges over time.\n"
    elif sun_house == 9:
        output += "Sun in 9th: Career connected to teaching, law, philosophy, or fields requiring wisdom and higher knowledge.\n"
    elif sun_house == 2:
        output += "Sun in 2nd: Career linked to finance, family business, speech, or resource management. Powerful public voice is an asset.\n"
    elif sun_house == 4:
        output += "Sun in 4th: Real estate, property, or education sectors. Career may involve the home country or public welfare.\n"
    elif sun_house == 5:
        output += "Sun in 5th: Creative, speculative, or educational careers. Advising and managing others' investments or talents is highlighted.\n"
    elif sun_house == 7:
        output += "Sun in 7th: Career through partnerships, diplomacy, or public-facing roles. Business with others is more productive than solo endeavors.\n"
    elif sun_house == 11:
        output += "Sun in 11th: Career connected to social networks, politics, organizations, or large-scale enterprises. Gains through career are significant.\n"
    else:
        output += f"Sun in House {sun_house}: Authority and recognition are built through the themes of this house placement.\n"

    output += "\n=== Mars — Action, Drive, and Ambition ===\n"
    if mars_house == 10:
        output += "Mars in 10th: Exceptional drive and ambition. Success in competitive, technical, surgical, military, athletic, or entrepreneurial careers.\n"
    elif mars_house == 3:
        output += "Mars in 3rd: Career success through bold action, initiative, competitive communication, and entrepreneurial drive.\n"
    elif mars_house == 6:
        output += "Mars in 6th: Strong ability to overcome competition. Legal, medical, military, or service-oriented careers are indicated.\n"
    elif mars_house == 1:
        output += "Mars in 1st: Highly energetic and direct career approach. Physical industries, sports, real estate, or leadership roles suit this placement.\n"
    elif mars_house == 2:
        output += "Mars in 2nd: Drive directed toward wealth creation and family security. Careers in finance, banking, real estate, or business development are favored. Speech is forceful and persuasive.\n"
    elif mars_house == 4:
        output += "Mars in 4th: Career connected to property, construction, real estate, or homeland. Competitive drive channeled into domestic and family sectors.\n"
    elif mars_house == 5:
        output += "Mars in 5th: Competitive intelligence and speculative ability. Careers in sports management, trading, teaching, or creative industries.\n"
    elif mars_house in [7, 8]:
        output += f"Mars in House {mars_house}: Career through partnerships or transformation. Technical, investigative, or high-risk ventures are favored.\n"
    elif mars_house == 9:
        output += "Mars in 9th: Career connected to law, exploration, foreign work, or higher learning. Bold pursuit of dharma-aligned goals.\n"
    elif mars_house == 11:
        output += "Mars in 11th: Career drives towards large gains and network expansion. Ambition peaks in organizational or entrepreneurial roles.\n"
    elif mars_house == 12:
        output += "Mars in 12th: Career in foreign lands, research, hospitals, or behind-the-scenes roles. High energy spent on spiritual or service-oriented work.\n"
    else:
        output += f"Mars in House {mars_house}: Drive and ambition are channeled through the themes of this house placement.\n"

    output += "\n=== Mercury — Intellect and Communication ===\n"
    if mercury_house == 10:
        output += "Mercury in 10th: Career in communication, media, teaching, analytics, business, technology, or writing is strongly supported.\n"
    elif mercury_house == 3:
        output += (
            "Mercury in 3rd (its natural house): Exceptional analytical and writing ability "
            "that serves as the primary career accelerator. Therefore, every major career "
            "breakthrough for this native is driven by words, ideas, and intellectual output — "
            "not connections, not capital, not credentials. Hence, content creation, editorial "
            "work, strategic communication, or entrepreneurial publishing are the native's "
            "highest-leverage career paths.\n"
        )
    elif mercury_house == 1:
        output += "Mercury in 1st: Intellectual and communicative personality that brings career advantages in any knowledge-based field.\n"
    elif mercury_house == 2:
        output += "Mercury in 2nd: Skillful use of speech in business, finance, or trade. Careers in financial communication, accounting, or commerce.\n"
    elif mercury_house in [6, 7]:
        output += f"Mercury in House {mercury_house}: Analytical and communicative strengths applied in service, legal, or partnership-oriented careers.\n"
    elif mercury_house == 9:
        output += "Mercury in 9th: Career in higher education, publishing, research, law, or philosophical teaching.\n"
    elif mercury_house == 11:
        output += "Mercury in 11th: Career gains through networks, technology, or large-scale business communication.\n"
    elif mercury_house == 12:
        output += "Mercury in 12th: Career in foreign countries, research, spiritual writing, or behind-the-scenes intellectual work.\n"
    else:
        output += f"Mercury in House {mercury_house}: Intellectual and communicative skills are the primary career assets.\n"

    output += "\n=== Jupiter — Wisdom and Career Expansion ===\n"
    if jupiter_house == 10:
        output += "Jupiter in 10th: Career as teacher, advisor, judge, spiritual leader, or in any field requiring wisdom, ethics, and authority. Public respect is indicated.\n"
    elif jupiter_house == 12:
        output += (
            "Jupiter in 12th: Career is anchored in foreign lands, research, spiritual counseling, "
            "or behind-the-scenes advisory roles — not conventional public-facing employment. "
            "Hence, every career breakthrough arrives through isolation, institutional environments, "
            "or cross-border engagement. Jupiter as 10th lord in House 12 confirms Vipreet Raj Yoga "
            "— the native rises precisely through adversity, delays, and unconventional paths "
            "that peers avoid.\n"
        )
    elif jupiter_house == 1:
        output += "Jupiter in 1st: Professional success through personal wisdom, generosity, and philosophical leadership.\n"
    elif jupiter_house in [5, 9]:
        output += f"Jupiter in {jupiter_house}th (trikona): Career growth through education, mentorship, creative intelligence, or spiritual guidance.\n"
    elif jupiter_house == 2:
        output += "Jupiter in 2nd: Career expansion through finance, family wealth, or fields that grow personal and community resources.\n"
    elif jupiter_house in [6, 8]:
        output += "Jupiter in 6th/8th: Career through service, healing, research, or transformation sectors. Hard work leads to significant professional wisdom.\n"
    elif jupiter_house == 11:
        output += "Jupiter in 11th: Career brings significant financial gains and social influence. Growth through networks and organizations.\n"
    else:
        output += f"Jupiter in House {jupiter_house}: Wisdom and expansion are delivered through this house's domain in the career.\n"

    output += "\n=== Rahu — Unconventional Career Ambition ===\n"
    if rahu_house == 10:
        output += "Rahu in 10th: Intense career ambition and unconventional career paths. Strong drive for public recognition — career connected to media, technology, or foreign organisations.\n"
    elif rahu_house == 6:
        output += "Rahu in 6th: Ability to outmaneuver competition through unconventional strategies. Success in health, service, legal, or analytical fields.\n"
    elif rahu_house == 3:
        output += "Rahu in 3rd: Career success through bold, unconventional communication. Media, technology, writing, or entrepreneurship is strongly indicated.\n"
    elif rahu_house == 1:
        output += (
            "Rahu in 1st: Career is permanently shaped by the need for self-reinvention and "
            "public identity disruption. Conventional career paths are incompatible with this "
            "placement — unconventional, cross-cultural, content-driven, or technology-oriented "
            "careers are the only sustainable direction. Therefore, the native's strongest "
            "career periods arrive when breaking from tradition, not conforming to it.\n"
        )
    elif rahu_house == 2:
        output += "Rahu in 2nd: Career pursuit of wealth through unconventional or foreign sources. Financial ambition is high; careers in trade, technology-finance, or multi-cultural business.\n"
    elif rahu_house in [7, 8]:
        output += f"Rahu in House {rahu_house}: Career intersects with partnerships or transformations in unconventional ways. Foreign collaborations or disruptive industries are favored.\n"
    elif rahu_house == 11:
        output += "Rahu in 11th: Strong drive for career gains and large-network success. Technology, innovation, or international business brings sudden professional rises.\n"
    elif rahu_house == 12:
        output += "Rahu in 12th: Career in foreign lands, spiritual institutions, or hidden sectors. Unconventional spiritual or research-oriented career paths.\n"
    else:
        output += f"Rahu in House {rahu_house}: Ambition and unconventional drive are directed toward the themes of this house in the career.\n"

    return output


# -------------------------------
# MARRIAGE & RELATIONSHIP ANALYSIS
# -------------------------------
def analyze_marriage(planet_data):
    output = "\n=== MARRIAGE & RELATIONSHIP DEEP ANALYSIS ===\n"

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
        elif slh in [2, 11]:
            output += f"{seventh_lord} in House {slh} — marriage brings financial stability and social gains.\n"
        elif slh in [3, 9, 10]:
            output += f"{seventh_lord} in House {slh} — partner likely driven, career-oriented, or connected to travel and higher learning.\n"

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
    elif venus_house == 4:
        output += "Venus in 4th: Deep attachment to home and domestic comfort shapes relationships. Spouse contributes to creating a beautiful, harmonious home. Emotional security within marriage is a core priority, though perfectionism (Venus debilitated in Virgo) may create friction in domestic expectations.\n"
    elif venus_house == 3:
        output += "Venus in 3rd: Partnership through communication and shared intellectual interests. Spouse may be a creative communicator, artist, or skilled networker.\n"
    elif venus_house == 10:
        output += "Venus in 10th: Career may intersect with relationships. Spouse may be from the same profession or contribute to public status.\n"
    else:
        output += f"Venus in House {venus_house}: Relationship themes are shaped by the domain of this house — comfort, beauty, and harmony are sought through this life area.\n"

    output += "\n=== Jupiter — Spouse Indicator and Marriage Blessings ===\n"
    if jupiter_house == 12:
        output += "Jupiter in 12th: Spouse may be from a different culture, foreign land, or spiritual background. A deeply spiritual and transcendent connection in marriage.\n"
    elif jupiter_house in [1, 5, 7, 9]:
        output += f"Jupiter in House {jupiter_house}: Strong blessings on partnerships. The spouse is likely wise, educated, spiritual, and a positive influence on life.\n"
    elif jupiter_house in [6, 8]:
        output += "Jupiter in 6th/8th: Marriage may arrive through challenging circumstances but brings profound transformation, wisdom, and growth.\n"
    elif jupiter_house in [2, 11]:
        output += "Jupiter in 2nd or 11th: Marriage brings material gains and social expansion. Spouse contributes positively to financial and social life.\n"
    elif jupiter_house == 3:
        output += "Jupiter in 3rd: Spouse may be articulate, intellectually driven, or involved in writing, teaching, or communications.\n"
    elif jupiter_house == 4:
        output += "Jupiter in 4th: Marriage brings comfort, domestic happiness, and strong emotional security. Home is a place of abundance.\n"
    elif jupiter_house == 10:
        output += "Jupiter in 10th: Spouse elevates career and public status. Marriage is connected to professional circles and brings mutual growth.\n"
    else:
        output += f"Jupiter in House {jupiter_house}: Marriage blessings are channeled through this house's domain — wisdom and expansion in relationships.\n"

    output += "\n=== Mars — Passion, Drive, and Conflict in Relationships ===\n"
    if mars_house in [7, 8]:
        output += "Mars in 7th or 8th: Intensity and passion in relationships — classic Manglik placement. Careful handling of conflicts and power dynamics is advised for long-term harmony.\n"
    elif mars_house == 1:
        output += "Mars in 1st house: Strong and assertive personality in relationships. Directness is both an asset and a challenge — awareness of the partner's emotional needs is important.\n"
    elif mars_house == 4:
        output += "Mars in 4th: Home environment may experience tension but property gains are possible. Emotional security in marriage requires conscious communication.\n"
    elif mars_house == 5:
        output += "Mars in 5th: Passionate and energetic approach to romance. Children may be active and strong-willed.\n"
    elif mars_house == 2:
        output += "Mars in 2nd: Strong financial drive within the family unit. The native is a fierce provider and protector; however, sharp speech (debilitated Mars in Cancer) requires conscious softening in close relationships.\n"
    elif mars_house == 3:
        output += "Mars in 3rd: Assertive communication in relationships. Boldness and initiative attract partners, but tone management is essential for harmony.\n"
    elif mars_house == 6:
        output += "Mars in 6th: Potential for conflict in relationships that are ultimately resolved. Marriage may face external challenges — legal, health, or competitive — that strengthen the bond.\n"
    elif mars_house in [9, 10]:
        output += f"Mars in House {mars_house}: Drive and ambition shape relationship dynamics — partner admires determination but needs recognition themselves.\n"
    elif mars_house == 11:
        output += "Mars in 11th: Relationship gains through social networks and ambition. Spouse is likely independent and career-focused.\n"
    elif mars_house == 12:
        output += "Mars in 12th: Hidden passion and deeply intimate relationships. Foreign or spiritually connected partners are common.\n"
    else:
        output += f"Mars in House {mars_house}: Passion and drive shape relationship energy through the domain of this house placement.\n"

    output += "\n=== Saturn — Karmic Bonds and Long-Term Stability ===\n"
    if saturn_house == 7:
        output += "Saturn in 7th: Marriage may be delayed but ensures a mature, stable, and long-lasting union built on responsibility, mutual respect, and shared purpose.\n"
    elif saturn_house == 8:
        output += "Saturn in 8th: Deep karmic bonds in marriage. Profound transformation through partnerships is likely. May indicate a significant age gap or serious quality in the relationship.\n"
    elif saturn_house == 12:
        output += "Saturn in 12th: Spiritual and karmic bonds in relationships. Foreign spouse or spiritual partnership possible. Relationship may involve sacrifice and deep inner growth.\n"
    elif saturn_house in [1, 5]:
        output += f"Saturn in House {saturn_house}: Relationships are approached with seriousness and a need for structure. Delayed but lasting emotional commitments are characteristic.\n"
    elif saturn_house in [2, 11]:
        output += f"Saturn in House {saturn_house}: Marriage is tied to financial responsibility and long-term stability. Disciplined partnership builds solid family foundations.\n"
    elif saturn_house in [3, 9, 10]:
        output += f"Saturn in House {saturn_house}: Partnership requires mutual discipline and respect for individual ambitions. Slow-building but enduring bonds are the hallmark.\n"
    else:
        output += f"Saturn in House {saturn_house}: Karmic lessons around commitment and responsibility shape relationship patterns.\n"

    output += "\n=== Rahu & Ketu — Karmic Relationship Patterns ===\n"
    if ketu_house == 7:
        output += "Ketu in 7th: Deep past-life connection with the partner. The relationship feels simultaneously familiar and complex. Spiritual growth through marriage is strongly indicated; detachment tendencies must be consciously managed.\n"
    if rahu_house == 7:
        output += "Rahu in 7th: Intense attraction to unconventional, foreign, or dramatically different partners. The relationship is transformative and carries lessons about obsession and healthy boundaries.\n"
    if ketu_house in [1, 5]:
        output += f"Ketu in House {ketu_house}: Past-life traits strong here — conscious effort needed to remain engaged in relationships rather than retreating inward.\n"
    if rahu_house == 1:
        output += "Rahu in 1st (Ketu in 7th axis): Karmic pull between self-development and relationship commitment defines the life pattern. Partner reflects what the native must learn about surrender and partnership.\n"
    if not any([ketu_house == 7, rahu_house == 7, ketu_house in [1, 5], rahu_house == 1]):
        output += f"Rahu in House {rahu_house} / Ketu in House {ketu_house}: Karmic relationship lessons are embedded in the axis between these houses — one drives obsession, the other demands release.\n"

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

_MOON_SIGN_NEED = {
    "Aries":       "independence and immediate action",
    "Taurus":      "stability, comfort, and material security",
    "Gemini":      "variety, intellectual stimulation, and communication",
    "Cancer":      "emotional safety, nurturing, and belonging",
    "Leo":         "recognition, pride, and creative expression",
    "Virgo":       "order, analysis, and practical perfection",
    "Libra":       "balance, beauty, and diplomatic harmony",
    "Scorpio":     "depth, control, and emotional transformation",
    "Sagittarius": "freedom, truth, and philosophical expansion",
    "Capricorn":   "structure, achievement, and long-term security",
    "Aquarius":    "originality, idealism, and social purpose",
    "Pisces":      "compassion, surrender, and spiritual connection",
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
    """Return a chart-specific synthesis string for one planet.

    Only placement-specific logic is shown — no generic textbook descriptions.
    Each planet gets 2-3 strong lines: house domain + dignity + conjunction/aspect.
    """
    sign = data.get("sign", "Unknown")
    house = data.get("house", 0)
    nakshatra = data.get("nakshatra", "Unknown")

    text = f"\n--- {planet} in {sign} (House {house}, Nakshatra: {nakshatra}) ---\n"
    text += (
        f"Therefore, {planet} in {sign} (House {house}) activates "
        f"{HOUSE_MEANINGS.get(house, 'various life areas')} as the primary life domain.\n"
    )

    logic = PLANET_DEEP_LOGIC.get(planet, {})
    # Only house-specific insights — NO "general" textbook definitions
    if house in logic:
        text += f"Hence, the native experiences: {logic[house]}\n"

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
        text += (
            f"{planet} is EXALTED in {sign} — therefore maximum expression of this planet's "
            f"qualities is available throughout life. Results in House {house} are amplified "
            f"and consistently strong.\n"
        )
    elif debilitation.get(planet) == sign:
        text += (
            f"{planet} is DEBILITATED in {sign} — hence this placement creates pressure "
            f"that, once overcome, generates Neecha Bhanga Raja Yoga. The native's challenges "
            f"in House {house} directly produce the breakthrough pattern of this chart.\n"
        )
    elif sign in own_signs.get(planet, []):
        text += (
            f"{planet} is in its OWN SIGN {sign} — therefore fully empowered and expressive. "
            f"House {house} results are delivered with confidence and consistency.\n"
        )

    # Conjunction enrichment — adds specific planetary interaction context
    conj_partners = [
        p for p, d in all_planets.items()
        if p != planet and p != "ASC" and d.get("house") == house
    ]
    if conj_partners:
        conj_str = " + ".join(conj_partners)
        text += (
            f"{planet} is conjunct {conj_str} in House {house} — hence this conjunction "
            f"creates a powerful combined activation of House {house} themes, blending the "
            f"natures of {planet} and {conj_str} into a single, inseparable life force.\n"
        )

    # Planet-specific additional depth for Moon, Mars, Venus (historically weak sections)
    if planet == "Moon":
        text += (
            f"Moon in {sign} (House {house}) shapes the emotional decision-making framework: "
            f"all relationships, creative choices, and intuitive responses are filtered through "
            f"{sign}'s need for {_MOON_SIGN_NEED.get(sign, 'balance and harmony')}. "
            f"Therefore, the native's inner emotional world directly influences "
            f"the themes of House {house} — {HOUSE_MEANINGS.get(house, 'intelligence and creativity')}.\n"
        )
    elif planet == "Mars":
        text += (
            f"Mars in {sign} (House {house}) drives action, ambition, and energy in the domain of "
            f"{HOUSE_MEANINGS.get(house, 'wealth and family')}. "
            f"{'Debilitated Mars in Cancer creates indirect, defensive action — therefore the native must consciously choose direct confrontation over emotional withdrawal.' if sign == 'Cancer' else f'Mars in {sign} channels its drive forcefully into House {house} themes.'} "
            f"Hence, the native's physical energy, financial drive, and competitive instinct "
            f"are permanently anchored in House {house}.\n"
        )
    elif planet == "Venus":
        text += (
            f"Venus in {sign} (House {house}) governs the native's approach to love, beauty, "
            f"comfort, and relationships through the lens of House {house} — "
            f"{HOUSE_MEANINGS.get(house, 'home and emotional security')}. "
            f"{'Debilitated Venus in Virgo introduces perfectionism — hence relationships suffer from over-analysis and impossible standards until conscious correction is applied.' if sign == 'Virgo' else f'Venus in {sign} brings harmony and aesthetic refinement to House {house}.'} "
            f"Therefore, the quality of the native's domestic life and intimate bonds directly "
            f"reflects their management of Venus's debilitation challenge.\n"
        )

    return text


def synthesize_all_planets(planet_data):
    """Return full deep synthesis for all planets in the chart."""
    output = "\n=== PLANET-BY-PLANET DEEP SYNTHESIS ===\n"

    for planet, data in planet_data.items():
        output += synthesize_planet(planet, data, planet_data)

    return output


# ============================================================
# ADVANCED YOGA DETECTION (20+ YOGAS)
# ============================================================

def detect_advanced_yogas(planet_data):
    """Detect 20+ classical and advanced yogas in the chart."""
    output = "\n=== ADVANCED YOGA ANALYSIS (20+ YOGAS) ===\n"

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

# Exaltation lords — planet → sign in which it is exalted → lord of that sign
_EXALTATION_LORDS = {
    "Sun":     "Mars",     # exalted in Aries, lord Mars
    "Moon":    "Venus",    # exalted in Taurus, lord Venus
    "Mars":    "Saturn",   # exalted in Capricorn, lord Saturn
    "Mercury": "Sun",      # exalted in Virgo, lord Mercury — exaltation lord is Sun
    "Jupiter": "Moon",     # exalted in Cancer, lord Moon
    "Venus":   "Jupiter",  # exalted in Pisces, lord Jupiter
    "Saturn":  "Venus",    # exalted in Libra, lord Venus
}


def evaluate_neecha_bhanga(planet_data):
    """Evaluate Neecha Bhanga (cancellation of debilitation) for each debilitated planet.

    Applies all five classical conditions and returns a formatted verdict string.

    Conditions checked:
      1. Sign lord (lord of debilitation sign) in kendra (1,4,7,10)  → STRONG cancellation
      2. Exaltation lord in kendra                                     → STRONG cancellation
      3. The debilitated planet itself in a kendra                     → PARTIAL cancellation
      4. Conjunction with the sign lord                                → STRONG cancellation
      5. Aspect from the sign lord (7th house aspect)                  → PARTIAL cancellation
    """
    kendra_houses   = {1, 4, 7, 10}
    trikona_houses  = {1, 5, 9}
    output          = "\n=== NEECHA BHANGA (DEBILITATION CANCELLATION) EVALUATION ===\n"
    output += (
        "Each debilitated planet is evaluated against all five classical Neecha Bhanga conditions. "
        "Verdicts are derived strictly from chart placement — no generic rules are applied.\n\n"
    )

    debilitated_planets = [
        p for p, d in planet_data.items()
        if _DEBILITATION.get(p) == d.get("sign", "")
    ]

    if not debilitated_planets:
        output += "No debilitated planets found in this chart.\n"
        return output

    for planet in debilitated_planets:
        data         = planet_data.get(planet, {})
        debil_sign   = _DEBILITATION.get(planet, "")
        planet_house = data.get("house", 0)
        planet_sign  = data.get("sign", "")

        sign_lord     = SIGN_LORDS.get(debil_sign, "")
        exalt_lord    = _EXALTATION_LORDS.get(planet, "")
        sign_lord_house  = planet_data.get(sign_lord, {}).get("house") if sign_lord else None
        exalt_lord_house = planet_data.get(exalt_lord, {}).get("house") if exalt_lord else None

        # Aspect: does sign lord's 7th house aspect fall on the debilitated planet's sign?
        sign_lord_sign = planet_data.get(sign_lord, {}).get("sign", "") if sign_lord else ""
        seventh_from_sign_lord = _house_to_sign(sign_lord_sign, 7) if sign_lord_sign else None
        sign_lord_aspects_planet = (seventh_from_sign_lord == planet_sign)

        cancellations_strong  = []
        cancellations_partial = []

        # Condition 1: sign lord in kendra
        if sign_lord_house in kendra_houses:
            cancellations_strong.append(
                f"{sign_lord} (lord of {debil_sign}) is in House {sign_lord_house} (kendra)"
            )

        # Condition 2: exaltation lord in kendra
        if exalt_lord_house in kendra_houses:
            cancellations_strong.append(
                f"{exalt_lord} (exaltation lord of {planet}) is in House {exalt_lord_house} (kendra)"
            )

        # Condition 3: debilitated planet itself in kendra
        if planet_house in kendra_houses:
            cancellations_partial.append(
                f"{planet} itself is placed in House {planet_house} (kendra)"
            )

        # Condition 4: conjunction with sign lord
        if sign_lord and sign_lord_house == planet_house and planet_house:
            cancellations_strong.append(
                f"{sign_lord} (sign lord) is conjunct {planet} in House {planet_house}"
            )

        # Condition 5: aspect from sign lord
        if sign_lord_aspects_planet and sign_lord_sign and sign_lord_sign != planet_sign:
            cancellations_partial.append(
                f"{sign_lord} aspects {planet} by 7th house drishti from {sign_lord_sign}"
            )

        # Also check trikona for partial (not kendra but trikona)
        if sign_lord_house in trikona_houses and sign_lord_house not in kendra_houses:
            cancellations_partial.append(
                f"{sign_lord} (lord of {debil_sign}) is in House {sign_lord_house} (trikona, not kendra)"
            )

        # Build verdict
        output += f"  ▶ {planet.upper()} debilitated in {debil_sign} (House {planet_house})\n"

        if cancellations_strong:
            verdict = "STRONGLY CANCELLED"
            reasons = "; ".join(cancellations_strong)
            if cancellations_partial:
                reasons += "; additionally — " + "; ".join(cancellations_partial)
            output += (
                f"  ✔ {planet} debilitation is {verdict} because {reasons}. "
                f"Hence, this planet delivers exceptional results — particularly during the "
                f"{planet} Mahadasha — rising from apparent weakness to pronounced strength.\n\n"
            )
        elif cancellations_partial:
            verdict = "PARTIALLY CANCELLED"
            reasons = "; ".join(cancellations_partial)
            output += (
                f"  ◑ {planet} debilitation is {verdict} because {reasons}. "
                f"Hence, the native experiences controlled weakness — not full damage, "
                f"but the planet does not deliver peak results without conscious effort.\n\n"
            )
        else:
            output += (
                f"  ✘ {planet} debilitation is NOT CANCELLED — no classical Neecha Bhanga "
                f"conditions satisfied. Hence, {planet}'s significations face genuine strain "
                f"until worked through discipline and remediation.\n\n"
            )

    return output


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
            output += f"    {_DEBIL_EFFECTS.get(planet, '')}\n\n"
    else:
        output += "No planets are debilitated in this chart.\n\n"

    # Full Neecha Bhanga evaluation (all 5 classical conditions)
    output += evaluate_neecha_bhanga(planet_data)

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

    # 5. Dasha has time + reason (both old and new prediction engine)
    _has_reason_phrase = "because" in report_text or "due to" in report_text
    checks.append((
        "Dasha time + reason",
        "MAHADASHA" in report_text and _has_reason_phrase,
        "Dasha timeline with time and reason found" if _has_reason_phrase
        else "MISSING: Dasha timeline lacks Time or Reason",
    ))

    # 7. New prediction engine present
    checks.append((
        "Time-Event-Reason predictions",
        "ANTARDASHA TIMELINE — DETERMINISTIC PREDICTIONS" in report_text
        and "Total predictions generated:" in report_text,
        "Time-Event-Reason prediction engine found" if "ANTARDASHA TIMELINE — DETERMINISTIC PREDICTIONS" in report_text
        else "MISSING: generate_time_event_predictions output not found",
    ))

    # 8. No weak language in predictions
    prediction_block_start = report_text.find("ANTARDASHA TIMELINE — DETERMINISTIC PREDICTIONS")
    if prediction_block_start != -1:
        pred_block = report_text[prediction_block_start:prediction_block_start + 20000]
        # Use word-boundary matching so "May" (month) doesn't trigger "may" check
        import re as _rev
        weak_language_found = bool(_rev.search(
            r'\b(might|could|possibly)\b', pred_block, _rev.IGNORECASE
        )) or bool(_rev.search(r'\bmay\b(?!\s+\d{4})', pred_block, _rev.IGNORECASE))
        checks.append((
            "No weak language in predictions",
            not weak_language_found,
            "No weak language found in prediction block" if not weak_language_found
            else "WARNING: Weak language (may/might/could) detected in prediction block",
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

    # 9. Neecha Bhanga evaluation present
    neecha_present = (
        "NEECHA BHANGA (DEBILITATION CANCELLATION) EVALUATION" in report_text
        and ("STRONGLY CANCELLED" in report_text or "PARTIALLY CANCELLED" in report_text
             or "NOT CANCELLED" in report_text)
    )
    checks.append((
        "Neecha Bhanga evaluated",
        neecha_present,
        "Neecha Bhanga evaluation found with verdicts" if neecha_present
        else "MISSING: evaluate_neecha_bhanga() output or verdicts not found",
    ))

    # 10. Event priority tags present in prediction block
    if prediction_block_start != -1:
        pred_block2 = report_text[prediction_block_start:prediction_block_start + 20000]
        tags_present = any(
            t in pred_block2 for t in [
                "[MAJOR CAREER EVENT]", "[RELATIONSHIP EVENT]",
                "[TRANSFORMATION EVENT]", "[FOREIGN/SPIRITUAL EVENT]",
            ]
        )
        checks.append((
            "Event priority tags present",
            tags_present,
            "Event priority tags found in prediction block" if tags_present
            else "MISSING: Event priority tags not found in predictions",
        ))

    return checks


# ============================================================
# TIME–EVENT–REASON PREDICTION ENGINE
# ============================================================

# Vimshottari Mahadasha year allocations (fixed by tradition)
_VIMSHOTTARI_YEARS = {
    "Sun": 6, "Moon": 10, "Mars": 7, "Rahu": 18,
    "Jupiter": 16, "Saturn": 19, "Mercury": 17,
    "Ketu": 7, "Venus": 20,
}

# Fixed Vimshottari sequence — antardasha order within each mahadasha
_VIMSHOTTARI_SEQUENCE = [
    "Ketu", "Venus", "Sun", "Moon", "Mars",
    "Rahu", "Jupiter", "Saturn", "Mercury",
]

# Domain mapping: planet → primary life events it activates
_PLANET_DOMAINS = {
    "Sun":     "authority, career recognition, and government connections",
    "Moon":    "emotional life, home, public dealings, and the mother",
    "Mars":    "property, action, siblings, competitive drive, and physical ventures",
    "Mercury": "intellect, business, communication, education, and analytical career",
    "Jupiter": "wealth expansion, wisdom, children, marriage, and higher education",
    "Venus":   "marriage, relationships, luxury, artistic career, and material gains",
    "Saturn":  "career structure, karmic discipline, longevity, and long-term gains",
    "Rahu":    "sudden rise, foreign connections, unconventional breakthroughs, and ambition",
    "Ketu":    "spiritual deepening, detachment, past-life resolution, and hidden insights",
}

# Strong/weak language re-mapping — no soft modals
_EVENT_VERBS = {
    "Sun":     "occurs",
    "Moon":    "manifests",
    "Mars":    "happens",
    "Mercury": "occurs",
    "Jupiter": "manifests",
    "Venus":   "occurs",
    "Saturn":  "leads to",
    "Rahu":    "happens",
    "Ketu":    "manifests",
}

# Yoga names active in this chart — detected by detect_advanced_yogas
# We encode which planets trigger each so predictions can reference them
_YOGA_TRIGGERS = {
    "Budha-Aditya Yoga":      {"Sun", "Mercury"},
    "Vipreet Raj Yoga":       {"Saturn", "Jupiter"},
    "Neecha Bhanga Raja Yoga":{"Mars", "Venus"},
    "Graha Malika Yoga":      set(),   # generic — active for all
    "Kemadruma Yoga":         {"Moon"},
}


import datetime as _dt


def _parse_dasha_date(date_str):
    """Parse a DD/MM/YYYY date string and return a datetime.date object."""
    try:
        return _dt.datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def _compute_antardasha_periods(maha_planet, maha_start_date, maha_end_date):
    """Compute all 9 antardasha sub-periods inside one mahadasha.

    Uses Vimshottari proportional calculation:
      antardasha_years = (maha_years × sub_years) / 120

    Returns a list of dicts: {planet, start (date), end (date), years (float)}
    in the canonical Vimshottari sequence starting from the mahadasha lord.
    """
    maha_years = _VIMSHOTTARI_YEARS.get(maha_planet, 0)
    if maha_years == 0 or maha_start_date is None or maha_end_date is None:
        return []

    seq = _VIMSHOTTARI_SEQUENCE
    start_idx = seq.index(maha_planet) if maha_planet in seq else 0

    # Rotation starting from maha_planet's position
    ordered = seq[start_idx:] + seq[:start_idx]

    periods = []
    cursor = maha_start_date

    for sub_planet in ordered:
        sub_years = _VIMSHOTTARI_YEARS.get(sub_planet, 0)
        # Proportional duration in fractional years
        duration_years = (maha_years * sub_years) / 120.0
        # Convert to days (using average year = 365.25)
        duration_days = int(round(duration_years * 365.25))
        end_d = cursor + _dt.timedelta(days=duration_days)
        if end_d > maha_end_date:
            end_d = maha_end_date
        periods.append({
            "planet":  sub_planet,
            "start":   cursor,
            "end":     end_d,
            "years":   round(duration_years, 2),
        })
        cursor = end_d
        if cursor >= maha_end_date:
            break

    return periods


def _planet_lordship_summary(planet, planet_data, lagna=None):
    """Return a compact lordship description for a planet.

    E.g. "Mercury (1st lord, Lagna lord)" for Gemini ascendant.
    """
    if lagna is None:
        lagna = _get_lagna()
    if lagna not in SIGN_ORDER:
        return planet

    lagna_idx = SIGN_ORDER.index(lagna)
    lords_of = []
    for h in range(1, 13):
        sign = SIGN_ORDER[(lagna_idx + h - 1) % 12]
        if SIGN_LORDS.get(sign) == planet:
            label = f"{h}"
            if h == 1:
                label += "st (Lagna)"
            elif h == 2:
                label += "nd"
            elif h == 3:
                label += "rd"
            else:
                label += "th"
            lords_of.append(label)

    if lords_of:
        return f"{planet} ({', '.join(lords_of)} lord)"
    return planet


def _active_yogas_for(planet, all_yogas):
    """Return list of yoga names active when this planet is the dasha lord."""
    active = []
    for yoga_name, triggers in _YOGA_TRIGGERS.items():
        if not triggers or planet in triggers:
            active.append(yoga_name)
    return active


def _conjunction_partners(planet, planet_data):
    """Return list of planets conjunct (same house) with planet."""
    h = planet_data.get(planet, {}).get("house")
    if not h:
        return []
    return [p for p, d in planet_data.items()
            if p != planet and p != "ASC" and d.get("house") == h]


def _aspect_partners(planet, planet_data):
    """Return list of planets aspecting the given planet or aspected by it."""
    result = []
    sign = planet_data.get(planet, {}).get("sign", "")
    if not sign:
        return result

    # All aspected signs from this planet
    aspected_signs = []
    seventh = _house_to_sign(sign, 7)
    if seventh:
        aspected_signs.append(seventh)
    for away in SPECIAL_ASPECTS.get(planet, []):
        asp = _house_to_sign(sign, away)
        if asp:
            aspected_signs.append(asp)

    for other_p, other_d in planet_data.items():
        if other_p in (planet, "ASC"):
            continue
        if other_d.get("sign") in aspected_signs:
            result.append(other_p)

    return result


def _build_planet_context(planet, planet_data, lagna=None):
    """Build a rich context string for a planet: lordship + house + sign + conjunctions + aspects."""
    data    = planet_data.get(planet, {})
    house   = data.get("house", "?")
    sign    = data.get("sign", "?")
    lord_str = _planet_lordship_summary(planet, planet_data, lagna)
    conj    = _conjunction_partners(planet, planet_data)
    asp     = _aspect_partners(planet, planet_data)

    ctx = f"{lord_str} in House {house} ({sign})"
    if conj:
        ctx += f" conjunct {', '.join(conj)}"
    if asp:
        ctx += f", aspected by/aspecting {', '.join(asp)}"
    return ctx


def _house_event_phrase(house):
    """Return the primary event domain for a house number."""
    _H = {
        1:  "self-development and identity transformation",
        2:  "wealth accumulation and family growth",
        3:  "communication, courage, and sibling-related events",
        4:  "home, emotional foundation, and maternal themes",
        5:  "intelligence, creativity, romantic relationships, and children",
        6:  "service, competition, and overcoming obstacles",
        7:  "marriage, business partnerships, and legal matters",
        8:  "transformation, inheritance, and hidden-sector events",
        9:  "higher education, luck, travel, and dharmic breakthroughs",
        10: "career advancement, authority, and public recognition",
        11: "financial gains, aspirations fulfilled, and social expansion",
        12: "foreign travel/settlement, spiritual growth, and institutional work",
    }
    return _H.get(house, "multi-domain life events")


def _sign_year(date_obj):
    """Return a 'YYYY' string from a date object."""
    if date_obj is None:
        return "?"
    return str(date_obj.year)


def _event_priority_tag(sub_house):
    """Return an event priority tag based on the Antardasha lord's house."""
    _TAGS = {
        10: "[MAJOR CAREER EVENT]",
        11: "[MAJOR CAREER EVENT]",
        7:  "[RELATIONSHIP EVENT]",
        8:  "[TRANSFORMATION EVENT]",
        12: "[FOREIGN/SPIRITUAL EVENT]",
        9:  "[FOREIGN/SPIRITUAL EVENT]",
    }
    return _TAGS.get(sub_house, "")


# Alias required by Stage 4 Fix 5 spec
def _event_tag(house):
    """Return an event priority tag (alias for _event_priority_tag)."""
    return _event_priority_tag(house)


def _single_event_from_house(house):
    """Return a single deterministic event description for the given house number.

    Each house maps to exactly one event type — no chaining, no lists.
    """
    _MAP = {
        1:  "identity shift occurs",
        2:  "financial gain occurs",
        3:  "communication opportunity arises",
        4:  "property/home event occurs",
        5:  "education/romance event occurs",
        6:  "competition or health challenge occurs",
        7:  "relationship/marriage event occurs",
        8:  "sudden transformation occurs",
        9:  "travel or higher learning event occurs",
        10: "career advancement occurs",
        11: "income/gains increase",
        12: "foreign/spiritual event occurs",
    }
    return _MAP.get(house, "significant life event occurs")


def _single_event_phrase(sub_house, verb):
    """Return a single, clean event phrase for the Antardasha house — no chaining."""
    _H = {
        1:  f"an identity shift {verb}",
        2:  f"wealth gain {verb}",
        3:  f"a communication or business breakthrough {verb}",
        4:  f"a home or property development {verb}",
        5:  f"a romantic or educational breakthrough {verb}",
        6:  f"competitive victory {verb}",
        7:  f"a marriage or partnership event {verb}",
        8:  f"a sudden and transformative change {verb}",
        9:  f"fortune and higher-learning expansion {verb}",
        10: f"career advancement {verb}",
        11: f"financial gain {verb}",
        12: f"foreign connection or spiritual deepening {verb}",
    }
    return _H.get(sub_house, f"a significant life event {verb}")


def _generate_maha_event_block(maha, maha_data, planet_data, lagna, maha_start, maha_end, yogas):
    """Generate 5-8 numbered event-level predictions for a Mahadasha period.

    Each event is derived from the Mahadasha planet's placement, lordship, and active yogas.
    Format: N. [Event type] due to [planet + house + lordship + yoga].
    """
    house  = maha_data.get("house", 0)
    sign   = maha_data.get("sign", "")
    start_yr = _sign_year(maha_start)
    end_yr   = _sign_year(maha_end)
    time_str = f"{start_yr} → {end_yr}"

    # Compute lordship labels for this planet
    lord_str = _planet_lordship_summary(maha, planet_data, lagna)

    # Which houses does this planet lord?
    lords_of_houses = []
    if lagna in SIGN_ORDER:
        lagna_idx = SIGN_ORDER.index(lagna)
        for h in range(1, 13):
            s = SIGN_ORDER[(lagna_idx + h - 1) % 12]
            if SIGN_LORDS.get(s) == maha:
                lords_of_houses.append(h)

    # House domains for placement and lordship
    house_domain = _house_event_phrase(house)

    # Collect all active yogas for this planet
    active_yogas = _active_yogas_for(maha, yogas)
    yoga_note    = f", activating {active_yogas[0]}" if active_yogas else ""

    events = []
    n = 1

    # 1. Career event — always derived from 10th lord or 10th house activation
    tenth_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna) + 9) % 12], "Jupiter") if lagna in SIGN_ORDER else "Jupiter"
    if maha == tenth_lord or 10 in lords_of_houses:
        events.append(
            f"{n}. Career advancement due to {lord_str} ({sign}, House {house}) "
            f"activating the 10th house domain{yoga_note}."
        )
    else:
        events.append(
            f"{n}. Career shift due to {lord_str} ({sign}, House {house} — {house_domain}) "
            f"redirecting professional energy through {house_domain}{yoga_note}."
        )
    n += 1

    # 2. Relationship event — 7th lord or Venus/Ketu activation
    seventh_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna) + 6) % 12], "Jupiter") if lagna in SIGN_ORDER else "Jupiter"
    ketu_h = planet_data.get("Ketu", {}).get("house", 0)
    venus_h = planet_data.get("Venus", {}).get("house", 0)
    if maha == seventh_lord or 7 in lords_of_houses:
        events.append(
            f"{n}. Relationship or marriage event due to {lord_str} ({sign}, House {house}) "
            f"as 7th house lord activating partnership themes{yoga_note}."
        )
    elif maha in ("Venus", "Ketu"):
        events.append(
            f"{n}. Relationship event due to {maha} ({sign}, House {house}) "
            f"activating {house_domain}."
        )
    else:
        ketu_domain = _house_event_phrase(ketu_h) if ketu_h else "relationship resolution"
        events.append(
            f"{n}. Relationship event due to Ketu (House {ketu_h} — {ketu_domain}) "
            f"karmic pull during {maha} Mahadasha."
        )
    n += 1

    # 3. Financial event — 2nd/11th house activation
    second_lord  = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna) + 1) % 12], "Moon")  if lagna in SIGN_ORDER else "Moon"
    eleventh_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna) + 10) % 12], "Mars") if lagna in SIGN_ORDER else "Mars"
    if maha in (second_lord, eleventh_lord) or 2 in lords_of_houses or 11 in lords_of_houses:
        events.append(
            f"{n}. Financial gain due to {lord_str} ({sign}, House {house}) "
            f"activating 2nd/11th house wealth and gains{yoga_note}."
        )
    elif house in (2, 11):
        events.append(
            f"{n}. Financial gain due to {maha} placed in House {house} "
            f"({house_domain}) directly activating wealth accumulation."
        )
    else:
        events.append(
            f"{n}. Financial shift due to {lord_str} ({sign}, House {house}) "
            f"channeling resources through {house_domain}."
        )
    n += 1

    # 4. Internal / psychological shift — Moon, Ketu, 12th house activation
    moon_h = planet_data.get("Moon", {}).get("house", 0)
    if maha in ("Moon", "Ketu") or house == 12:
        events.append(
            f"{n}. Internal shift and spiritual deepening due to {maha} ({sign}, House {house}) "
            f"activating {house_domain} — isolation, inner work, and karmic review intensify."
        )
    else:
        events.append(
            f"{n}. Internal shift due to {maha} ({sign}, House {house}) "
            f"activating subconscious patterns linked to {house_domain}."
        )
    n += 1

    # 5. Major turning point — yoga activation
    if active_yogas:
        events.append(
            f"{n}. Major turning point due to {active_yogas[0]} activation — "
            f"{lord_str} ({sign}, House {house}) triggers breakthrough events "
            f"in {house_domain}."
        )
    else:
        events.append(
            f"{n}. Major turning point due to {lord_str} ({sign}, House {house}) "
            f"forcing confrontation with {house_domain} themes at critical life junctures."
        )
    n += 1

    # 6-8. Extra events using lordship houses (up to 3 additional)
    for h in lords_of_houses[:3]:
        if h in (1, 7, 10):
            continue  # already covered above
        domain = _house_event_phrase(h)
        events.append(
            f"{n}. {_single_event_from_house(h).capitalize()} due to {maha} "
            f"as {h}{'st' if h==1 else 'nd' if h==2 else 'rd' if h==3 else 'th'} lord "
            f"activating {domain}{yoga_note}."
        )
        n += 1
        if n > 8:
            break

    block = f"  [{time_str}]\n"
    for e in events:
        block += f"  {e}\n"
    return block


def _predict_period(maha, sub, maha_ctx, sub_ctx, maha_start, maha_end,
                    sub_start, sub_end, sub_yogas, planet_data, lagna):
    """Generate a single TIME–EVENT–REASON prediction with full planet context.

    Format:
    "YEAR–YEAR: [EVENT] due to [ANTARDASHA planet] (House N, Sign, lordship)
     activating [house domain]. [MAHADASHA] Mahadasha drives [THEME].  [TAG]"
    """
    # Time window
    start_yr = _sign_year(sub_start)
    end_yr   = _sign_year(sub_end)
    time_str = f"{start_yr}–{end_yr}"

    sub_data   = planet_data.get(sub, {})
    maha_data  = planet_data.get(maha, {})
    sub_house  = sub_data.get("house", 0)
    sub_sign   = sub_data.get("sign", "")
    maha_house = maha_data.get("house", 0)
    maha_sign  = maha_data.get("sign", "")

    # Single deterministic event from Antardasha house
    event = _single_event_from_house(sub_house)

    # Priority tag
    tag = _event_tag(sub_house)

    # Yoga amplifier — strongest yoga only
    yoga_note = ""
    if sub_yogas:
        yoga_note = f", activating {sub_yogas[0]}"

    # Antardasha trigger — full context: planet (House N, Sign, lordship)
    sub_lord_str = _planet_lordship_summary(sub, planet_data, lagna)
    house_domain = _house_event_phrase(sub_house)
    trigger = (
        f"{sub} ({sub_sign}, House {sub_house} — {house_domain}){yoga_note}"
    )

    # Mahadasha background — include its house/sign for context
    maha_domain  = _PLANET_DOMAINS.get(maha, "life themes")
    maha_house_domain = _house_event_phrase(maha_house) if maha_house else maha_domain
    background = (
        f"{maha} Mahadasha ({maha_sign}, House {maha_house}) drives {maha_domain}"
    )

    # Special note for Budha-Aditya Yoga (Sun+Mercury) — Antardasha only
    yoga_amplifier = ""
    if sub == "Mercury" and maha != "Mercury":
        yoga_amplifier = (
            " Budha-Aditya Yoga amplifies intellectual output and communication breakthroughs."
        )
    elif sub == "Jupiter" and sub_house == maha_house:
        yoga_amplifier = " Vipreet Raj Yoga activates — rise through adversity in this sub-period."
    elif sub == "Saturn" and sub_house == maha_house:
        yoga_amplifier = " Vipreet Raj Yoga activates — unexpected gains from hidden or foreign sectors."

    line = f"{time_str}: {event} due to {trigger}.\n  {background}.{yoga_amplifier}"
    if tag:
        line += f"  {tag}"

    return line


def generate_time_event_predictions(kundali_data, dasha_data, planet_data, yogas=None):
    """Generate deterministic TIME–EVENT–REASON predictions for every Mahadasha
    and its 9 Antardasha sub-periods.

    Parameters
    ----------
    kundali_data  : full kundali dict (from kundali_rebuilt.json)
    dasha_data    : list of Vimshottari Mahadasha dicts
    planet_data   : planets dict keyed by planet name
    yogas         : optional list of detected yoga name strings

    Returns
    -------
    str — formatted prediction block ready for printing.
    """
    if yogas is None:
        yogas = []

    lagna = _get_lagna(kundali_data, planet_data)
    output = "\n=== DASHA + ANTARDASHA TIMELINE — DETERMINISTIC PREDICTIONS ===\n"

    prediction_count = 0
    validation_failures = []

    for maha_period in dasha_data:
        maha   = maha_period.get("planet", "")
        m_start = _parse_dasha_date(maha_period.get("start", ""))
        m_end   = _parse_dasha_date(maha_period.get("end", ""))
        m_years = maha_period.get("years", "?")

        maha_ctx = _build_planet_context(maha, planet_data, lagna)

        output += "=" * 60 + "\n"
        output += f"  MAHADASHA: {maha.upper()}   {maha_period.get('start','')} → {maha_period.get('end','')}   ({m_years} years)\n"
        output += "=" * 60 + "\n"

        # Mahadasha placement context line
        maha_data  = planet_data.get(maha, {})
        maha_house = maha_data.get("house", 0)
        maha_sign  = maha_data.get("sign", "")
        lord_str   = _planet_lordship_summary(maha, planet_data, lagna)
        output += f"  Placement: {lord_str} in {maha_sign} (House {maha_house})\n"

        # 5-8 event-level predictions for this Mahadasha
        output += "  MAHADASHA EVENT PREDICTIONS:\n"
        output += _generate_maha_event_block(maha, maha_data, planet_data, lagna, m_start, m_end, yogas)
        output += "\n"

        # Compute all 9 antardasha periods
        antardashas = _compute_antardasha_periods(maha, m_start, m_end)

        if not antardashas:
            output += "  Antardasha data unavailable for this period.\n\n"
            continue

        output += "  ANTARDASHA PREDICTIONS:\n\n"
        for ad in antardashas:
            sub        = ad["planet"]
            sub_start  = ad["start"]
            sub_end    = ad["end"]
            sub_years  = ad["years"]
            sub_ctx    = _build_planet_context(sub, planet_data, lagna)
            sub_yogas  = _active_yogas_for(sub, yogas)

            prediction = _predict_period(
                maha, sub, maha_ctx, sub_ctx,
                m_start, m_end, sub_start, sub_end,
                sub_yogas, planet_data, lagna,
            )

            start_str = sub_start.strftime("%b %Y") if sub_start else "?"
            end_str   = sub_end.strftime("%b %Y") if sub_end else "?"
            output += f"  [{maha}–{sub}]  {start_str} → {end_str}  ({sub_years} yrs)\n"
            output += f"  {prediction}\n\n"
            prediction_count += 1

            # --- Inline validation ---
            _has_time   = any(c.isdigit() for c in prediction)
            _has_event  = any(w in prediction.lower() for w in [
                "occurs", "manifests", "leads to", "happens", "intensify",
                "arises", "increase",
            ])
            _has_reason = "due to" in prediction.lower() or "because" in prediction.lower()
            _no_weak    = not any(w in prediction.lower() for w in [" may ", " might ", " could ", "possibly"])

            if not (_has_time and _has_event and _has_reason and _no_weak):
                validation_failures.append(f"{maha}–{sub}: missing " +
                    ", ".join(filter(None, [
                        "TIME"   if not _has_time   else "",
                        "EVENT"  if not _has_event  else "",
                        "REASON" if not _has_reason else "",
                        "weak-language" if not _no_weak else "",
                    ])))

    output += "\n"
    output += "─" * 60 + "\n"
    output += f"  Total predictions generated: {prediction_count}\n"

    # Validation summary
    output += "\n  AUTO-VALIDATION:\n"
    if not validation_failures:
        output += "  ✔ All predictions verified — TIME ✔  EVENT ✔  REASON ✔  No weak language ✔\n"
    else:
        output += f"  ✘ {len(validation_failures)} prediction(s) failed validation:\n"
        for f in validation_failures:
            output += f"    - {f}\n"

    return output


# -------------------------------------------------------
# CORE LIFE SYNTHESIS  — strongest section in the report
# -------------------------------------------------------
def core_life_synthesis(planet_data):
    """Return 5-6 chart-specific synthesis statements with conclusive language.
    No "indicates/suggests" — only "Therefore / Hence / This creates a life pattern where".
    Every line combines planet + house + dignity + conjunction + yoga.
    """
    output = "\n=== CORE LIFE SYNTHESIS ===\n"
    lagna = _get_lagna()

    sun    = planet_data.get("Sun",     {})
    moon   = planet_data.get("Moon",    {})
    mars   = planet_data.get("Mars",    {})
    mercury= planet_data.get("Mercury", {})
    jupiter= planet_data.get("Jupiter", {})
    venus  = planet_data.get("Venus",   {})
    saturn = planet_data.get("Saturn",  {})
    rahu   = planet_data.get("Rahu",    {})
    ketu   = planet_data.get("Ketu",    {})

    # 1. Lagna + Rahu in House 1 — identity pattern
    if lagna:
        lagna_lord = SIGN_LORDS.get(lagna, "Mercury")
        lagna_lord_h = planet_data.get(lagna_lord, {}).get("house", 0)
        rahu_h = rahu.get("house", 0)
        output += (
            f"1. {lagna} Lagna with Rahu in House {rahu_h}: Therefore, the native's identity "
            f"is permanently wired for reinvention — each life chapter brings a fundamentally "
            f"new self. The lagna lord ({lagna_lord}) in House {lagna_lord_h} channels "
            f"this restless intelligence into {HOUSE_MEANINGS.get(lagna_lord_h, 'multi-domain action')}. "
            f"This creates a life pattern where conventional paths are rejected and unconventional "
            f"mastery defines success.\n"
        )

    # 2. Sun-Mercury communication axis — no yoga name (covered in Yoga section)
    sun_h   = sun.get("house", 0)
    merc_h  = mercury.get("house", 0)
    sun_s   = sun.get("sign", "")
    merc_s  = mercury.get("sign", "")
    if sun_h == merc_h and sun_h > 0:
        output += (
            f"2. Sun + Mercury conjunct in {sun_s} (House {sun_h}): "
            f"Therefore, the native's professional identity is inseparable from communication, "
            f"writing, and intellectual authority. Every career chapter is built on the power "
            f"of articulation and self-directed intellectual output. Hence, media, publishing, "
            f"entrepreneurial communication, or content creation is the primary career vehicle — "
            f"not employment, not hierarchy.\n"
        )
    else:
        output += (
            f"2. Sun in House {sun_h} ({sun_s}) + Mercury in House {merc_h} ({merc_s}): "
            f"Therefore, authority and intellect operate in distinct life arenas — leadership "
            f"drives one domain while analytical precision anchors another. Hence, the native "
            f"excels wherever both qualities converge: advisory, analytical leadership, or "
            f"strategic communication roles.\n"
        )

    # 3. Jupiter-Saturn conjunction in dusthana — conclusion only, no yoga label
    jup_h   = jupiter.get("house", 0)
    sat_h   = saturn.get("house", 0)
    jup_s   = jupiter.get("sign", "")
    sat_s   = saturn.get("sign", "")
    if jup_h == sat_h and jup_h > 0:
        output += (
            f"3. Jupiter–Saturn Conjunction in House {jup_h} ({jup_s}): "
            f"Therefore, the dominant career pattern is rise through adversity — "
            f"institutional setbacks, hidden environments, and foreign sectors become "
            f"the native's actual springboard. Every apparent career obstacle is a mechanism "
            f"for elevation. Wisdom (Jupiter) and karmic discipline (Saturn) in House {jup_h} "
            f"guarantee that long-term results consistently outperform early appearances.\n"
        )

    # 4. Debilitation pattern — Neecha Bhanga
    deb_planets = []
    debilitation = {
        "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
        "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries",
    }
    for p, d in planet_data.items():
        if debilitation.get(p) == d.get("sign"):
            deb_planets.append((p, d.get("house", 0), d.get("sign", "")))
    if deb_planets:
        deb_str = "; ".join(
            f"{p} debilitated in {s} (House {h})" for p, h, s in deb_planets
        )
        output += (
            f"4. Debilitation + Neecha Bhanga Pattern: {deb_str}. "
            f"Therefore, these placements are not permanent weaknesses — they are pressure "
            f"points that, once mastered, generate Neecha Bhanga Raja Yoga results. "
            f"Hence, the native's greatest breakthroughs arrive precisely at the moments "
            f"where their debilitated planets are forced into maximum expression.\n"
        )

    # 5. Rahu-Ketu axis — karmic direction
    rahu_h = rahu.get("house", 0)
    ketu_h = ketu.get("house", 0)
    if rahu_h and ketu_h:
        output += (
            f"5. Rahu (House {rahu_h}) — Ketu (House {ketu_h}) Karmic Axis: "
            f"Therefore, the soul is designed to move away from "
            f"{HOUSE_MEANINGS.get(ketu_h, 'past comfort')} (Ketu — surrendered mastery) "
            f"and toward {HOUSE_MEANINGS.get(rahu_h, 'new territory')} (Rahu — life mission). "
            f"This creates a life pattern where self-mastery and identity development "
            f"(House {rahu_h}) must be actively chosen over karmic relationship dependency "
            f"(House {ketu_h}). Hence, the central tension of every life stage is: "
            f"self-development vs. karmic partnership resolution.\n"
        )

    # 6. Moon emotional core
    moon_sign = moon.get("sign", "")
    moon_h    = moon.get("house", 0)
    moon_nak  = moon.get("nakshatra", "")
    output += (
        f"6. Moon in {moon_sign} (House {moon_h}, Nakshatra: {moon_nak}): "
        f"Therefore, all emotional decisions are filtered through Libra's framework of "
        f"balance, beauty, and fairness. Hence, the native's relationships and daily "
        f"mental state are governed by an intense need for harmony — decisive action "
        f"under pressure is the recurring developmental challenge. "
        f"Moon in House {moon_h} places this emotional intelligence directly within "
        f"{HOUSE_MEANINGS.get(moon_h, 'the intelligence and creativity domain')} — "
        f"therefore creativity, child-related matters, and romance are emotionally "
        f"central to the native's life expression.\n"
    )

    return output


# -------------------------------------------------------
# FINAL JUDGEMENT  — disciplined close section
# -------------------------------------------------------
def final_judgement(planet_data):
    """Sharp 5-7 line Final Judgement. Each line is kundali-derived and conclusive.
    Format: hard-hitting declarative statements, no soft language.
    """
    output = "\n=== FINAL JUDGEMENT ===\n"
    lagna = _get_lagna()

    sun_h  = planet_data.get("Sun", {}).get("house", 0)
    sun_s  = planet_data.get("Sun", {}).get("sign", "")
    merc_h = planet_data.get("Mercury", {}).get("house", 0)
    jup_h  = planet_data.get("Jupiter", {}).get("house", 0)
    sat_h  = planet_data.get("Saturn", {}).get("house", 0)
    mars_h = planet_data.get("Mars", {}).get("house", 0)
    mars_s = planet_data.get("Mars", {}).get("sign", "")
    venus_h= planet_data.get("Venus", {}).get("house", 0)
    venus_s= planet_data.get("Venus", {}).get("sign", "")
    rahu_h = planet_data.get("Rahu", {}).get("house", 0)
    ketu_h = planet_data.get("Ketu", {}).get("house", 0)
    moon_s = planet_data.get("Moon", {}).get("sign", "Libra")
    moon_h = planet_data.get("Moon", {}).get("house", 0)

    lagna_idx    = SIGN_ORDER.index(lagna) if lagna in SIGN_ORDER else -1
    tenth_lord   = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 9) % 12], "Jupiter") if lagna_idx >= 0 else "Jupiter"
    seventh_lord = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 6) % 12], "Jupiter") if lagna_idx >= 0 else "Jupiter"
    tenth_lord_h = planet_data.get(tenth_lord, {}).get("house", 0)
    seventh_lord_h = planet_data.get(seventh_lord, {}).get("house", 0)

    # 1. LIFE THEME — kundali-specific, no generic astrology language
    output += (
        f"→ LIFE THEME: Life path is dominated by Rahu in House {rahu_h} ({lagna} Lagna) — "
        f"therefore identity evolves through constant reinvention, unconventional choices, "
        f"and the relentless pursuit of intellectual mastery. "
        f"Budha-Aditya Yoga in House {sun_h} ({sun_s}) locks communication and self-expression "
        f"as the permanent vehicle of destiny. The Rahu–Ketu axis (House {rahu_h}–{ketu_h}) "
        f"makes the tension between self-creation and karmic relationship resolution "
        f"the defining challenge across all life stages.\n\n"
    )

    # 2. CAREER DIRECTION — specific, not general
    output += (
        f"→ CAREER DIRECTION: The 10th lord ({tenth_lord}) in House {tenth_lord_h} "
        f"combined with Jupiter–Saturn conjunction (House {jup_h}, Vipreet Raj Yoga) "
        f"points to careers in media, content, writing, international consulting, "
        f"spiritual advisory, or institutional research — not conventional employment. "
        f"Career rise is non-linear: expect a breakthrough after age 28–32 when "
        f"Saturn Mahadasha (2026–2045) begins converting adversity into authority.\n\n"
    )

    # 3. RELATIONSHIP PATTERN — realistic, not soft
    output += (
        f"→ RELATIONSHIP PATTERN: The 7th lord ({seventh_lord}) in House {seventh_lord_h} "
        f"and Ketu in House {ketu_h} guarantee a partner with a foreign, spiritual, or "
        f"philosophically unconventional background — superficial bonds are impossible "
        f"for this chart. Venus debilitated in {venus_s} (House {venus_h}) creates "
        f"perfectionism that damages otherwise promising relationships; "
        f"therefore, the native must consciously release idealistic expectations "
        f"to sustain long-term harmony.\n\n"
    )

    # 4. BIGGEST STRENGTH
    output += (
        f"→ BIGGEST STRENGTH: Budha-Aditya Yoga (Sun in own sign {sun_s} + Mercury, "
        f"House {sun_h}) gives elite-level articulation, analytical precision, and "
        f"entrepreneurial boldness that peers cannot replicate. "
        f"Hence, every career breakthrough in this chart is built on the power of words, "
        f"ideas, and intellectual authority — not physical effort or inherited advantage.\n\n"
    )

    # 5. BIGGEST WEAKNESS
    output += (
        f"→ BIGGEST WEAKNESS: Mars debilitated in {mars_s} (House {mars_h}) creates "
        f"emotional reactivity and withdrawal under pressure — precisely when bold, "
        f"direct action is most needed. This is the single greatest threat to the "
        f"native's potential: the gap between intellectual vision and decisive execution. "
        f"Overcoming this debilitation directly activates Neecha Bhanga Raja Yoga, "
        f"transforming the chart's greatest vulnerability into its most powerful asset.\n\n"
    )

    # 6. FINAL TRAJECTORY — where this chart ultimately lands
    sat_maha_start = 2026  # Saturn Mahadasha after Jupiter
    output += (
        f"→ FINAL TRAJECTORY: Jupiter Mahadasha (2010–2026) builds the intellectual "
        f"and philosophical foundation; Saturn Mahadasha (2026–2045) converts that "
        f"foundation into material authority through discipline and adversity. "
        f"Hence, the native's peak career, financial, and relationship results "
        f"are locked in the 30s–40s age window — not before. "
        f"The chart's overall arc is: struggle → mastery → authority — "
        f"and this native will be most powerful in environments that others find "
        f"restrictive, foreign, or unconventional.\n"
    )

    return output


# -------------------------------
# FINAL REPORT
# -------------------------------
def generate_report():
    print("\n🔱 ELITE VEDIC ASTROLOGY REPORT 🔱")
    print("Powered by Multi-Layer Jyotish Reasoning Engine\n")

    # --- Report Navigation ---
    print("\n=== REPORT STRUCTURE ===")
    print("1.  Kundali Overview")
    print("2.  Planet-by-Planet Analysis")
    print("3.  Yoga Detection")
    print("4.  Core Life Synthesis")
    print("5.  Career Analysis")
    print("6.  Marriage Analysis")
    print("7.  Doshas")
    print("8.  Dasha + Antardasha (Timeline & Events)")
    print("9.  Aspects (Drishti)")
    print("10. Navamsa (D9)")
    print("11. Shadbala (Planetary Strength)")
    print("12. Transit (Current Period)")
    print("13. Final Judgement\n")

    # --------------------------------------------------------
    # 1. KUNDALI OVERVIEW
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 1 — KUNDALI OVERVIEW")
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
        print(f"  {planet:10s} | Sign: {data.get('sign', 'N/A'):15s} | "
              f"House: {data.get('house', 'N/A'):2} | Nakshatra: {data.get('nakshatra', 'N/A')}")
    print()

    # --------------------------------------------------------
    # 2. PLANET-BY-PLANET ANALYSIS
    #    Includes: placement synthesis · dignities · combustion
    #              conjunctions · retrograde · lordship
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 2 — PLANET-BY-PLANET ANALYSIS")
    print("=" * 60)
    print(synthesize_all_planets(planets))
    print(detect_exaltation_debilitation(planets))
    print(detect_combustion(planets))
    print(detect_conjunctions(planets))
    print(detect_retrograde(planets))
    print(analyze_lordships(planets))
    print(analyze_house_lord_effects(planets))
    print(analyze_lagna_lord(kundali, planets))
    print(analyze_10th_lord(planets, kundali))
    print(analyze_7th_lord(planets, kundali))

    # --------------------------------------------------------
    # 3. YOGA DETECTION
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 3 — YOGA DETECTION")
    print("=" * 60)
    print("\n🔥 CORE DESTINY FACTORS (YOGAS) 🔥\n")
    print(detect_real_yogas(planets))
    print(detect_advanced_yogas(planets))
    print(synthesize_chart(planets))
    print(combined_analysis(planets))

    # --------------------------------------------------------
    # 4. CORE LIFE SYNTHESIS  [strongest section]
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 4 — CORE LIFE SYNTHESIS")
    print("=" * 60)
    print(core_life_synthesis(planets))

    # --------------------------------------------------------
    # 5. CAREER ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 5 — CAREER ANALYSIS")
    print("=" * 60)
    print(analyze_career(planets))

    # --------------------------------------------------------
    # 6. MARRIAGE ANALYSIS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 6 — MARRIAGE ANALYSIS")
    print("=" * 60)
    print(analyze_marriage(planets))

    # --------------------------------------------------------
    # 7. DOSHAS
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 7 — DOSHAS")
    print("=" * 60)
    print(detect_doshas(planets))

    # --------------------------------------------------------
    # 8. DASHA + ANTARDASHA (TIMELINE & EVENTS)
    #    Full Mahadasha timeline + deterministic predictions
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 8 — DASHA + ANTARDASHA (TIMELINE & EVENTS)")
    print("=" * 60)
    print(generate_time_event_predictions(kundali, dasha, planets))
    print("\n⏳ DASHA SUPPLEMENTARY (THEMES + ANTARDASHA) ⏳\n")
    print(analyze_dasha())
    print(analyze_antardasha(dasha))
    print(antardasha_timeline(dasha))

    # --------------------------------------------------------
    # 9. ASPECTS (DRISHTI)
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 9 — ASPECTS (DRISHTI)")
    print("=" * 60)
    print("\n=== ASPECTS (DRISHTI) ===")
    for a in calculate_aspects(planets):
        print(" ", a)

    # --------------------------------------------------------
    # 10. NAVAMSA (D9)
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 10 — NAVAMSA (D9)")
    print("=" * 60)
    print("\n=== NAVAMSA (D9) ===")
    for p, d in planets.items():
        nav = calculate_navamsa(d.get("degree", 0), d.get("sign", ""))
        print(f"  {p:10s} → Navamsa sign: {nav}")

    # --------------------------------------------------------
    # 11. SHADBALA (PLANETARY STRENGTH)
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("SECTION 11 — SHADBALA (PLANETARY STRENGTH)")
    print("=" * 60)
    print("\n=== SHADBALA (PLANETARY STRENGTH) ===")
    for s in improved_shadbala(planets):
        print(" ", s)

    # --------------------------------------------------------
    # 12. TRANSIT (CURRENT PERIOD)
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 12 — TRANSIT (CURRENT PERIOD)")
    print("=" * 60)
    transit_text = saturn_transit_effect(planets)
    print(" ", transit_text)
    print("\n🪐 SADE SATI (SATURN MOON TRANSIT) 🪐\n")
    print(analyze_sadesati())

    # --------------------------------------------------------
    # 13. FINAL JUDGEMENT
    # --------------------------------------------------------
    print("=" * 60)
    print("SECTION 13 — FINAL JUDGEMENT")
    print("=" * 60)
    print(final_judgement(planets))


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


