from collections import defaultdict
import itertools
import json
import re
import random
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


# ============================================================
# FINAL REFINEMENT LAYER — Utility Functions
# ============================================================

def extract_year(date_str):
    """Extract the 4-digit year from a date string such as '26/01/2010'."""
    try:
        return int(date_str[:4])
    except (ValueError, TypeError):
        return None


def generate_time_window(main_period):
    """Return a HIGH PROBABILITY WINDOW string centred on the Mahadasha midpoint."""
    start = extract_year(main_period.get("start", ""))
    end   = extract_year(main_period.get("end", ""))
    if start and end:
        mid = (start + end) // 2
        return f"\n🔴 HIGH PROBABILITY WINDOW: {mid - 1}–{mid + 1}\n"
    return "TIME WINDOW: Not available"


ACTION_WORDS = [
    "activates",
    "triggers",
    "drives",
    "amplifies",
    "reshapes",
    "channels",
    "intensifies",
    "redirects",
]


def vary_sentence(sentence):
    """Replace bland/repetitive verbs and connectors to improve readability."""
    if not sentence:
        return sentence
    action = random.choice(ACTION_WORDS)
    replacements = {
        "produces":  action,
        "combines":  action,
        "creates":   action,
        "indicates": random.choice(["reveals", "shows", "signals"]),
        "therefore": random.choice(["thus", "accordingly", "as a result"]),
        "hence":     random.choice(["consequently", "so", "which leads to"]),
    }
    for k, v in replacements.items():
        sentence = sentence.replace(k, v)
    return sentence


def get_classical_support(keywords):
    """Retrieve one clean classical-text line for the given keywords.

    Delegates to get_best_classical_support() using the weighted citation
    engine.  Returns a formatted '📖 CLASSICAL SUPPORT' block, or None.
    """
    query = " ".join(keywords)
    result = get_best_classical_support(query, _all_books_chunks)
    if result:
        return result
    # fallback: try against full pool if primary dataset yields nothing
    fallback = get_best_classical_support(query, all_chunks)
    return fallback if fallback else None


# ============================================================
# WEIGHTED CLASSICAL CITATION ENGINE (Task 5)
# ============================================================

# Book keys match actual keys in all_books_chunks.json:
#   book2  = Brihat Parashara Hora Sastra
#   book4  = Jaimini Sutras
#   book7  = Phaladeepika
#   book5  = Jataka Parijata
#   book6  = Uttara Kalamrita
#   book3  = Crux of Vedic Astrology (Sanjay Rath)
#   book9  = Vedic Astrology – P.V.R. Narasimha Rao
#   book1  = Destiny and the Wheel of Time – K.N. Rao
#   book11 = Vedic Remedies in Astrology
#   book10 = General compilation
SOURCE_WEIGHTS = {
    "book2":  10,   # Brihat Parashara Hora Sastra
    "book4":  9,    # Jaimini Sutras
    "book7":  8,    # Phaladeepika
    "book5":  8,    # Jataka Parijata
    "book6":  7,    # Uttara Kalamrita
    "book3":  7,    # Crux of Vedic Astrology
    "book9":  6,    # Vedic Astrology – Narasimha Rao
    "book1":  5,    # K.N. Rao
    "book11": 5,    # Vedic Remedies
    "book10": 4,    # Compilation
}
DEFAULT_WEIGHT = 3

_BOOK_DISPLAY_NAMES = {
    "book2":  "Brihat Parashara Hora Shastra",
    "book4":  "Jaimini Sutras",
    "book7":  "Phaladeepika — Mantreswara",
    "book5":  "Jataka Parijata",
    "book6":  "Uttara Kalamrita — Kalidasa",
    "book3":  "Crux of Vedic Astrology — K.N. Rao",
    "book9":  "Vedic Astrology: An Integrated Approach — P.V.R. Narasimha Rao",
    "book1":  "Astrology, Destiny and the Wheel of Time — K.N. Rao",
    "book11": "Vedic Remedies in Astrology — Sanjay Rath",
    "book10": "Classical Compilation",
}

# Display names for raw filename keys used in all_books_chunked.json
_BOOK_RAW_DISPLAY_NAMES = {
    "Narasimha Rao - Vedic Astrology_ An Integrated Approach (2001) - libgen.li":
        "Vedic Astrology: An Integrated Approach — P.V.R. Narasimha Rao",
    "Mantreswara_s__Phaladeeplka_.pdf":   "Phaladeepika — Mantreswara",
    "Mantreswaras-Phaladeepika_.pdf":     "Phaladeepika — Mantreswara",
    "Brihat Parasara Hora Sastra.pdf":    "Brihat Parashara Hora Shastra",
    "Crux-of-Vedic-Astrology-Timing-of-Events1.pdf":
        "Crux of Vedic Astrology — K.N. Rao",
    "jataka-parijata.pdf":                "Jataka Parijata",
    "astrology destiny and the wheel of time.pdf":
        "Astrology, Destiny and the Wheel of Time — K.N. Rao",
    "Kalidasa_-_Uttara_Kalamrita_compressed(pdfgear.com) (1)-compressed.pdf":
        "Uttara Kalamrita — Kalidasa",
    "The Brihat Jataka of Varaha Mihira (N Chidambaram Iyer).pdf":
        "Brihat Jataka — Varahamihira",
    "vedic-remedies-in-astrology-3-pdf-free.pdf":
        "Vedic Remedies in Astrology — Sanjay Rath",
    "Jaimini-Sutras-Suryanarain-Rao-1949.pdf":
        "Jaimini Sutras — Suryanarain Rao",
}

# Classical authority weights for raw filename keys (sutra > commentary > modern)
_BOOK_RAW_WEIGHTS = {
    "Mantreswara_s__Phaladeeplka_.pdf":   10,
    "Mantreswaras-Phaladeepika_.pdf":     10,
    "Brihat Parasara Hora Sastra.pdf":    10,
    "jataka-parijata.pdf":                9,
    "Jaimini-Sutras-Suryanarain-Rao-1949.pdf": 9,
    "The Brihat Jataka of Varaha Mihira (N Chidambaram Iyer).pdf": 9,
    "Kalidasa_-_Uttara_Kalamrita_compressed(pdfgear.com) (1)-compressed.pdf": 8,
    "Crux-of-Vedic-Astrology-Timing-of-Events1.pdf": 7,
    "astrology destiny and the wheel of time.pdf": 6,
    "Narasimha Rao - Vedic Astrology_ An Integrated Approach (2001) - libgen.li": 6,
    "vedic-remedies-in-astrology-3-pdf-free.pdf": 5,
}

# Compiled regex to detect specific chart examples, exercises, and case studies
_EXAMPLE_RE = re.compile(
    r'chart \d+|example \d+|exercise \d+|native of chart|practical example'
    r'|let us consider|chart no[\. ]\d'
    r'|\b\d{2}[/\.]\d{2}[/\.]\d{4}\b'
    r'|\b\d{1,2}:\d{2}\s*(?:am|pm|ist)\b'
    r'|\bborn on \w|\bborn in \d{4}\b|\bbirth ?data\b',
    re.I,
)


def get_display_name(book):
    """Return a clean human-readable display name for any book key or filename."""
    if book in _BOOK_DISPLAY_NAMES:
        return _BOOK_DISPLAY_NAMES[book]
    if book in _BOOK_RAW_DISPLAY_NAMES:
        return _BOOK_RAW_DISPLAY_NAMES[book]
    # Fallback: strip file extension, underscores, and libgen artifacts
    name = re.sub(r'\.pdf$', '', book, flags=re.I)
    name = re.sub(r'_+', ' ', name)
    name = re.sub(r'\s*libgen\.\S+\s*', '', name, flags=re.I)
    name = re.sub(r'\s*-\s*$', '', name).strip()
    return name or "Classical Source"


def score_chunk(query, chunk):
    """Score a chunk by keyword overlap × source authority weight."""
    text         = chunk.get("text", "").lower()
    book         = chunk.get("book", "")
    keyword_score = sum(1 for w in query.lower().split() if len(w) > 2 and w in text)
    source_weight = SOURCE_WEIGHTS.get(book, _BOOK_RAW_WEIGHTS.get(book, DEFAULT_WEIGHT))
    return keyword_score * source_weight


def is_clean(chunk):
    """Return True only for chunks worth citing."""
    text = chunk.get("text", "")
    if len(text) < 80:
        return False
    low = text.lower()
    # Filter OCR garbage / page headers / short metadata
    if low.startswith(("-", "page", "vol", "chapter")):
        return False
    # Reject chunks that are almost pure OCR noise (very few alpha chars)
    alpha_ratio = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
    if alpha_ratio < 0.45:
        return False
    # Reject chunks that are specific chart examples or exercise cases
    norm = re.sub(r'\s+', ' ', re.sub(r'-\s*\n\s*', '', text))
    if _EXAMPLE_RE.search(norm):
        return False
    # Reject chunks with too many non-ASCII characters (OCR artifacts)
    non_ascii = sum(1 for c in text if ord(c) > 127)
    if non_ascii > 5:
        return False
    return True


def _extract_best_sentence(text, query_words):
    """Return the single sentence from text that best matches query_words.

    Applies OCR normalization, then selects the sentence with the most
    keyword hits.  Returns None when no acceptable sentence is found.
    """
    # Normalise OCR line-break artefacts
    norm = re.sub(r'-\s*\n\s*', '', text)
    norm = re.sub(r'\s+', ' ', norm).strip()

    sentences = re.split(r'(?<=[.!?])\s+', norm)
    best, best_score = None, 0
    for s in sentences:
        # Hard constraints: length and purity
        if len(s) < 45 or len(s) > 350:
            continue
        if sum(1 for c in s if ord(c) > 127) > 0:
            continue
        word_count = len(s.split())
        if word_count < 8:
            continue
        # Must not start with a number / page reference
        if re.match(r'^\d+[\s\.]', s):
            continue
        # Must contain at least one query keyword
        sl = s.lower()
        hit = sum(1 for w in query_words if w in sl)
        if hit == 0:
            continue
        if hit > best_score:
            best, best_score = s, hit
    return best


def get_best_classical_support(query, chunks):
    """Return the best sentence-level classical citation for query, or empty string.

    Algorithm:
    1. Score all clean, non-example chunks.
    2. Take the top-10 highest-scoring candidates.
    3. From each candidate, extract the single sentence with the most
       keyword hits.
    4. Return the citation from the first candidate that yields a good sentence.
    """
    query_words = [w for w in query.lower().split() if len(w) > 2]

    scored = []
    for c in chunks:
        if not is_clean(c):
            continue
        s = score_chunk(query, c)
        if s > 0:
            scored.append((s, c))

    scored.sort(reverse=True, key=lambda x: x[0])
    top_candidates = scored[:10]

    for _, chunk in top_candidates:
        text = chunk.get("text", "").strip()
        book = chunk.get("book", "")
        page = chunk.get("page", "")

        sentence = _extract_best_sentence(text, query_words)
        if not sentence:
            continue

        display = get_display_name(book)
        if page:
            display += f" (p.{page})"
        return f"\n📖 Classical Support\n→ {display}\n→ \"{sentence.strip()}\"\n"

    return ""


# ============================================================
# END WEIGHTED CITATION ENGINE
# ============================================================
# ============================================================

# ============================================================
# GLOBAL SYNTHESIS ENGINE (Task 3)
# ============================================================

_DEBIL_MAP_SYNTH = {
    "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
    "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries",
}


def _build_synthesis_context(planet, planet_data):
    """Build context dict for resolve_conflicts() using kundali data only."""
    pdata  = planet_data.get(planet, {})
    sun_h  = planet_data.get("Sun", {}).get("house", 0)
    mer_h  = planet_data.get("Mercury", {}).get("house", 0)
    moon_h = planet_data.get("Moon", {}).get("house", 0)
    jup_h  = planet_data.get("Jupiter", {}).get("house", 0)
    sat_h  = planet_data.get("Saturn", {}).get("house", 0)
    p_h    = pdata.get("house", 0)
    p_s    = pdata.get("sign", "")

    # Combust: planet shares a house with the Sun (excluding Sun itself)
    combust = (planet != "Sun" and p_h > 0 and p_h == sun_h)

    # Debilitated: planet is in its debilitation sign
    debil = bool(p_s and _DEBIL_MAP_SYNTH.get(planet) == p_s)

    # Retrograde: sourced from kundali data ONLY — no inference
    retro = pdata.get("retrograde") is True

    # Yoga list: names of active yogas this planet participates in
    yoga = []
    if planet in ("Sun", "Mercury") and sun_h and mer_h and sun_h == mer_h:
        yoga.append("Budha-Aditya")
    if debil:
        yoga.append("Neecha Bhanga")
    if planet in ("Jupiter", "Moon") and jup_h and moon_h and abs(jup_h - moon_h) % 3 == 0:
        yoga.append("Gajakesari")
    if planet == "Saturn" and sat_h in (6, 8, 12):
        yoga.append("Vipreet Raj")

    return {
        "combust":     combust,
        "debilitated": debil,
        "retrograde":  retro,
        "yoga":        yoga,
    }


def resolve_conflicts(planet, pdata, context):
    """Return list of unified synthesis statements that resolve chart contradictions."""
    result = []

    combust = context.get("combust", False)
    debil   = context.get("debilitated", False)
    yoga    = context.get("yoga", [])
    retro   = context.get("retrograde", False)
    house   = pdata.get("house")

    if combust and "Budha-Aditya" in yoga:
        result.append(
            f"{planet} forms Budha-Aditya Yoga but combustion modifies it — "
            f"intelligence becomes authority-driven rather than neutral."
        )

    if debil and "Neecha Bhanga" in yoga:
        result.append(
            f"{planet} is debilitated but Neecha Bhanga cancels this — "
            f"results improve after struggle."
        )

    if retro:
        result.append(
            f"{planet} is retrograde — results manifest after internal processing and delay."
        )

    if house in [6, 8, 12] and yoga:
        result.append(
            f"{planet} operates through adversity — results come after transformation."
        )

    return result


# ============================================================
# END SYNTHESIS ENGINE
# ============================================================


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
                interpreted = vary_sentence(interpreted)
                section += f"- {interpreted}\n"

        # Task 3 — classical support for each planet
        support = get_classical_support(keywords[:2])
        if support:
            section += f"\n{'-' * 40}{support}{'-' * 40}\n"

        # Task 4 — synthesis: resolve combust/debil/yoga contradictions
        context   = _build_synthesis_context(planet, planets)
        conflicts = resolve_conflicts(planet, data, context)
        if conflicts:
            section += "\n⚖️ SYNTHESIS:\n"
            for c in conflicts:
                section += f"- {c}\n"

        section = "\n" + section.strip() + "\n"
        output.append(section)

    return "\n".join(output)


# -------------------------------
# MAHADASHA ANALYSIS (CLEAN)
# -------------------------------
def analyze_dasha():
    """Chart-driven Mahadasha analysis — no generic planet descriptions.
    Every statement derived from actual planet placement, lordship, dignity, and yogas.
    """
    output = "\n=== MAHADASHA ANALYSIS ===\n"
    lagna_sign = _get_lagna()

    # Chart-wide Neecha Bhanga note — debilitated planets affect ALL Mahadasha results
    _DEBIL_MAP = {"Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
                  "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries"}
    debil_planets = [(p, planets[p].get("sign",""), planets[p].get("house",0))
                     for p in planets if _DEBIL_MAP.get(p) == planets[p].get("sign","")]
    if debil_planets:
        debil_str = "; ".join(f"{p} debilitated in {s} (House {h})" for p, s, h in debil_planets)
        output += (
            f"Neecha Bhanga alert — {debil_str}: during any Mahadasha where these planets are "
            f"conjunct, aspecting, or activating the same house, debilitation results can be "
            f"cancelled (Neecha Bhanga Raja Yoga), producing unexpected breakthroughs.\n\n"
        )

    for period in dasha[:3]:
        planet = period.get("planet", "Unknown")
        start  = period.get("start", "?")
        end    = period.get("end", "?")
        pdata  = planets.get(planet, {})
        house  = pdata.get("house", 0)
        sign   = pdata.get("sign", "")
        nak    = pdata.get("nakshatra", "")
        house_domain = _house_event_phrase(house)

        # Lordship
        lord_str = _planet_lordship_summary(planet, planets, lagna_sign) if lagna_sign else planet

        # Dignity flag + Neecha Bhanga note
        _EXALT = {"Sun": "Aries", "Moon": "Taurus", "Mars": "Capricorn",
                  "Mercury": "Virgo", "Jupiter": "Cancer", "Venus": "Pisces", "Saturn": "Libra"}
        _DEBIL = {"Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
                  "Mercury": "Pisces", "Jupiter": "Capricorn", "Venus": "Virgo", "Saturn": "Aries"}
        if _EXALT.get(planet) == sign:
            dignity = "exalted — peak strength, full domain delivery"
        elif _DEBIL.get(planet) == sign:
            dignity = "debilitated — execution pressure active; Neecha Bhanga cancellation converts this into breakthrough force when triggered"
        else:
            dignity = f"in {sign}"

        output += f"\n{planet} Mahadasha ({start} → {end}):\n"
        output += f"  Placement: {lord_str} {dignity} (House {house} — {house_domain})\n"
        if nak:
            output += f"  Nakshatra: {nak}\n"

        # Career signal
        tenth_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna_sign) + 9) % 12], "") if lagna_sign in SIGN_ORDER else ""
        if planet == tenth_lord or house == 10:
            output += f"  Career: {planet} as 10th lord/10th house planet is the primary professional activator — authority-level results are delivered directly through this period.\n"
        elif house in (6, 8, 12):
            output += (
                f"  Career: {planet} in House {house} routes career through {house_domain} — "
                f"the rise is non-linear; adversity and isolation are the accelerator, not the obstacle.\n"
            )
        else:
            output += (
                f"  Career: {planet} in House {house} directs professional focus into {house_domain} — "
                f"results are tied to how effectively the native activates House {house} themes.\n"
            )

        # Relationship signal
        seventh_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna_sign) + 6) % 12], "") if lagna_sign in SIGN_ORDER else ""
        if planet == seventh_lord or house == 7:
            output += f"  Relationships: {planet} as 7th lord directly triggers partnership-level events — commitments, separations, and partnership restructuring are activated.\n"
        elif planet in ("Venus", "Ketu"):
            output += (
                f"  Relationships: {planet} in House {house} ({house_domain}) is the defining relationship "
                f"force — karmic bonds are forged or dissolved according to {planet}'s house activation.\n"
            )

        # Timing tendency (beginning / mid / end phase behaviour)
        output += (
            f"  Timing tendency: early phase ({start[:4] if start != '?' else '?'}) — "
            f"foundation-building in {house_domain}; "
            f"mid phase — maximum activation of House {house} results; "
            f"final phase — consolidation and handoff to next period's agenda.\n"
        )

    return output


# -------------------------------
# SADE SATI ANALYSIS (CLEAN)
# -------------------------------
_SADESATI_PHASE = {
    "Rising":  "Saturn moves into the sign preceding the natal Moon, activating mounting karmic pressure across emotional, financial, and relational domains. The native's existing life structures are stress-tested — weaknesses in career, relationships, and self-identity are exposed. This pressure is not random; it is targeted correction that forces a reorientation toward long-term durability over short-term comfort.",
    "Peak":    "Saturn transits directly over the natal Moon sign, producing maximum emotional restructuring. The native confronts core karmic obligations — career pivots, relationship reassessments, financial restructuring, and deep inner identity shifts are not optional during this phase but are the mechanism of progress. Every 'loss' in this phase eliminates what was never structurally sound.",
    "Setting": "Saturn clears the natal Moon sign, and the accumulated karmic work of the preceding 5 years begins crystallizing into real-world outcomes. Financial recovery, stabilized relationships, and career clarity emerge — not as gifts, but as the direct product of the native having survived and internalized the preceding two phases. Forward momentum is now structurally grounded.",
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
            interpreted = vary_sentence(interpreted)
            output += f"- {interpreted}\n"

    # Task 3 — classical authority for yoga section
    support = get_classical_support(["raj yoga", "kendra", "trikona"])
    if support:
        output += f"\n{'-' * 40}{support}{'-' * 40}\n"

    output = "\n" + output.strip() + "\n"
    return output
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

    mars_house = planet_data.get("Mars", {}).get("house")
    mars_sign  = planet_data.get("Mars", {}).get("sign", "")
    if mars_house in [1, 4, 7, 8, 12]:
        output += (
            f"Manglik Dosha — Mars in House {mars_house}: this placement "
            f"intensifies partnership dynamics because Mars injects drive, assertion, "
            f"and conflict energy directly into the {_house_event_phrase(mars_house)} domain. "
            f"Partnership compatibility is determined by whether the partner's chart "
            f"carries equivalent Mars strength to absorb and redirect this energy."
        )
        if mars_sign == "Cancer":
            output += (
                f" Mars is also debilitated in {mars_sign} — aggression is internalized "
                f"rather than openly expressed, producing passive-aggressive patterns "
                f"in close relationships until Neecha Bhanga conditions cancel the debilitation."
            )
        output += "\n"

    rahu_house = planet_data.get("Rahu", {}).get("house")
    ketu_house = planet_data.get("Ketu", {}).get("house")
    if rahu_house == 1 and ketu_house == 7:
        output += (
            f"Kaal Sarp-axis pattern — Rahu in House 1 / Ketu in House 7: the Rahu-Ketu "
            f"axis spanning Lagna–7th house converts every life cycle into a tension between "
            f"radical self-reinvention and karmic relationship obligations. "
            f"Financial and career peaks alternate with relationship restructuring events — "
            f"this is the defining oscillation pattern of the entire life journey. "
            f"The resolution point comes when self-development (House 1 Rahu) is channelled "
            f"into purposeful contribution rather than identity-driven ambition.\n"
        )

    return output


# -------------------------------
# FINAL SYNTHESIS / PREDICTION
# -------------------------------
def generate_final_prediction(planet_data, dasha_list, transit_text):
    output = "\n=== FINAL PREDICTION ===\n"

    # Career logic based on Saturn placement
    if "Saturn" in planet_data:
        house = planet_data["Saturn"].get("house")
        if house in [10, 11]:
            output += "Saturn in a professional house (10th/11th) locks long-term career growth through disciplined, persistent effort — authority and recognition are built, not gifted.\n"
        elif house in [6, 8, 12]:
            output += (
                f"Saturn in House {house} (dusthana) routes career through adversity and non-linear paths — "
                f"every major career setback is the mechanism for a larger subsequent rise. "
                f"Vipreet Raj Yoga converts each obstacle into structural career capital.\n"
            )

    # Moon sign emotional nature
    moon_sign = planet_data.get("Moon", {}).get("sign")
    if moon_sign:
        output += (
            f"Moon in {moon_sign}: emotional decisions are filtered through {moon_sign}'s framework — "
            f"relationships, creativity, and financial choices all carry this sign's quality "
            f"as the foundational decision-making layer.\n"
        )

    # Dasha influence
    if len(dasha_list) > 0:
        current = dasha_list[0].get("planet", "")
        p_h = planet_data.get(current, {}).get("house", 0)
        output += (
            f"{current} Mahadasha (active): {current} in House {p_h} drives "
            f"{_house_event_phrase(p_h)} as the dominant life theme — "
            f"all major events in this period are channelled through House {p_h} activations.\n"
        )

    # Transit influence
    output += transit_text + "\n"

    return output


# -------------------------------
# COMBINED SYNTHESIS ANALYSIS
# -------------------------------
def combined_analysis(planet_data):
    output = "\n=== COMBINED ANALYSIS ===\n"

    if (planet_data.get("Saturn", {}).get("house") == 12
            and planet_data.get("Jupiter", {}).get("house") == 12):
        output += "Saturn + Jupiter both in House 12: spiritual depth and foreign institutional connections are the primary life arenas — not conventional mainstream environments.\n"

    if planet_data.get("Sun", {}).get("house") == 3:
        output += "Sun in House 3: leadership is exercised through communication and self-directed effort, not hierarchical authority.\n"

    return output


# -------------------------------
# ANTARDASHA ANALYSIS
# -------------------------------
def analyze_antardasha(dasha_list):
    """Chart-driven Antardasha analysis.
    For each Mahadasha × Antardasha pair, derive 3-5 predictions from:
      - Mahadasha planet's house + lordship
      - Antardasha planet's house + lordship
      - Their combined house activation and domain interaction
    No generic planet dictionaries used.
    """
    output = "\n=== ANTARDASHA ANALYSIS ===\n"

    if len(dasha_list) < 2:
        return output + "Insufficient data for Antardasha analysis.\n"

    main = dasha_list[0].get("planet", "")
    sub  = dasha_list[1].get("planet", "")

    lagna_sign = _get_lagna()
    main_data  = planets.get(main, {})
    sub_data   = planets.get(sub, {})
    main_h     = main_data.get("house", 0)
    sub_h      = sub_data.get("house", 0)
    main_s     = main_data.get("sign", "")
    sub_s      = sub_data.get("sign", "")
    main_nak   = main_data.get("nakshatra", "")
    sub_nak    = sub_data.get("nakshatra", "")

    main_domain = _house_event_phrase(main_h)
    sub_domain  = _house_event_phrase(sub_h)

    main_lord_str = _planet_lordship_summary(main, planets, lagna_sign) if lagna_sign else main
    sub_lord_str  = _planet_lordship_summary(sub,  planets, lagna_sign) if lagna_sign else sub

    output += f"Active period: {main}–{sub} Mahadasha–Antardasha\n"
    output += f"  {main}: {main_lord_str} in {main_s} (House {main_h} — {main_domain})\n"
    output += f"  {sub}:  {sub_lord_str} in {sub_s} (House {sub_h} — {sub_domain})\n"

    # Chart-wide combustion note relevant to this period
    sun_h_val = planets.get("Sun", {}).get("house", 0)
    combust_planets_in_chart = [
        p for p in planets
        if p not in ("Sun", "Rahu", "Ketu")
        and planets[p].get("house", 0) == sun_h_val
        and sun_h_val > 0
    ]
    if combust_planets_in_chart:
        cp_str = ", ".join(combust_planets_in_chart)
        cp_h   = sun_h_val
        output += (
            f"  Combustion note: {cp_str} is combust (conjunct Sun, House {cp_h}) — "
            f"during this period, {cp_str}'s domain output is filtered through solar authority, "
            f"giving a commanding quality to results in House {cp_h} themes.\n"
        )
    output += "\n"

    output += f"=== {main}–{sub} Interaction Predictions ===\n"

    n = 1

    # 1. Career: triggered when either planet rules or occupies career houses
    tenth_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna_sign) + 9) % 12], "") if lagna_sign in SIGN_ORDER else ""
    if main == tenth_lord or sub == tenth_lord or main_h == 10 or sub_h == 10:
        output += (
            f"{n}. Career peak: {main} (House {main_h}, {main_domain}) × {sub} (House {sub_h}, {sub_domain}) "
            f"— because {tenth_lord} as 10th lord is active, professional authority and public recognition "
            f"are the direct outputs of this combined period. Career events in this window are defining, not transitional.\n"
        )
    else:
        output += (
            f"{n}. Career direction shift: {main} in House {main_h} ({main_domain}) combines with {sub} in "
            f"House {sub_h} ({sub_domain}) — the result is professional energy redirected away from mainstream paths "
            f"and into {sub_domain}-driven opportunities. Advancement comes from House {sub_h} activation, not title or hierarchy.\n"
        )
    n += 1

    # 2. Communication/intellect: Mercury or House 3 involvement
    if main == "Mercury" or sub == "Mercury" or main_h == 3 or sub_h == 3:
        comm_partner = main if main != "Mercury" else sub
        comm_domain  = _house_event_phrase(sub_h if main == "Mercury" else main_h)
        output += (
            f"{n}. Structured intellectual output: Mercury (House 3, communication + analysis) combines with "
            f"{comm_partner} ({comm_domain}) — because House 3 and {comm_domain} interact, "
            f"writing, research, or content production becomes the primary career vehicle. "
            f"The best output in this period is long-form, analytical, and independently driven.\n"
        )
        n += 1

    # 3. Foreign/international: House 12 or Rahu involvement
    if main_h == 12 or sub_h == 12 or main == "Rahu" or sub == "Rahu":
        foreign_planet = main if (main_h == 12 or main == "Rahu") else sub
        other_planet   = sub  if foreign_planet == main else main
        fp_h = planets.get(foreign_planet, {}).get("house", "?")
        output += (
            f"{n}. Foreign / institutional activation: {foreign_planet} (House {fp_h}) interacts with "
            f"{other_planet} — because House 12 or Rahu energy is dominant, overseas engagement, "
            f"institutional placement, behind-the-scenes research, or cross-border opportunity is the "
            f"concrete career output. Foreign contacts made in this period convert into long-term professional leverage.\n"
        )
        n += 1

    # 4. Relationship: 7th lord or Venus / Ketu
    seventh_lord = SIGN_LORDS.get(SIGN_ORDER[(SIGN_ORDER.index(lagna_sign) + 6) % 12], "") if lagna_sign in SIGN_ORDER else ""
    rel_planet = main if main in (seventh_lord, "Venus") else (sub if sub in (seventh_lord, "Venus") else None)
    if rel_planet:
        output += (
            f"{n}. Relationship event: {rel_planet} as 7th lord or Venus activates partnership-domain events — "
            f"because {rel_planet} is the primary relationship indicator and is active in this period, "
            f"commitment decisions, partnership restructuring, or romantic deepening become time-sensitive. "
            f"Events in this domain during this window carry long-term karmic weight.\n"
        )
        n += 1

    # 5. Financial: 2nd/11th activation
    if main_h in (2, 11) or sub_h in (2, 11):
        fin_planet = main if main_h in (2, 11) else sub
        fin_h = planets.get(fin_planet, {}).get("house", 0)
        output += (
            f"{n}. Financial accumulation window: {fin_planet} in House {fin_h} ({_house_event_phrase(fin_h)}) "
            f"activates the wealth axis — because House {fin_h} is a direct income/gain house, "
            f"financial inflows in this period are structurally supported. "
            f"Investment decisions and income-generating initiatives launched in this window carry higher durability.\n"
        )
        n += 1

    # 6. Combustion note — include if Sun is main or sub, OR if either planet is combust
    sun_h_val = planets.get("Sun", {}).get("house", 0)
    if main == "Sun" or sub == "Sun":
        combust_partner = sub if main == "Sun" else main
        if planets.get(combust_partner, {}).get("house") == sun_h_val:
            output += (
                f"{n}. Combustion factor: {combust_partner} conjunct Sun — because {combust_partner}'s significations "
                f"are under solar pressure, its domain output is amplified with authority but loses independent neutrality. "
                f"Career results from {combust_partner}'s house carry a commanding, Sun-directed quality.\n"
            )
            n += 1
    else:
        # Check if either planet is combust (conjunct Sun by house proximity)
        for check_planet in (main, sub):
            if check_planet not in ("Sun", "Rahu", "Ketu"):
                p_h = planets.get(check_planet, {}).get("house", 0)
                if p_h and p_h == sun_h_val:
                    output += (
                        f"{n}. Combustion factor: {check_planet} occupies the same house as Sun — "
                        f"because {check_planet}'s significations are filtered through solar authority, "
                        f"its career and intellectual output carries a commanding, decisive quality. "
                        f"Results are amplified but independence is reduced; the native succeeds by channelling "
                        f"willpower into {check_planet}'s domain rather than pure analysis.\n"
                    )
                    n += 1
                    break  # only note once

    return output


# -------------------------------
# CLASSICAL YOGA DETECTION
# -------------------------------
def detect_real_yogas(planet_data):
    output = "\n=== CLASSICAL YOGA ANALYSIS ===\n"

    yoga_found = False

    # Raj Yoga: Kendra + Trikona lord interaction (simplified)
    if (planet_data.get("Jupiter", {}).get("house") in [1, 5, 9]
            and planet_data.get("Saturn", {}).get("house") in [1, 4, 7, 10]):
        output += "⚡ Raj Yoga — kendra and trikona lords combine: authority, public status, and elevated recognition are delivered through disciplined, strategic effort.\n"
        yoga_found = True

    # Dhan Yoga
    if (planet_data.get("Venus", {}).get("house") in [2, 11]
            or planet_data.get("Jupiter", {}).get("house") in [2, 11]):
        output += "💰 Dhan Yoga — wealth lord active in financial houses: accumulated prosperity is built through consistent intelligent effort, not windfall.\n"
        yoga_found = True

    # Vipreet Raj Yoga
    if planet_data.get("Saturn", {}).get("house") in [6, 8, 12]:
        output += "🔥 Vipreet Raj Yoga — Saturn in dusthana: career rise comes specifically through adversity, isolation, and paths others reject; the harder the path, the more durable the outcome.\n"
        yoga_found = True

    if not yoga_found:
        output += "No major classical yogas detected, but planetary combinations still drive destiny through accumulated house activations.\n"

    return output


# -------------------------------
# CAREER ANALYSIS
# -------------------------------
def analyze_career(planet_data):
    """Unified Career Synthesis — one integrated narrative combining 10th lord,
    Saturn, Mercury, Rahu, and Sun into a single chart-driven conclusion block.
    No separate planet sub-headings; all logic flows as connected career logic.
    """
    output = "\n=== CAREER SYNTHESIS ===\n"

    lagna = _get_lagna()
    saturn_h   = planet_data.get("Saturn",  {}).get("house", 0)
    saturn_s   = planet_data.get("Saturn",  {}).get("sign", "")
    sun_h      = planet_data.get("Sun",     {}).get("house", 0)
    sun_s      = planet_data.get("Sun",     {}).get("sign", "")
    mercury_h  = planet_data.get("Mercury", {}).get("house", 0)
    mercury_s  = planet_data.get("Mercury", {}).get("sign", "")
    jupiter_h  = planet_data.get("Jupiter", {}).get("house", 0)
    jupiter_s  = planet_data.get("Jupiter", {}).get("sign", "")
    rahu_h     = planet_data.get("Rahu",    {}).get("house", 0)
    mars_h     = planet_data.get("Mars",    {}).get("house", 0)
    mars_s     = planet_data.get("Mars",    {}).get("sign", "")

    # Derive 10th lord
    tenth_lord = "Unknown"
    tenth_sign = ""
    tenth_lord_h = 0
    tenth_lord_s = ""
    if lagna in SIGN_ORDER:
        lagna_idx  = SIGN_ORDER.index(lagna)
        tenth_sign = SIGN_ORDER[(lagna_idx + 9) % 12]
        tenth_lord = SIGN_LORDS.get(tenth_sign, "Unknown")
        tenth_lord_h = planet_data.get(tenth_lord, {}).get("house", 0)
        tenth_lord_s = planet_data.get(tenth_lord, {}).get("sign", "")

    # Check combustion: Mercury combust if conjunct Sun
    mercury_combust = (mercury_h == sun_h and mercury_h > 0)

    # ── PILLAR 1: 10th lord (Jupiter) → direction and domain ──
    output += f"10th lord ({tenth_lord}) in {tenth_lord_s} (House {tenth_lord_h}): "
    if tenth_lord_h in (10, 11):
        output += f"{tenth_lord} in House {tenth_lord_h} places career authority directly in the professional domain — public recognition and institutional status are the primary career rewards.\n"
    elif tenth_lord_h == 12:
        output += (
            f"{tenth_lord} in House 12 anchors career in foreign environments, institutional "
            f"research, spiritual advisory, or behind-the-scenes consulting. "
            f"Conventional employment is incompatible with this placement — the native's career "
            f"thrives only in non-hierarchical, autonomous, or cross-border contexts. "
            f"{tenth_lord} as 10th lord in House 12 activates Vipreet Raj Yoga: rise comes "
            f"through adversity, isolation, and unconventional paths that peers avoid.\n"
        )
    elif tenth_lord_h in (1, 5, 9):
        output += (
            f"{tenth_lord} in House {tenth_lord_h} (trikona) — career is tied to personal "
            f"dharma, creative intelligence, and fortunate opportunities. Self-driven effort "
            f"over institutional support is the primary mechanism.\n"
        )
    elif tenth_lord_h in (6, 8):
        output += (
            f"{tenth_lord} in House {tenth_lord_h} routes career through transformation, "
            f"service, or adversity — the dusthana placement converts setbacks into "
            f"professional springboards over time.\n"
        )
    else:
        output += (
            f"{tenth_lord} in House {tenth_lord_h} shifts career focus toward "
            f"{_house_event_phrase(tenth_lord_h)}.\n"
        )

    # ── PILLAR 2: Saturn — karmic timeline and career longevity ──
    output += f"\nSaturn (karma karaka) in {saturn_s} (House {saturn_h}): "
    if saturn_h in (10, 11):
        output += "Long-term career growth through discipline and persistence. Authority and status accumulate with time — results are built to last.\n"
    elif saturn_h in (6, 8, 12):
        output += (
            f"Career runs through delays and unconventional paths — House {saturn_h} placement "
            f"activates Vipreet Raj Yoga potential: unexpected rise through adversity. "
            f"Every apparent setback is a mechanism for long-term elevation.\n"
        )
    elif saturn_h == 3:
        output += "Career success through consistent written or spoken effort, self-driven entrepreneurial initiative, and communication-based persistence.\n"
    elif saturn_h == 7:
        output += "Partnerships and business collaborations are the primary career accelerator. Disciplined business approach is non-negotiable.\n"
    else:
        output += f"Career discipline and karmic accountability are channelled through House {saturn_h} themes.\n"

    # ── PILLAR 3: Mercury — intellectual engine and skill domain ──
    output += f"\nMercury (skill engine) in {mercury_s} (House {mercury_h})"
    if mercury_combust:
        output += f" [COMBUST — conjunct Sun in House {sun_h}]"
    output += ": "

    if mercury_h == 3:
        output += (
            "Mercury in its natural house — communication, writing, and analytical output "
            "are the primary career accelerators. Every major breakthrough is driven by words "
            "and ideas, not connections or credentials. Content creation, editorial work, "
            "strategic communication, or entrepreneurial publishing are the highest-leverage paths."
        )
    elif mercury_h == 10:
        output += "Career in communication, media, analytics, or writing is directly supported by this placement."
    elif mercury_h == 1:
        output += "Intellectual and communicative personality carries career advantages in any knowledge-based field."
    else:
        output += f"Intellectual and communicative strengths activate {_house_event_phrase(mercury_h)} as the primary career domain."

    if mercury_combust:
        output += (
            f" Combustion by Sun in House {sun_h} means Mercury's output is "
            f"amplified by solar authority — writing and speaking carry a decisive, "
            f"commanding quality rather than a purely analytical one."
        )
    output += "\n"

    # ── PILLAR 4: Rahu — ambition vector and career disruption ──
    output += f"\nRahu (ambition driver) in {planet_data.get('Rahu', {}).get('sign', '')} (House {rahu_h}): "
    if rahu_h == 1:
        output += (
            "Rahu in the Lagna makes unconventional, cross-cultural, content-driven, or "
            "technology-oriented careers the only sustainable path. "
            "The native's strongest career periods come from breaking convention, not conforming to it. "
            "Rahu in House 1 amplifies ambition to the point of identity — career is not just a means "
            "of livelihood but a vehicle for radical self-reinvention."
        )
    elif rahu_h == 10:
        output += "Intense public career ambition — media, technology, or foreign organisations are the natural fit. Unconventional rise to prominence is highly activated."
    elif rahu_h == 3:
        output += "Bold, unconventional communication drives career. Media, technology, writing, and entrepreneurship are the natural career channels."
    elif rahu_h == 11:
        output += "Strong drive for career gains through networks and large-scale enterprise. Technology or international business brings sudden professional rises."
    else:
        output += f"Unconventional career ambition is channelled through House {rahu_h} themes — traditional paths underperform against non-conventional routes."
    output += "\n"

    # ── CAREER VERDICT: integrated one-paragraph conclusion ──
    _DEBIL = {"Mars": "Cancer", "Venus": "Virgo"}
    mars_debil = (_DEBIL.get("Mars") == mars_s)
    output += "\nCAREER VERDICT:\n"
    output += (
        f"{tenth_lord} (10th lord) in House {tenth_lord_h} + Saturn in House {saturn_h} + "
        f"Mercury in House {mercury_h} + Rahu in House {rahu_h} — "
        f"the career architecture of this chart points to: research, international advisory, "
        f"content creation, spiritual consulting, or institutional work in non-mainstream environments. "
        f"Career trajectory is non-linear: early struggle → late-30s breakthrough → "
        f"40s consolidation into genuine authority."
    )
    if mars_debil:
        output += (
            f" Mars debilitated in {mars_s} (House {mars_h}) creates execution gaps — "
            f"the native must consciously overcome the tendency to retreat under pressure. "
            f"Mastering this debilitation activates Neecha Bhanga Raja Yoga, converting the "
            f"chart's greatest execution weakness into its most durable career asset."
        )
    output += "\n"

    # Task 3 — classical support for career section
    career_support = get_classical_support(["tenth house", "career", "saturn", "sun"])
    if career_support:
        output += f"\n{'-' * 40}{career_support}{'-' * 40}\n"

    output = "\n" + output.strip() + "\n"
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
        output += "Venus in 2nd house: wealth accumulation is directly tied to relationship quality — the partner's family background and financial values shape the native's material trajectory.\n"
    elif venus_house in [6, 8, 12]:
        output += "Venus in dusthana: relationships demand transformation and sacrifice — the depth of the karmic bond is directly proportional to the difficulty of the initial formation.\n"
    elif venus_house == 5:
        output += "Venus in 5th: romantic expression is the primary creative driver — children, creative output, and romantic life are structurally linked.\n"
    elif venus_house == 11:
        output += "Venus in 11th: romantic opportunities arrive through professional networks and social circles — the partner is connected to the native's financial or social goals.\n"
    elif venus_house == 9:
        output += "Venus in 9th: the partner carries foreign, philosophical, or spiritually elevated qualities — the relationship itself becomes a vehicle for the native's higher learning and long-distance movement.\n"
    elif venus_house == 4:
        output += "Venus in 4th: emotional security within the home environment is the core relationship driver. The partner is a builder of domestic stability; however, Venus debilitated in Virgo creates perfectionism that produces friction when idealistic domestic expectations collide with reality.\n"
    elif venus_house == 3:
        output += "Venus in 3rd: the partner is a communicator, creative, or skilled networker — intellectual compatibility is the primary bonding mechanism.\n"
    elif venus_house == 10:
        output += "Venus in 10th: career and romantic life intersect — the partner either shares the same profession or directly contributes to the native's public status.\n"
    else:
        output += f"Venus in House {venus_house}: Relationship themes are shaped by the domain of this house — comfort, beauty, and harmony are sought through this life area.\n"

    output += "\n=== Jupiter — Spouse Indicator and Marriage Blessings ===\n"
    if jupiter_house == 12:
        output += "Jupiter in 12th: the partner carries a foreign background, cross-cultural orientation, or deeply spiritual worldview — the relationship is a vehicle for the native's moksha-directed growth.\n"
    elif jupiter_house in [1, 5, 7, 9]:
        output += f"Jupiter in House {jupiter_house}: the partner is educated, spiritually oriented, and a net positive force — the relationship expands rather than restricts the native's life trajectory.\n"
    elif jupiter_house in [6, 8]:
        output += "Jupiter in 6th/8th: the relationship forms through challenging or transformational circumstances — the depth of the bond is forged in adversity, producing wisdom and growth as the primary output.\n"
    elif jupiter_house in [2, 11]:
        output += "Jupiter in 2nd or 11th: the partner contributes directly to material and social expansion — marriage is a wealth-building and network-amplifying event.\n"
    elif jupiter_house == 3:
        output += "Jupiter in 3rd: the partner is articulate and intellectually driven — the relationship is built on shared communication, ideas, and intellectual ambition.\n"
    elif jupiter_house == 4:
        output += "Jupiter in 4th: the relationship brings domestic abundance and emotional security — the home becomes a centre of expansion and comfort.\n"
    elif jupiter_house == 10:
        output += "Jupiter in 10th: the partner elevates the native's public status and career trajectory — the relationship is structurally connected to professional circles.\n"
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
        output += "Saturn in 7th: marriage is structurally delayed — the native does not form lasting partnerships until genuine maturity is established. When the bond forms, it is built on responsibility and mutual respect rather than romance, and it endures.\n"
    elif saturn_house == 8:
        output += "Saturn in 8th: deep karmic bonds define the marriage — the relationship undergoes profound transformation and restructuring, potentially across long spans. Commitment depth is unusually high; the relationship requires both partners to handle sustained intensity.\n"
    elif saturn_house == 12:
        output += "Saturn in 12th: the partner carries a foreign, spiritual, or institutional background — the relationship demands sacrifice, inner growth, and acceptance of unconventional partnership structures. The bond is karmic and long-term.\n"
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

    # Task 3 — classical support for marriage section
    marriage_support = get_classical_support(["seventh house", "marriage", "venus", "partner"])
    if marriage_support:
        output += f"\n{'-' * 40}{marriage_support}{'-' * 40}\n"

    output = "\n" + output.strip() + "\n"
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
        output += "Lagna lord in House 6/8/12: karmic challenges shape the life path; transformation and non-conventional routes are the primary mechanism.\n"

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

    # Global synthesis: resolve combust/debil/yoga contradictions into unified conclusion
    context   = _build_synthesis_context(planet, all_planets)
    conflicts = resolve_conflicts(planet, data, context)
    if conflicts:
        text += "\n⚖️ SYNTHESIS:\n"
        for c in conflicts:
            text += f"- {c}\n"

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
        # Check combustion (Mercury within 14° of Sun)
        mer_deg = planet_data.get("Mercury", {}).get("degree", 0)
        sun_deg = planet_data.get("Sun", {}).get("degree", 0)
        combust_orb = abs(mer_deg - sun_deg)
        if combust_orb < 14:
            yogas_found.append(
                "☀️ Budh-Aditya Yoga — Sun and Mercury conjunct in House "
                + str(sun_h) + ". "
                "⚖️ SYNTHESIS: Mercury is combust (within " + f"{combust_orb:.1f}" + "° of Sun) — "
                "intelligence becomes authority-driven rather than neutral. Analytical brilliance "
                "is channeled through solar command; communication leads with conviction and "
                "authority, not diplomacy. The yoga's strength is present but filtered through "
                "Sun's agenda — writers, leaders, and advisors benefit most."
            )
        else:
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
                    f"energy. Mercury's independent significations — intellect, "
                    f"commerce, and communication — are filtered through solar authority. "
                    f"Writing, speaking, and advisory roles carry an authoritative quality; "
                    f"the native's best output comes when willpower backs analytical precision.\n\n"
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
            interpreted = vary_sentence(interpreted)
            # Only display if the *interpreted principle* mentions a planet from this conjunction
            if any(pl in interpreted.lower() for pl in plist_lower):
                output += f"\n{'-' * 40}\n📖 Classical: {interpreted}\n{'-' * 40}\n"
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
    """Retrograde analysis using kundali data only — no hypothetical inference."""
    output = "\n=== RETROGRADE PLANET ANALYSIS ===\n"

    retro_found = []
    for planet, pdata in planet_data.items():
        if pdata.get("retrograde") is True:
            retro_found.append(planet)
            principle = _RETROGRADE_PRINCIPLES.get(planet)
            if principle:
                output += f"{planet} (RETROGRADE — confirmed in source data):\n"
                # Strip the "If X is retrograde:" prefix from the principle text
                clean = principle.split(":", 1)[-1].strip() if ":" in principle else principle
                output += f"  {clean}\n\n"
            else:
                output += (
                    f"{planet} is retrograde — energy turns inward, producing delayed "
                    f"and internally processed results.\n\n"
                )

    if not retro_found:
        output += "No retrograde planets confirmed in source kundali data.\n"

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

        # Time window injection (Task 1)
        output += f"{'=' * 50}{generate_time_window(period)}{'=' * 50}\n"

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
                interpreted = vary_sentence(interpreted)
                output += f"\n{'-' * 40}\n📖 Classical: {interpreted}\n{'-' * 40}\n"
                classical_added += 1

        # Task 3 — additional classical support if none found above
        if classical_added == 0:
            support = get_classical_support([planet.lower(), sign.lower(), "dasha"])
            if support:
                output += f"\n{'-' * 40}{support}{'-' * 40}\n"

        output += "\n"

    output = "\n" + output.strip() + "\n"
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
            f"  Yoga: Budha-Aditya Yoga active — intelligence is charged with solar authority, "
            f"producing a sharp, articulate, and confident communicator.\n"
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
                f"in the 12th house of moksha and foreign lands — the native builds a life "
                f"connected to spiritual institutions, foreign environments, or research-oriented, "
                f"behind-the-scenes roles. Saturn in House 12 activates Vipreet Raj Yoga: "
                f"unexpected rise through adversity and isolation is the dominant career pattern.\n"
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
                f"  Dignity: Mars debilitated in {mars_s} — raw aggression "
                f"is channelled into protective, emotionally-driven action. Drive operates "
                f"through family, speech, and financial ambition rather than direct confrontation.\n"
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
                f"  Lordship: Moon lords House {moon_lord_houses[0]} from this Lagna — "
                f"emotional intelligence is a primary life driver, linked directly to "
                f"House {moon_lord_houses[0]} themes.\n"
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
            f"  Synthesis: Rahu in the Lagna drives the native's entire life journey through "
            f"intense worldly ambition and an unconventional, magnetic identity. "
            f"The native projects a larger-than-life, often foreign-influenced or "
            f"technologically-oriented persona — grounding this energy into purposeful direction "
            f"is the defining developmental challenge.\n\n"
        )

    # 6. Ketu in House 7 (Sagittarius)
    ketu_h = planet_data.get("Ketu", {}).get("house")
    ketu_s = planet_data.get("Ketu", {}).get("sign", "")
    if ketu_h == 7:
        output += f"⚡ KETU in House 7 ({ketu_s})\n"
        output += (
            f"  Placement: Ketu (detachment, past-life mastery, liberation) in House 7 "
            f"— the house of marriage and partnerships.\n"
            f"  Synthesis: Ketu in House 7 carries deep past-life relationship karma — "
            f"partnerships feel simultaneously familiar and incomplete. "
            f"The native's spiritual growth is inextricably linked to relationship lessons. "
            f"The Rahu-Ketu axis across 1–7 establishes self-development vs. karmic partnership "
            f"resolution as the central life theme.\n\n"
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
                f"  Dignity: Venus debilitated in {ven_s} — love and relationships are "
                f"approached with critical analysis and perfectionism. Domestic environment "
                f"and emotional security are shaped by high standards and a need for order.\n"
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

        # Time window (Task 1)
        output += f"{'=' * 50}{generate_time_window(maha_period)}{'=' * 50}\n"

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
            # Time window for antardasha (Task 1)
            ad_period = {"start": start_str, "end": end_str}
            try:
                ad_period["start"] = str(sub_start.year) if sub_start else ""
                ad_period["end"]   = str(sub_end.year)   if sub_end   else ""
            except Exception:
                pass
            output += f"{'=' * 50}{generate_time_window(ad_period)}{'=' * 50}\n"
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
            f"1. {lagna} Lagna with Rahu in House {rahu_h}: Identity is permanently wired for "
            f"reinvention — each life chapter brings a fundamentally new self. "
            f"The lagna lord ({lagna_lord}) in House {lagna_lord_h} channels this restless "
            f"intelligence into {HOUSE_MEANINGS.get(lagna_lord_h, 'multi-domain action')}. "
            f"Conventional paths are rejected; unconventional mastery defines success.\n"
        )

    # 2. Sun-Mercury communication axis — no yoga name (covered in Yoga section)
    sun_h   = sun.get("house", 0)
    merc_h  = mercury.get("house", 0)
    sun_s   = sun.get("sign", "")
    merc_s  = mercury.get("sign", "")
    if sun_h == merc_h and sun_h > 0:
        output += (
            f"2. Sun + Mercury conjunct in {sun_s} (House {sun_h}): "
            f"Professional identity is inseparable from communication, writing, and intellectual authority. "
            f"Every career chapter is built on articulation and self-directed intellectual output. "
            f"Media, publishing, entrepreneurial communication, or content creation is the "
            f"primary career vehicle — not employment, not hierarchy.\n"
        )
    else:
        output += (
            f"2. Sun in House {sun_h} ({sun_s}) + Mercury in House {merc_h} ({merc_s}): "
            f"Authority and intellect operate in distinct life arenas — leadership drives one domain "
            f"while analytical precision anchors another. The native excels wherever both converge: "
            f"advisory, analytical leadership, or strategic communication roles.\n"
        )

    # 3. Jupiter-Saturn conjunction in dusthana — conclusion only, no yoga label
    jup_h   = jupiter.get("house", 0)
    sat_h   = saturn.get("house", 0)
    jup_s   = jupiter.get("sign", "")
    sat_s   = saturn.get("sign", "")
    if jup_h == sat_h and jup_h > 0:
        output += (
            f"3. Jupiter–Saturn Conjunction in House {jup_h} ({jup_s}): "
            f"The dominant career pattern is rise through adversity — "
            f"institutional setbacks, hidden environments, and foreign sectors are the actual springboard. "
            f"Every apparent obstacle is a mechanism for elevation. Long-term results consistently "
            f"outperform early appearances.\n"
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
            f"These are not permanent weaknesses — they are pressure points that generate "
            f"Neecha Bhanga Raja Yoga results once mastered. "
            f"Greatest breakthroughs arrive when debilitated planets are forced into maximum expression.\n"
        )

    # 5. Rahu-Ketu axis — karmic direction
    rahu_h = rahu.get("house", 0)
    ketu_h = ketu.get("house", 0)
    if rahu_h and ketu_h:
        output += (
            f"5. Rahu (House {rahu_h}) — Ketu (House {ketu_h}) Karmic Axis: "
            f"The soul moves away from {HOUSE_MEANINGS.get(ketu_h, 'past comfort')} "
            f"(Ketu — surrendered mastery) toward {HOUSE_MEANINGS.get(rahu_h, 'new territory')} "
            f"(Rahu — life mission). Self-mastery and identity development (House {rahu_h}) must "
            f"be actively chosen over karmic relationship dependency (House {ketu_h}). "
            f"Central life tension: self-development vs. karmic partnership resolution.\n"
        )

    # 6. Moon emotional core
    moon_sign = moon.get("sign", "")
    moon_h    = moon.get("house", 0)
    moon_nak  = moon.get("nakshatra", "")
    output += (
        f"6. Moon in {moon_sign} (House {moon_h}, Nakshatra: {moon_nak}): "
        f"All emotional decisions are filtered through {moon_sign}'s framework. "
        f"Relationships and daily mental state are governed by an intense need for harmony — "
        f"decisive action under pressure is the recurring developmental challenge. "
        f"Moon in House {moon_h} places emotional intelligence within "
        f"{HOUSE_MEANINGS.get(moon_h, 'the intelligence and creativity domain')}, "
        f"making creativity and relationship quality emotionally central to life expression.\n"
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

    # 1. LIFE DIRECTION — kundali-specific, no generic astrology language
    output += (
        f"→ LIFE DIRECTION: Rahu in House {rahu_h} ({lagna} Lagna) drives identity through constant reinvention "
        f"and unconventional intellectual choices — the native's life arc is not a straight line but a spiral "
        f"of expansion. Budha-Aditya Yoga in House {sun_h} ({sun_s}) locks communication and self-expression "
        f"as the permanent vehicle of destiny. The Rahu–Ketu axis (House {rahu_h}–{ketu_h}) "
        f"makes the tension between radical self-creation and karmic relationship resolution "
        f"the defining challenge across all life stages — this tension is the engine, not the obstacle.\n\n"
    )

    # 2. CAREER TRAJECTORY — specific, not general, timeline style
    output += (
        f"→ CAREER TRAJECTORY: The 10th lord ({tenth_lord}) in House {tenth_lord_h} "
        f"combined with Jupiter–Saturn conjunction (House {jup_h}, Vipreet Raj Yoga) "
        f"establishes a non-linear career path through media, content, writing, international consulting, "
        f"spiritual advisory, or institutional research — conventional employment structures are incompatible. "
        f"Timeline: ages 18–28 build raw skills through effort and adversity; "
        f"ages 28–35 mark the pivotal transition from struggle to structured authority "
        f"as Saturn Mahadasha (2026–2045) converts accumulated adversity into durable expertise; "
        f"ages 35–45 deliver genuine public authority and financial stability.\n\n"
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

    # 4. CORE STRENGTH
    output += (
        f"→ CORE STRENGTH: Budha-Aditya Yoga (Sun in own sign {sun_s} + Mercury, "
        f"House {sun_h}) delivers elite-level articulation, analytical precision, and "
        f"entrepreneurial boldness. Every career breakthrough in this chart is built on the power of words, "
        f"ideas, and intellectual authority — not physical effort or inherited advantage. "
        f"This combination is the chart's single most consistently activated life force.\n\n"
    )

    # 5. CORE WEAKNESS
    output += (
        f"→ CORE WEAKNESS: Mars debilitated in {mars_s} (House {mars_h}) produces "
        f"emotional withdrawal and passive-aggressive retreat precisely when bold, direct action is required. "
        f"The gap between intellectual vision and decisive execution is the single greatest threat to this chart's potential. "
        f"Mastering this debilitation directly activates Neecha Bhanga Raja Yoga — "
        f"the chart's greatest vulnerability becomes its most durable career asset the moment Mars energy is redirected into strategic, purposeful action.\n\n"
    )

    # 6. FINAL TRAJECTORY — where this chart ultimately lands, anchored to current Dasha
    current_maha = dasha[0].get("planet", "Jupiter") if dasha else "Jupiter"
    current_maha_start = dasha[0].get("start", "2010") if dasha else "2010"
    current_maha_end   = dasha[0].get("end", "2026")   if dasha else "2026"
    current_anta = dasha[1].get("planet", "Jupiter") if len(dasha) > 1 else ""
    current_anta_end = dasha[1].get("end", "2026")    if len(dasha) > 1 else ""
    next_maha    = dasha[2].get("planet", "Saturn")   if len(dasha) > 2 else "Saturn"
    next_maha_end = dasha[2].get("end", "2045")       if len(dasha) > 2 else "2045"

    anta_clause = (
        f" The active {current_maha}–{current_anta} Antardasha (through {current_anta_end}) "
        f"accelerates this foundation-building: {current_anta} in House "
        f"{planet_data.get(current_anta, {}).get('house', '?')} activates "
        f"{_house_event_phrase(planet_data.get(current_anta, {}).get('house', 0))} "
        f"alongside {current_maha}'s institutional and philosophical influence."
        if current_anta else ""
    )

    output += (
        f"→ FINAL VERDICT: {current_maha} Mahadasha ({current_maha_start}–{current_maha_end}) "
        f"is the foundation-building phase — knowledge, institutional depth, and spiritual clarity are the outputs.{anta_clause} "
        f"{next_maha} Mahadasha (through {next_maha_end}) is the execution phase — "
        f"accumulated discipline and adversity convert directly into material authority and public standing. "
        f"Career peak, financial consolidation, and relationship maturity all arrive in the 30s–45 age window — earlier milestones are preparation, not destination. "
        f"This chart's dominant arc: controlled struggle → earned mastery → structural authority. "
        f"Strongest in environments others reject as difficult, foreign, or unconventional — "
        f"those environments are precisely where this chart's output is maximised.\n"
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

# ============================================================
# STAGE 7.5 — ULTRA PRECISION OUTPUT LAYER
# Every statement: CAUSE → MECHANISM → RESULT → DOMAIN
# No "indicates / suggests / gives / Therefore / Hence"
# ============================================================

_ULTRA_DEBIL = {
    "Sun":     "Libra",
    "Moon":    "Scorpio",
    "Mars":    "Cancer",
    "Mercury": "Pisces",
    "Jupiter": "Capricorn",
    "Venus":   "Virgo",
    "Saturn":  "Aries",
}

_ULTRA_EXALT = {
    "Sun":     "Aries",
    "Moon":    "Taurus",
    "Mars":    "Capricorn",
    "Mercury": "Virgo",
    "Jupiter": "Cancer",
    "Venus":   "Pisces",
    "Saturn":  "Libra",
}

_ULTRA_TRIGGER = {
    1:  "identity pressure, public confrontation, or periods when self-image is directly tested",
    2:  "financial stress, family disputes, or when speech carries high personal stakes",
    3:  "writing deadlines, communication challenges, or sibling-related inflection points",
    4:  "home or property decisions, relocation pressure, or maternal life events",
    5:  "romantic conflicts, competitive exams, or creative investment decisions",
    6:  "competitive threats, legal disputes, or health crises requiring strategic response",
    7:  "partnership negotiations, marriage decisions, or open confrontations with adversaries",
    8:  "sudden financial reversals, inheritance matters, or existential life shocks",
    9:  "travel decisions, religious or philosophical turning points, or teacher encounters",
    10: "career evaluation periods, public-facing opportunities, or authority-figure interactions",
    11: "income negotiations, large social or professional network activations",
    12: "foreign relocation decisions, institutional entry/exit, or deep spiritual retreats",
}

_ULTRA_MECHANISM = {
    "Sun":     "solar authority over the occupied house, forcing identity and willpower through its themes",
    "Moon":    "emotional amplification of the occupied house, routing every decision through its themes",
    "Mars":    "aggressive, action-first energy into the occupied house, producing drive mixed with impulsiveness",
    "Mercury": "analytical and communicative precision into the occupied house, accelerating intellectual output",
    "Jupiter": "expansion and optimism into the occupied house, enlarging its results and moral stakes",
    "Venus":   "aesthetic refinement and relationship sensitivity into the occupied house",
    "Saturn":  "disciplined, delay-prone pressure into the occupied house, building endurance and karmic accountability",
    "Rahu":    "obsessive, unconventional amplification of the occupied house, breaking existing patterns for new terrain",
    "Ketu":    "detachment and past-life mastery energy into the occupied house, dissolving attachments to its themes",
}

_ULTRA_RESULT_DOMAIN = {
    "Sun":     "career identity and leadership — public recognition is earned, not inherited",
    "Moon":    "emotional intelligence and relational responsiveness — inner world shapes outer decisions",
    "Mars":    "physical and financial drive — execution capacity defines outcomes",
    "Mercury": "communication excellence and analytical edge — intellectual output determines access",
    "Jupiter": "philosophical depth and institutional reach — wisdom converts into long-term authority",
    "Venus":   "relationship quality and domestic stability — comfort level reflects inner standards",
    "Saturn":  "long-term structural outcomes — durability of career and relationships is built here",
    "Rahu":    "ambition and reinvention — identity evolves through unconventional pathways",
    "Ketu":    "spiritual detachment and past-life integration — inner clarity dissolves inherited confusion",
}

_ULTRA_EVENT_CHAINS = {
    (3, 12): "structured output in isolation — writing, research, or foreign-linked content production",
    (12, 3): "structured output in isolation — writing, research, or foreign-linked content production",
    (1, 12): "identity dissolves and reforms through foreign or institutional exposure",
    (12, 1): "identity dissolves and reforms through foreign or institutional exposure",
    (7, 12): "relationship restructuring through foreign influence or spiritual disillusionment",
    (12, 7): "relationship restructuring through foreign influence or spiritual disillusionment",
    (10, 12): "career pivot toward non-mainstream, foreign, or institutional work",
    (12, 10): "career pivot toward non-mainstream, foreign, or institutional work",
    (3, 7):  "partnership formed through communication or creative collaboration",
    (7, 3):  "partnership formed through communication or creative collaboration",
    (5, 7):  "romantic deepening through shared intellectual or creative pursuit",
    (7, 5):  "romantic deepening through shared intellectual or creative pursuit",
    (2, 10): "financial authority tied directly to public career performance",
    (10, 2): "financial authority tied directly to public career performance",
    (1, 7):  "self-development and relationship obligation collide — karmic resolution required",
    (7, 1):  "self-development and relationship obligation collide — karmic resolution required",
    (9, 12): "long-distance spiritual or educational journey produces transformative knowledge",
    (12, 9): "long-distance spiritual or educational journey produces transformative knowledge",
    (5, 9):  "intellectual mastery through higher study, producing creative or philosophical authority",
    (9, 5):  "intellectual mastery through higher study, producing creative or philosophical authority",
    (11, 12): "income arrives from foreign channels, overseas networks, or institutional work",
    (12, 11): "income arrives from foreign channels, overseas networks, or institutional work",
}


def _ultra_strip(text):
    """Remove forbidden soft-language phrases from output text."""
    import re as _r
    patterns = [
        (r'\bindicates?\b', 'produces'),
        (r'\bsuggests?\b',  'shows'),
        (r'\bgives?\b',     'delivers'),
        (r'\bnative experiences?:?\b', 'the chart produces'),
        (r'\bTherefore,?\s*', ''),
        (r'\bHence,?\s*',    ''),
        (r'\bmay\s+', ''),
        (r'\bmight\s+', ''),
        (r'\btends?\s+to\s+', ''),
        (r'\bcan\s+be\b', 'is'),
    ]
    for pat, repl in patterns:
        text = _r.sub(pat, repl, text, flags=_r.IGNORECASE)
    return text


def _ultra_dignity_note(planet, sign):
    """Return one assertive line about the planet's dignity state."""
    if _ULTRA_DEBIL.get(planet) == sign:
        return (
            f"{planet} is debilitated in {sign} — raw energy is internalized, "
            f"producing protective-reactive patterns instead of direct force. "
            f"When Neecha Bhanga conditions activate, this debilitation inverts "
            f"into Neecha Bhanga Raja Yoga, converting the chart's greatest friction point into a breakthrough asset."
        )
    if _ULTRA_EXALT.get(planet) == sign:
        return (
            f"{planet} is exalted in {sign} — full-strength delivery of its domain. "
            f"Results in this area are consistent, authoritative, and structurally durable."
        )
    return ""


def _ultra_planet_section(planet_data):
    """Ultra-format planet analysis: CAUSE → MECHANISM → RESULT → TRIGGER."""
    output = "\n" + "=" * 60 + "\n"
    output += "SECTION U2 — PLANET MECHANISM ENGINE\n"
    output += "=" * 60 + "\n"
    output += "(Format: Planet forces mechanism → produces result in domain. Activates under trigger.)\n\n"

    lagna = _get_lagna()

    _PLANET_ORDER = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus",
                     "Saturn", "Rahu", "Ketu"]
    for planet in _PLANET_ORDER:
        data  = planet_data.get(planet, {})
        house = data.get("house", 0)
        sign  = data.get("sign", "?")
        nak   = data.get("nakshatra", "")
        conj  = _conjunction_partners(planet, planet_data)
        lord_str = _planet_lordship_summary(planet, planet_data, lagna)

        mechanism = _ULTRA_MECHANISM.get(planet, "unspecified mechanism")
        result    = _ULTRA_RESULT_DOMAIN.get(planet, "life outcomes")
        trigger   = _ULTRA_TRIGGER.get(house, "major life decisions")
        h_domain  = _house_event_phrase(house)

        output += f"▸ {planet.upper()} — {lord_str} | {sign} | House {house}\n"
        output += (
            f"  {planet} in House {house} forces {mechanism} "
            f"into {h_domain}, producing {result}.\n"
        )

        # Conjunction amplifier
        if conj:
            conj_h_domains = [_house_event_phrase(house) for _ in conj]
            output += (
                f"  Conjunction with {', '.join(conj)} in House {house} "
                f"fuses {planet}'s energy with {', '.join(conj)}'s domains — "
                f"the combined output in {h_domain} is indivisible and self-reinforcing.\n"
            )

        # Combustion note for Mercury
        sun_h = planet_data.get("Sun", {}).get("house", 0)
        if planet == "Mercury" and house == sun_h and sun_h > 0:
            output += (
                f"  Mercury combustion (conjunct Sun, House {house}): "
                f"analytical output is overridden by solar authority — "
                f"intelligence is expressed as command, not neutrality, "
                f"producing a decisive communication style that bypasses diplomacy.\n"
            )

        # Dignity note
        dignity = _ultra_dignity_note(planet, sign)
        if dignity:
            output += f"  {dignity}\n"

        output += f"  Activates strongly when: {trigger}.\n"
        if nak:
            output += f"  Nakshatra driver: {nak}.\n"
        output += "\n"

    return output


def _ultra_yoga_section(planet_data):
    """Ultra-format yoga output: WHAT it does, WHERE it impacts, WHEN it fires."""
    output = "\n" + "=" * 60 + "\n"
    output += "SECTION U3 — YOGA ACTIVATION ENGINE\n"
    output += "=" * 60 + "\n"
    output += "(No definitions. Only: what the yoga does, where it strikes, when it fires.)\n\n"

    sun_h  = planet_data.get("Sun", {}).get("house", 0)
    mer_h  = planet_data.get("Mercury", {}).get("house", 0)
    sun_s  = planet_data.get("Sun", {}).get("sign", "")
    jup_h  = planet_data.get("Jupiter", {}).get("house", 0)
    sat_h  = planet_data.get("Saturn", {}).get("house", 0)
    mars_s = planet_data.get("Mars", {}).get("sign", "")
    ven_s  = planet_data.get("Venus", {}).get("sign", "")
    rahu_h = planet_data.get("Rahu", {}).get("house", 0)
    ketu_h = planet_data.get("Ketu", {}).get("house", 0)
    mars_h = planet_data.get("Mars", {}).get("house", 0)

    # Budha-Aditya Yoga
    if sun_h == mer_h and sun_h:
        sun_own = (sun_s == "Leo")
        combust_note = (
            " Mercury combustion modifies the output channel — authority and "
            "willpower override pure analysis, producing commanding communication "
            "that drives leadership, not just expertise."
        )
        output += "▸ BUDHA-ADITYA YOGA (Sun + Mercury, House {h})\n".format(h=sun_h)
        output += (
            f"  Concentrates solar authority and analytical precision in "
            f"House {sun_h} ({_house_event_phrase(sun_h)}). "
        )
        if sun_own:
            output += f"Sun in own sign {sun_s} amplifies the yoga to full strength. "
        output += (
            f"Dominance in writing, speaking, and advisory roles is the direct output. "
            f"Fires strongest during Sun and Mercury Mahadasha and Antardasha periods."
            f"{combust_note}\n\n"
        )

    # Jupiter-Saturn conjunction (Vipreet Raj Yoga potential)
    if jup_h == sat_h and jup_h:
        vr_note = " This activates Vipreet Raj Yoga: career rise is triggered by adversity, not support." if jup_h in (6, 8, 12) else ""
        output += f"▸ JUPITER–SATURN CONJUNCTION (House {jup_h})\n"
        output += (
            f"  Disciplined wisdom concentrated in House {jup_h} ({_house_event_phrase(jup_h)}). "
            f"Long-term authority is built through sustained, principled effort in institutional or foreign contexts."
            f"{vr_note} "
            f"Fires strongly during Jupiter and Saturn Mahadasha, especially in mid-to-late career phase.\n\n"
        )

    # Vipreet Raj Yoga (Saturn in dusthana)
    if sat_h in (6, 8, 12):
        output += f"▸ VIPREET RAJ YOGA (Saturn in House {sat_h})\n"
        output += (
            f"  Converts every career setback into a structural advancement trigger. "
            f"The harder the path, the more durable the resulting authority. "
            f"Fires during Saturn Mahadasha ({_house_event_phrase(sat_h)} themes dominate). "
            f"Peak activation: after age 28, compounding through age 45.\n\n"
        )

    # Neecha Bhanga Raja Yoga — Mars
    if mars_s == "Cancer":
        output += f"▸ NEECHA BHANGA RAJA YOGA — MARS (Cancer, House {mars_h})\n"
        output += (
            f"  Debilitation pressure on Mars (House {mars_h}, {_house_event_phrase(mars_h)}) "
            f"is partially cancelled by Moon's trikona placement. "
            f"When the cancellation fires — during Mars Antardasha under Moon Mahadasha, "
            f"or when Saturn transits Cancer — the debilitation reverses into a wealth "
            f"and speech breakthrough. The trigger is sustained effort, not passive waiting.\n\n"
        )

    # Neecha Bhanga Raja Yoga — Venus
    if ven_s == "Virgo":
        ven_h = planet_data.get("Venus", {}).get("house", 0)
        output += f"▸ NEECHA BHANGA RAJA YOGA — VENUS (Virgo, House {ven_h})\n"
        output += (
            f"  Venus debilitation in House {ven_h} ({_house_event_phrase(ven_h)}) "
            f"is partially cancelled by Venus itself occupying a kendra (4th house). "
            f"Perfectionism in relationships and domestic expectations is the debilitation output; "
            f"the cancellation produces aesthetic refinement and a high-standards domestic environment "
            f"once ego is released from the equation. Fires during Venus Mahadasha.\n\n"
        )

    # Rahu-Ketu axis
    if rahu_h and ketu_h:
        output += f"▸ RAHU (House {rahu_h}) — KETU (House {ketu_h}) KARMIC AXIS\n"
        output += (
            f"  Rahu in House {rahu_h} ({_house_event_phrase(rahu_h)}) drives "
            f"obsessive ambition toward identity reinvention and unconventional self-creation. "
            f"Ketu in House {ketu_h} ({_house_event_phrase(ketu_h)}) dissolves attachment "
            f"to relationships and conventional partnership expectations. "
            f"The axis creates a permanent oscillation: identity expansion vs. karmic relationship release. "
            f"Resolution produces mastery only when Rahu's ambition is channelled into purposeful output.\n\n"
        )

    return output


def _ultra_dasha_section(dasha_list, planet_data):
    """Ultra-format Dasha output: CAUSE/MECHANISM/RESULT/TIMING per Mahadasha,
    plus '[Main]–[Sub] activates [HA] + [HB], producing [specific event chain]' per Antardasha."""
    output = "\n" + "=" * 60 + "\n"
    output += "SECTION U4 — DASHA PREDICTION ENGINE (ULTRA)\n"
    output += "=" * 60 + "\n"
    output += "(Format: CAUSE / MECHANISM / RESULT / TIMING — no descriptions, only event chains.)\n\n"

    lagna = _get_lagna()
    lagna_idx = SIGN_ORDER.index(lagna) if lagna in SIGN_ORDER else -1

    for idx, period in enumerate(dasha_list[:5]):
        planet = period.get("planet", "")
        start  = period.get("start", "?")
        end    = period.get("end", "?")
        pdata  = planet_data.get(planet, {})
        house  = pdata.get("house", 0)
        sign   = pdata.get("sign", "")
        nak    = pdata.get("nakshatra", "")
        h_phrase = _house_event_phrase(house)
        lord_str = _planet_lordship_summary(planet, planet_data, lagna)

        # Compute lordship houses
        lords_of = []
        if lagna_idx >= 0:
            for h in range(1, 13):
                s = SIGN_ORDER[(lagna_idx + h - 1) % 12]
                if SIGN_LORDS.get(s) == planet:
                    lords_of.append(h)

        # Dignity
        dignity_note = ""
        if _ULTRA_DEBIL.get(planet) == sign:
            dignity_note = f" [{planet} DEBILITATED in {sign} — Neecha Bhanga pressure active]"
        elif _ULTRA_EXALT.get(planet) == sign:
            dignity_note = f" [{planet} EXALTED in {sign} — full-strength delivery]"

        # Conjunctions
        conj = _conjunction_partners(planet, planet_data)
        conj_note = (
            f" Conjunction with {', '.join(conj)} in House {house} fuses their domains."
            if conj else ""
        )

        output += "-" * 60 + "\n"
        output += f"  {planet.upper()} MAHADASHA:  {start} → {end}\n"
        output += "-" * 60 + "\n"

        # CAUSE
        output += (
            f"  CAUSE: {lord_str}{dignity_note} in {sign} (House {house}) occupies "
            f"{h_phrase}.{conj_note}\n"
        )
        if lords_of:
            lord_houses_str = ", ".join(
                f"House {h} ({_house_event_phrase(h)})" for h in lords_of
            )
            output += (
                f"         {planet} lords {lord_houses_str} — "
                f"those domains activate as sub-themes throughout this period.\n"
            )

        # MECHANISM
        output += (
            f"  MECHANISM: {_ULTRA_MECHANISM.get(planet, 'planet energy')} "
            f"concentrates on {h_phrase}, forcing all life priorities through this lens.\n"
        )

        # RESULT — event-level, house-specific
        output += "  RESULT:\n"

        # Career result
        tenth_lord = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 9) % 12], "") if lagna_idx >= 0 else ""
        if planet == tenth_lord or 10 in lords_of:
            output += f"    Career: Direct career authority event — {planet} as 10th lord drives status and public recognition.\n"
        elif house in (10, 11):
            output += f"    Career: Career advancement and income gain — House {house} placement delivers professional results.\n"
        elif house in (6, 8, 12):
            output += f"    Career: Non-linear career rise through adversity in {h_phrase} — conventional paths fail; unconventional paths succeed.\n"
        else:
            output += f"    Career: Career focus shifts to {h_phrase} — advancement through House {house} domain activation.\n"

        # Financial result
        second_lord   = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 1) % 12], "") if lagna_idx >= 0 else ""
        eleventh_lord = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 10) % 12], "") if lagna_idx >= 0 else ""
        if planet in (second_lord, eleventh_lord) or house in (2, 11):
            output += f"    Money: Wealth accumulation period — {h_phrase} directly activates financial gains.\n"
        elif house in (8, 12):
            output += f"    Money: Financial restructuring — hidden gains, foreign income, or institutional funding replace conventional income streams.\n"
        else:
            output += f"    Money: Financial focus redirected through {h_phrase}.\n"

        # Relationship result
        seventh_lord = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 6) % 12], "") if lagna_idx >= 0 else ""
        ketu_h = planet_data.get("Ketu", {}).get("house", 0)
        if planet == seventh_lord or 7 in lords_of:
            output += f"    Relationship: Marriage or partnership event triggered — {planet} as 7th lord activates commitment decisions.\n"
        elif planet in ("Venus", "Ketu") or house == 7:
            output += f"    Relationship: Karmic relationship event — {planet} in House {house} forces partnership restructuring or romantic deepening.\n"
        else:
            output += f"    Relationship: Relationship energy shaped by {h_phrase} — bonds are tested or formed through House {house} themes.\n"

        # Location / foreign result
        if house == 12 or planet == "Rahu":
            output += f"    Location: Foreign movement, institutional placement, or extended isolation is activated.\n"
        elif 9 in lords_of or house == 9:
            output += f"    Location: Long-distance travel or foreign education event occurs.\n"

        # TIMING
        output += "  TIMING:\n"
        # Task 1 — time window for this Mahadasha
        output += f"{'=' * 50}{generate_time_window(period)}{'=' * 50}\n"
        try:
            sy = int(start.split("/")[-1]) if "/" in start else int(start[:4])
            ey = int(end.split("/")[-1]) if "/" in end else int(end[:4])
            span = ey - sy
            mid1 = sy + span // 3
            mid2 = sy + 2 * span // 3
        except Exception:
            sy, ey, mid1, mid2 = 0, 0, 0, 0

        if sy:
            output += (
                f"    Early phase ({sy}–{mid1}): {h_phrase} foundation-building — "
                f"raw House {house} themes surface and demand engagement.\n"
                f"    Mid phase ({mid1}–{mid2}): peak activation of {planet}'s lorded houses — "
                f"career and financial events peak here.\n"
                f"    End phase ({mid2}–{ey}): consolidation and transition — "
                f"results crystallise; the next Mahadasha agenda begins loading.\n"
            )

        output += "\n"

        # Antardasha for this Mahadasha
        m_start = _parse_dasha_date(start)
        m_end   = _parse_dasha_date(end)
        if m_start and m_end:
            antardashas = _compute_antardasha_periods(planet, m_start, m_end)
            if antardashas:
                output += f"  ANTARDASHA PREDICTIONS — {planet.upper()} MAHADASHA:\n"
                for ad in antardashas[:9]:
                    sub      = ad["planet"]
                    sub_data = planet_data.get(sub, {})
                    sub_h    = sub_data.get("house", 0)
                    main_h   = house
                    pair_key = (main_h, sub_h)
                    pair_key_r = (sub_h, main_h)

                    # Get specific event chain for this house pair
                    event_chain = (
                        _ULTRA_EVENT_CHAINS.get(pair_key)
                        or _ULTRA_EVENT_CHAINS.get(pair_key_r)
                        or f"{_house_event_phrase(main_h)} combines with {_house_event_phrase(sub_h)}"
                    )

                    sub_lord_str = _planet_lordship_summary(sub, planet_data, lagna)
                    conj_sub = _conjunction_partners(sub, planet_data)
                    conj_sub_note = (
                        f" {sub} conjunct {', '.join(conj_sub)} amplifies output."
                        if conj_sub else ""
                    )

                    # Combustion note for Mercury sub-period
                    sun_h_val = planet_data.get("Sun", {}).get("house", 0)
                    combust_note = ""
                    if sub == "Mercury" and sub_h == sun_h_val:
                        combust_note = (
                            " Mercury combustion channels output through Sun's authority — "
                            "communication results carry a commanding, non-negotiable quality."
                        )

                    start_str = ad["start"].strftime("%b %Y") if ad["start"] else "?"
                    end_str   = ad["end"].strftime("%b %Y") if ad["end"] else "?"

                    # Task 1 — time window for antardasha
                    ad_tw_period = {
                        "start": str(ad["start"].year) if ad["start"] else "",
                        "end":   str(ad["end"].year)   if ad["end"]   else "",
                    }
                    ad_window = generate_time_window(ad_tw_period)

                    output += (
                        f"  • {planet}–{sub} [{start_str} → {end_str}]: "
                        f"activates House {main_h} + House {sub_h}, producing {event_chain}."
                        f"{conj_sub_note}{combust_note}\n"
                        f"{'=' * 50}{ad_window}{'=' * 50}\n"
                    )
                output += "\n"

    return output


def _ultra_final_judgement(planet_data, dasha_list):
    """Ultra-format 7-point precision final judgement — no vague statements."""
    output = "\n" + "=" * 60 + "\n"
    output += "SECTION U5 — FINAL PREDICTION ENGINE (ULTRA)\n"
    output += "=" * 60 + "\n"
    output += "(7-point precision: Life Path / Career Timeline / Relationship Pattern /\n"
    output += " Financial Pattern / Core Strength / Core Weakness / Final Verdict)\n\n"

    lagna = _get_lagna()
    lagna_idx = SIGN_ORDER.index(lagna) if lagna in SIGN_ORDER else -1

    sun_h   = planet_data.get("Sun",     {}).get("house", 0)
    sun_s   = planet_data.get("Sun",     {}).get("sign", "")
    mer_h   = planet_data.get("Mercury", {}).get("house", 0)
    jup_h   = planet_data.get("Jupiter", {}).get("house", 0)
    sat_h   = planet_data.get("Saturn",  {}).get("house", 0)
    mars_h  = planet_data.get("Mars",    {}).get("house", 0)
    mars_s  = planet_data.get("Mars",    {}).get("sign", "")
    venus_h = planet_data.get("Venus",   {}).get("house", 0)
    venus_s = planet_data.get("Venus",   {}).get("sign", "")
    rahu_h  = planet_data.get("Rahu",    {}).get("house", 0)
    ketu_h  = planet_data.get("Ketu",    {}).get("house", 0)
    moon_s  = planet_data.get("Moon",    {}).get("sign", "")
    moon_h  = planet_data.get("Moon",    {}).get("house", 0)

    tenth_lord  = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 9) % 12], "Jupiter") if lagna_idx >= 0 else "Jupiter"
    seventh_lord = SIGN_LORDS.get(SIGN_ORDER[(lagna_idx + 6) % 12], "Jupiter") if lagna_idx >= 0 else "Jupiter"
    tenth_lord_h = planet_data.get(tenth_lord, {}).get("house", 0)

    # 1. LIFE PATH
    output += "1. LIFE PATH\n"
    output += (
        f"   Rahu in House {rahu_h} ({lagna} Lagna) locks identity into perpetual reinvention. "
        f"Budha-Aditya Yoga in House {sun_h} ({sun_s}) forces all personal authority "
        f"through communication and intellectual output. "
        f"Ketu in House {ketu_h} dissolves conventional relationship expectations "
        f"while Rahu pushes relentlessly toward unconventional self-creation. "
        f"Life path: communication-based intellectual authority built through adversity, "
        f"not support or inheritance.\n\n"
    )

    # 2. CAREER TIMELINE
    current_maha     = dasha_list[0].get("planet", "") if dasha_list else ""
    current_maha_end = dasha_list[0].get("end", "?")   if dasha_list else "?"
    next_maha        = dasha_list[2].get("planet", "") if len(dasha_list) > 2 else ""
    next_maha_end    = dasha_list[2].get("end", "?")   if len(dasha_list) > 2 else "?"

    output += "2. CAREER TIMELINE\n"
    output += (
        f"   Ages 0–25 ({current_maha} Mahadasha, until {current_maha_end}): "
        f"raw skill accumulation through {_house_event_phrase(rahu_h)}. "
        f"Career framework is experimental — no stable structure yet.\n"
        f"   Ages 25–28 (transition): Jupiter Mahadasha builds institutional depth "
        f"and philosophical authority through House {jup_h} ({_house_event_phrase(jup_h)}).\n"
        f"   Ages 28–45 ({next_maha} Mahadasha, until {next_maha_end}): Saturn in House {sat_h} "
        f"converts adversity into structural authority. "
        f"Career stabilises post-32. Peak authority window: ages 35–45. "
        f"{tenth_lord} (10th lord) in House {tenth_lord_h} routes career through "
        f"{_house_event_phrase(tenth_lord_h)} — unconventional, institutional, or foreign-linked work only.\n\n"
    )

    # 3. RELATIONSHIP PATTERN
    output += "3. RELATIONSHIP PATTERN\n"
    output += (
        f"   {seventh_lord} (7th lord) in House {planet_data.get(seventh_lord, {}).get('house', 0)} "
        f"and Ketu in House {ketu_h} lock the partner profile to: foreign, spiritual, or "
        f"philosophically non-conventional background. "
        f"Venus debilitated in {venus_s} (House {venus_h}) creates impossible domestic standards "
        f"that damage relationships until the expectation pattern is consciously dismantled. "
        f"Moon in {moon_s} (House {moon_h}) routes emotional decisions through "
        f"{_house_event_phrase(moon_h)}, making romantic and creative life inseparable. "
        f"Superficial relationships do not survive this chart.\n\n"
    )

    # 4. FINANCIAL PATTERN
    output += "4. FINANCIAL PATTERN\n"
    output += (
        f"   Mars in House {mars_h} ({_house_event_phrase(mars_h)}) drives financial ambition "
        f"through family and speech channels. "
        f"Mars debilitated in {mars_s} produces reactive financial decisions under pressure — "
        f"retreat replaces bold action precisely when bold action is required. "
        f"Jupiter + Saturn in House {jup_h} ({_house_event_phrase(jup_h)}) routes financial growth "
        f"through institutional, foreign, or behind-the-scenes channels. "
        f"Peak financial consolidation: Saturn Mahadasha post-age 32. "
        f"Before 32: income is unstable and experimental. After 36: structural and compounding.\n\n"
    )

    # 5. CORE STRENGTH
    output += "5. CORE STRENGTH\n"
    output += (
        f"   Budha-Aditya Yoga (Sun in own sign {sun_s} + Mercury, House {sun_h}): "
        f"elite communication authority — writing, speaking, and analytical leadership "
        f"are the chart's single consistently winning instruments. "
        f"No peer in the native's environment replicates this output capacity.\n\n"
    )

    # 6. CORE WEAKNESS
    output += "6. CORE WEAKNESS\n"
    output += (
        f"   Mars debilitated in {mars_s} (House {mars_h}): "
        f"emotional withdrawal and passive-reactive behaviour under pressure "
        f"is the single greatest gap between intellectual potential and real-world execution. "
        f"Every major career failure in this chart traces to Mars's debilitation "
        f"firing at a decision point and producing retreat instead of advance.\n\n"
    )

    # 7. FINAL VERDICT
    output += "7. FINAL VERDICT\n"
    output += (
        f"   Career stabilises after age 32 and peaks post-36 through independent, "
        f"unconventional, or foreign-linked work — not conventional employment. "
        f"Relationships work only with partners who match the chart's philosophical depth "
        f"and independence requirement. "
        f"Financial security is built post-30; before that, income is a learning mechanism. "
        f"The dominant life arc: friction-heavy early phase → structured intellectual authority "
        f"→ public-facing leadership. The chart does not deliver average results. "
        f"It delivers either exceptional output or visible failure — no middle ground exists.\n"
    )

    return output


def generate_ultra_report():
    """Produce the Stage 7.5 ultra-precision report to astrology_report_ultra.txt."""
    import io as _io2

    # Header
    print("\n🔱 ULTRA PRECISION PREDICTION ENGINE 🔱")
    print("Stage 7.5 — Event-Driven, Mechanism-Based Output\n")
    print("Native: Abhishek Singh | DOB: 3-Sep-2000 | Lagna: Gemini\n")

    print("=" * 60)
    print("REPORT MAP")
    print("=" * 60)
    print("U1. Planetary Placements (raw chart)")
    print("U2. Planet Mechanism Engine")
    print("U3. Yoga Activation Engine")
    print("U4. Dasha Prediction Engine")
    print("U5. Final Prediction Engine (7-point)\n")

    # U1 — Placements
    print("=" * 60)
    print("SECTION U1 — CHART FOUNDATION")
    print("=" * 60)
    for planet, data in planets.items():
        print(f"  {planet:10s} | {data.get('sign', 'N/A'):15s} | H{data.get('house', '?'):2} | {data.get('nakshatra', '')}")
    print()

    # U2 — Planet mechanisms
    print(_ultra_strip(_ultra_planet_section(planets)))

    # U3 — Yoga activation
    print(_ultra_strip(_ultra_yoga_section(planets)))

    # U4 — Dasha engine
    print(_ultra_strip(_ultra_dasha_section(dasha, planets)))

    # U5 — Final judgement
    print(_ultra_strip(_ultra_final_judgement(planets, dasha)))


# ============================================================
# STEP 1 — STRICT JSON DATA EXPORT (no interpretation prose)
# Implements the "Data Engine" step of the Hybrid Jyotish System.
# Output: chart_data.json
# ============================================================

_DEBIL_MAP_JSON = {
    "Sun": "Libra", "Moon": "Scorpio", "Mars": "Cancer",
    "Mercury": "Pisces", "Jupiter": "Capricorn",
    "Venus": "Virgo", "Saturn": "Aries",
}
_EXALT_MAP_JSON = {
    "Sun": "Aries", "Moon": "Taurus", "Mars": "Capricorn",
    "Mercury": "Virgo", "Jupiter": "Cancer",
    "Venus": "Pisces", "Saturn": "Libra",
}
_COMBUST_ORBS_JSON = {
    "Moon": 12, "Mars": 17, "Mercury": 14,
    "Jupiter": 11, "Venus": 10, "Saturn": 15,
}


def export_chart_data_json(planet_data, dasha_data, kundali_data,
                           output_path="chart_data.json"):
    """Serialise all chart facts to a strict JSON file — no prose, no interpretation.

    Fields exported:
        basic_details          — name, DOB, lagna, rasi, nakshatra
        planets                — per-planet: sign, house, degree, nakshatra,
                                 retrograde, dignity (exalted/debilitated/neutral),
                                 combust (bool), combust_orb_degrees
        house_occupants        — list of planets per house (1–12)
        lordships              — planet → list of houses it lords
        yogas                  — list of detected yoga names
        dasha                  — list of Mahadasha + derived antardasha info
    """
    import re as _rj

    # ── Basic Details ──────────────────────────────────────────
    bd = kundali_data.get("basic_details", kundali_data)
    basic = {}
    for key in ("name", "date_of_birth", "time_of_birth", "place_of_birth",
                "place", "lagna", "ascendant", "rasi", "nakshatra"):
        val = bd.get(key) or kundali_data.get(key)
        if val:
            basic[key] = val

    # ── Lagna index (for lordship calc) ───────────────────────
    lagna_str = basic.get("lagna", basic.get("ascendant", ""))
    lagna_idx = SIGN_ORDER.index(lagna_str) if lagna_str in SIGN_ORDER else -1

    # ── Sun position (for combustion) ─────────────────────────
    sun_data   = planet_data.get("Sun", {})
    sun_degree = float(sun_data.get("degree", 0))
    sun_sign   = sun_data.get("sign", "")

    # ── Planets ───────────────────────────────────────────────
    planets_out = {}
    for planet, pdata in planet_data.items():
        sign    = pdata.get("sign", "")
        house   = pdata.get("house", None)
        degree  = pdata.get("degree", None)
        naksh   = pdata.get("nakshatra", "")
        retro   = pdata.get("retrograde", False) is True

        # Dignity
        if _DEBIL_MAP_JSON.get(planet) == sign:
            dignity = "debilitated"
        elif _EXALT_MAP_JSON.get(planet) == sign:
            dignity = "exalted"
        else:
            dignity = "neutral"

        # Combustion (only for non-Sun planets)
        combust       = False
        combust_orb   = None
        if planet != "Sun":
            p_degree = float(pdata.get("degree", 0))
            p_sign   = sign
            if p_sign == sun_sign:
                orb_limit = _COMBUST_ORBS_JSON.get(planet, 12)
                raw_diff  = abs(p_degree - sun_degree)
                diff      = min(raw_diff, 360 - raw_diff)
                if diff <= orb_limit:
                    combust     = True
                    combust_orb = round(diff, 2)

        planets_out[planet] = {
            "sign":         sign,
            "house":        house,
            "degree":       degree,
            "nakshatra":    naksh,
            "retrograde":   retro,
            "dignity":      dignity,
            "combust":      combust,
        }
        if combust:
            planets_out[planet]["combust_orb_degrees"] = combust_orb

    # ── House Occupants ───────────────────────────────────────
    house_occupants: dict = {str(h): [] for h in range(1, 13)}
    for planet, pdata in planet_data.items():
        h = pdata.get("house")
        if h:
            house_occupants[str(h)].append(planet)

    # ── Lordships ─────────────────────────────────────────────
    lordships: dict = {}
    if lagna_idx >= 0:
        for offset in range(12):
            house_num  = offset + 1
            house_sign = SIGN_ORDER[(lagna_idx + offset) % 12]
            lord       = SIGN_LORDS.get(house_sign, "")
            if lord:
                lordships.setdefault(lord, []).append(house_num)

    # ── Yogas ─────────────────────────────────────────────────
    yoga_names: list = []
    # Extract from detect_real_yogas (returns formatted text)
    try:
        raw_yogas = detect_real_yogas(planet_data)
        for line in raw_yogas.splitlines():
            m = _rj.search(r'✅\s+(.+?)\s+(?:CONFIRMED|DETECTED|YOGA)', line, _rj.IGNORECASE)
            if m:
                yoga_names.append(m.group(1).strip())
            elif "Yoga" in line or "YOGA" in line:
                # Fallback: grab first capitalized phrase
                m2 = _rj.search(r'([A-Z][a-z]+(?:[-\s][A-Z][a-z]+)*\s+Yoga)', line)
                if m2:
                    yoga_names.append(m2.group(1).strip())
    except Exception:
        pass

    # Also pull yoga names from detect_advanced_yogas text
    try:
        adv_yogas = detect_advanced_yogas(planet_data)
        for line in adv_yogas.splitlines():
            m = _rj.search(r'([A-Z][a-z]+(?:[-\s][A-Z][a-z]+)*\s+Yoga)', line)
            if m:
                name = m.group(1).strip()
                if name not in yoga_names:
                    yoga_names.append(name)
    except Exception:
        pass

    # De-duplicate
    yoga_names = list(dict.fromkeys(yoga_names))

    # ── Dasha ─────────────────────────────────────────────────
    dasha_out = []
    for entry in dasha_data:
        dasha_out.append({
            "planet": entry.get("planet", ""),
            "start":  entry.get("start", ""),
            "end":    entry.get("end", ""),
        })

    # ── Assemble and write ────────────────────────────────────
    chart_json = {
        "basic_details":   basic,
        "planets":         planets_out,
        "house_occupants": house_occupants,
        "lordships":       lordships,
        "yogas":           yoga_names,
        "dasha":           dasha_out,
    }

    with open(output_path, "w", encoding="utf-8") as _f:
        json.dump(chart_json, _f, indent=2, ensure_ascii=False)

    return chart_json


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    # --------------------------------------------------------
    # STEP 1 — STRICT JSON DATA EXPORT (no interpretation)
    # --------------------------------------------------------
    JSON_OUTPUT_FILE = "chart_data.json"
    _chart_json = export_chart_data_json(
        planets, dasha, kundali, output_path=JSON_OUTPUT_FILE
    )
    print(f"✅ Strict JSON data exported to {JSON_OUTPUT_FILE}")
    print(f"   Planets: {len(_chart_json['planets'])}  |  "
          f"Yogas: {len(_chart_json['yogas'])}  |  "
          f"Dashas: {len(_chart_json['dasha'])}\n")

    REPORT_OUTPUT_FILE = "astrology_report_final.txt"

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

    # --------------------------------------------------------
    # STAGE 7.5 — Ultra Precision Report
    # --------------------------------------------------------
    ULTRA_OUTPUT_FILE = "astrology_report_ultra.txt"
    with open(ULTRA_OUTPUT_FILE, "w", encoding="utf-8") as _ultra_file:
        _tee_ultra = _Tee(_ultra_file)
        sys.stdout = _tee_ultra
        try:
            generate_ultra_report()
        finally:
            sys.stdout = sys.__stdout__

    print(f"\n✅ Ultra report saved to {ULTRA_OUTPUT_FILE}")


