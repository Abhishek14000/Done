"""
Generate aesthetic HTML reports from astrology report text files.
Run: python3 generate_html_reports.py
"""

import subprocess
import sys
import re
import html as html_escape_module
import os


# ─────────────────────────────────────────────────────────────
# STEP 1 — Capture the Vedic engine output to both .txt files
# ─────────────────────────────────────────────────────────────
def generate_txt_files():
    result = subprocess.run(
        [sys.executable, "vedic_reasoning_engine1.py"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print("⚠ Engine error (stderr):", result.stderr[:500])
    content = result.stdout
    with open("astrology_report_final.txt", "w", encoding="utf-8") as f:
        f.write(content)
    with open("astrology_report_ultra.txt", "w", encoding="utf-8") as f:
        f.write(content)
    print("✔ Text files written.")
    return content


# ─────────────────────────────────────────────────────────────
# STEP 2 — Parse helpers
# ─────────────────────────────────────────────────────────────
_SECTION_RE = re.compile(
    r"^={10,}\s*$\n(SECTION \d+ .+?)\n={10,}\s*$",
    re.MULTILINE
)


def parse_sections(text):
    """Return list of (title, body) tuples for each section."""
    parts = _SECTION_RE.split(text)
    # parts[0] = preamble before first section
    sections = [("PREAMBLE", parts[0])]
    for i in range(1, len(parts), 2):
        title = parts[i].strip()
        body = parts[i + 1] if i + 1 < len(parts) else ""
        sections.append((title, body))
    return sections


def text_to_html_content(text):
    """
    Convert plain report text into safe HTML snippets:
    - Lines starting with '---' become sub-headings
    - Lines starting with '===' become sub-headings
    - Lines starting with '-' become list items
    - Emoji lines get bold emphasis
    - Separator lines of '=' are stripped
    - Everything else is a <p>
    """
    esc = html_escape_module.escape
    out = []
    bullet_open = False
    lines = text.split("\n")

    for line in lines:
        stripped = line.strip()

        # Skip pure separator lines
        if re.match(r"^=+\s*$", stripped) or re.match(r"^-{10,}\s*$", stripped):
            if bullet_open:
                out.append("</ul>")
                bullet_open = False
            continue

        # Section sub-heading  === FOO ===
        if stripped.startswith("===") and stripped.endswith("==="):
            if bullet_open:
                out.append("</ul>")
                bullet_open = False
            heading = stripped.strip("= ").strip()
            out.append(f'<h3 class="sub-heading">{esc(heading)}</h3>')
            continue

        # Sub-sub-heading  --- FOO ---
        if stripped.startswith("---") and stripped.endswith("---"):
            if bullet_open:
                out.append("</ul>")
                bullet_open = False
            heading = stripped.strip("- ").strip()
            out.append(f'<h4 class="planet-heading">{esc(heading)}</h4>')
            continue

        # Bullet items
        if stripped.startswith("- "):
            if not bullet_open:
                out.append('<ul class="insight-list">')
                bullet_open = True
            out.append(f"<li>{esc(stripped[2:])}</li>")
            continue

        # Close any open bullet
        if bullet_open:
            out.append("</ul>")
            bullet_open = False

        # Numbered list (e.g. "1.  Foo")
        m = re.match(r"^(\d+)\.\s+(.+)$", stripped)
        if m:
            out.append(f'<p class="numbered"><strong>{esc(m.group(1))}.</strong> {esc(m.group(2))}</p>')
            continue

        # Lines with lots of emoji — treat as emphasis banners
        if stripped and sum(1 for c in stripped if ord(c) > 0x2600) >= 2:
            out.append(f'<p class="banner">{esc(stripped)}</p>')
            continue

        # Lines that look like table rows for planetary placements
        if "|" in stripped and ("Sign:" in stripped or "House:" in stripped or "Nakshatra:" in stripped):
            parts = [p.strip() for p in stripped.split("|")]
            cells = "".join(f"<td>{esc(p)}</td>" for p in parts)
            out.append(f'<tr>{cells}</tr>')
            continue

        # Empty line → paragraph break
        if not stripped:
            out.append('<div class="spacer"></div>')
            continue

        # Default paragraph
        out.append(f'<p>{esc(stripped)}</p>')

    if bullet_open:
        out.append("</ul>")

    # Wrap consecutive <tr> elements in a <table>
    result = "\n".join(out)
    result = re.sub(
        r'(<tr>.*?</tr>(\s*<tr>.*?</tr>)*)',
        r'<div class="table-wrap"><table class="planet-table"><tbody>\1</tbody></table></div>',
        result,
        flags=re.DOTALL
    )
    return result


# ─────────────────────────────────────────────────────────────
# SECTION ICON MAP
# ─────────────────────────────────────────────────────────────
SECTION_ICONS = {
    "1": "🌟", "2": "🪐", "3": "🔗", "4": "🔥",
    "5": "📜", "6": "👑", "7": "💼", "8": "💞",
    "9": "⏳", "10": "🔭", "11": "🔮", "12": "⚡",
    "13": "📚", "14": "⚠️", "15": "🌊", "16": "🌿",
    "17": "🎯", "18": "✨",
}


def section_icon(title):
    m = re.match(r"SECTION (\d+)", title)
    if m:
        return SECTION_ICONS.get(m.group(1), "🌐")
    return "🌐"


def section_id(title):
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


# ─────────────────────────────────────────────────────────────
# STEP 3a — Generate FINAL HTML (clean, professional)
# ─────────────────────────────────────────────────────────────
FINAL_CSS = """
  :root {
    --bg-deep:    #0d0d1a;
    --bg-card:    #131328;
    --bg-header:  #1a1a35;
    --gold:       #c9a84c;
    --gold-light: #f0d080;
    --text-main:  #e8e0d0;
    --text-muted: #8a8090;
    --accent:     #7c5cbf;
    --accent2:    #c05c5c;
    --border:     rgba(201,168,76,0.25);
    --shadow:     rgba(0,0,0,0.6);
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }
  body {
    background: var(--bg-deep);
    color: var(--text-main);
    font-family: 'Segoe UI', Georgia, serif;
    font-size: 15px;
    line-height: 1.75;
    min-height: 100vh;
  }

  /* ── Starfield ── */
  body::before {
    content: '';
    position: fixed; inset: 0;
    background:
      radial-gradient(ellipse at 20% 30%, rgba(124,92,191,.15) 0%, transparent 60%),
      radial-gradient(ellipse at 80% 70%, rgba(192,92,92,.10) 0%, transparent 50%);
    pointer-events: none;
    z-index: 0;
  }

  /* ── Layout ── */
  #app { position: relative; z-index: 1; max-width: 960px; margin: 0 auto; padding: 0 1.5rem 4rem; }

  /* ── Hero header ── */
  .hero {
    text-align: center;
    padding: 3.5rem 1rem 2.5rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 2rem;
  }
  .hero-badge {
    display: inline-block;
    font-size: .75rem;
    letter-spacing: .15em;
    text-transform: uppercase;
    color: var(--gold);
    border: 1px solid var(--gold);
    padding: .25rem .9rem;
    border-radius: 2rem;
    margin-bottom: 1.2rem;
  }
  .hero h1 {
    font-size: clamp(1.8rem, 4vw, 2.8rem);
    font-weight: 700;
    background: linear-gradient(135deg, var(--gold-light) 0%, var(--gold) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1.2;
    margin-bottom: .6rem;
  }
  .hero p { color: var(--text-muted); font-size: .9rem; }

  /* ── Table of Contents ── */
  .toc {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: .75rem;
    padding: 1.5rem 2rem;
    margin-bottom: 2.5rem;
  }
  .toc h2 { color: var(--gold); font-size: 1rem; letter-spacing: .1em; text-transform: uppercase; margin-bottom: 1rem; }
  .toc ol { padding-left: 1.2rem; columns: 2; column-gap: 2rem; }
  .toc li { margin-bottom: .3rem; break-inside: avoid; }
  .toc a { color: var(--text-main); text-decoration: none; font-size: .88rem; }
  .toc a:hover { color: var(--gold-light); }

  /* ── Section cards ── */
  .section-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: .85rem;
    margin-bottom: 2rem;
    overflow: hidden;
  }
  .section-header {
    background: linear-gradient(90deg, var(--bg-header) 0%, rgba(26,26,53,.6) 100%);
    padding: 1rem 1.5rem;
    display: flex; align-items: center; gap: .75rem;
    border-bottom: 1px solid var(--border);
  }
  .section-icon { font-size: 1.4rem; }
  .section-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: var(--gold);
    letter-spacing: .05em;
    text-transform: uppercase;
  }
  .section-body { padding: 1.4rem 1.8rem; }

  /* ── Typography in body ── */
  .section-body p { margin-bottom: .75rem; }
  .section-body p:last-child { margin-bottom: 0; }
  .sub-heading {
    font-size: .9rem; font-weight: 600;
    letter-spacing: .08em; text-transform: uppercase;
    color: var(--gold); margin: 1.25rem 0 .5rem;
    border-bottom: 1px solid var(--border); padding-bottom: .25rem;
  }
  .planet-heading {
    font-size: .95rem; font-weight: 600;
    color: var(--gold-light); margin: 1rem 0 .3rem;
  }
  .insight-list { padding-left: 1.4rem; margin-bottom: .75rem; }
  .insight-list li { margin-bottom: .4rem; color: var(--text-main); }
  .numbered { margin-bottom: .4rem; }
  .banner {
    text-align: center; padding: .6rem;
    color: var(--gold-light); font-weight: 600;
    font-size: 1rem; margin: .75rem 0;
  }
  .spacer { height: .5rem; }

  /* ── Planetary table ── */
  .table-wrap { overflow-x: auto; margin: .75rem 0; }
  .planet-table {
    border-collapse: collapse; width: 100%;
    font-size: .85rem;
  }
  .planet-table td {
    padding: .45rem .75rem;
    border: 1px solid var(--border);
    white-space: nowrap;
  }
  .planet-table tr:nth-child(odd) td { background: rgba(201,168,76,.05); }
  .planet-table tr:hover td { background: rgba(201,168,76,.12); }

  /* ── Preamble ── */
  .preamble {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: .85rem;
    padding: 1.4rem 1.8rem;
    margin-bottom: 2rem;
    font-size: .88rem;
    color: var(--text-muted);
  }

  /* ── Footer ── */
  footer {
    text-align: center; padding: 2rem 0 1rem;
    color: var(--text-muted); font-size: .8rem;
    border-top: 1px solid var(--border);
    margin-top: 3rem;
  }
  footer span { color: var(--gold); }

  @media (max-width: 600px) {
    .toc ol { columns: 1; }
    #app { padding: 0 1rem 3rem; }
  }
"""


def build_final_html(report_text):
    sections = parse_sections(report_text)
    preamble_title, preamble_body = sections[0]
    real_sections = sections[1:]

    toc_items = "\n".join(
        f'<li><a href="#{section_id(t)}">{t}</a></li>'
        for t, _ in real_sections
    )

    section_html_parts = []
    for title, body in real_sections:
        sid = section_id(title)
        icon = section_icon(title)
        content = text_to_html_content(body)
        section_html_parts.append(f"""
  <div class="section-card" id="{sid}">
    <div class="section-header">
      <span class="section-icon">{icon}</span>
      <span class="section-title">{html_escape_module.escape(title)}</span>
    </div>
    <div class="section-body">
{content}
    </div>
  </div>""")

    preamble_content = text_to_html_content(preamble_body)
    sections_html = "\n".join(section_html_parts)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Elite Vedic Astrology Report — Final</title>
  <style>
{FINAL_CSS}
  </style>
</head>
<body>
<div id="app">

  <header class="hero">
    <div class="hero-badge">Vedic Jyotish</div>
    <h1>🔱 Elite Vedic Astrology Report 🔱</h1>
    <p>Powered by Multi-Layer Jyotish Reasoning Engine</p>
  </header>

  <nav class="toc" aria-label="Report sections">
    <h2>📋 Report Contents</h2>
    <ol>{toc_items}</ol>
  </nav>

  <div class="preamble">
{preamble_content}
  </div>

{sections_html}

  <footer>
    <p>Generated by <span>Elite Vedic Astrology Engine</span> · Jyotish Wisdom</p>
  </footer>

</div>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────
# STEP 3b — Generate ULTRA HTML (premium visual)
# ─────────────────────────────────────────────────────────────
ULTRA_CSS = """
  @import url('https://fonts.googleapis.com/css2?family=Cinzel:wght@400;600;700&family=Inter:wght@300;400;500&display=swap');

  :root {
    --bg0:        #070710;
    --bg1:        #0f0f22;
    --bg2:        #181830;
    --bg3:        #20203e;
    --gold:       #d4a843;
    --gold2:      #f5d78a;
    --gold3:      #ffe9a0;
    --saffron:    #e07b39;
    --purple:     #7c5cbf;
    --teal:       #4db6ac;
    --red:        #c0392b;
    --text:       #ede8e0;
    --muted:      #7a7090;
    --border:     rgba(212,168,67,.2);
    --glow:       rgba(212,168,67,.08);
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; }

  body {
    background-color: var(--bg0);
    background-image:
      radial-gradient(ellipse 120% 80% at 10% 5%,  rgba(124,92,191,.18) 0%, transparent 55%),
      radial-gradient(ellipse 80%  60% at 90% 95%, rgba(212,168,67,.12)  0%, transparent 50%),
      radial-gradient(ellipse 60%  40% at 50% 50%, rgba(224,123,57,.06)  0%, transparent 60%);
    color: var(--text);
    font-family: 'Inter', 'Segoe UI', sans-serif;
    font-size: 15px;
    line-height: 1.8;
    min-height: 100vh;
  }

  /* ── Scroll progress bar ── */
  #progress-bar {
    position: fixed; top: 0; left: 0; height: 3px; width: 0%;
    background: linear-gradient(90deg, var(--gold), var(--saffron), var(--purple));
    z-index: 9999;
    transition: width .1s linear;
  }

  /* ── Sticky nav ── */
  #topnav {
    position: sticky; top: 0; z-index: 100;
    background: rgba(7,7,16,.88);
    backdrop-filter: blur(12px);
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 1rem;
    padding: .6rem 2rem;
    overflow-x: auto;
  }
  #topnav span { font-size: .7rem; color: var(--gold); letter-spacing: .12em; text-transform: uppercase; white-space: nowrap; }
  #topnav a {
    font-size: .72rem; color: var(--muted); text-decoration: none;
    white-space: nowrap; padding: .2rem .5rem; border-radius: .3rem;
    transition: color .2s, background .2s;
  }
  #topnav a:hover { color: var(--gold2); background: var(--glow); }

  /* ── Layout ── */
  .wrapper { max-width: 1000px; margin: 0 auto; padding: 0 2rem 5rem; }

  /* ── Hero ── */
  .hero {
    text-align: center;
    padding: 5rem 1rem 3rem;
    position: relative;
  }
  .hero::after {
    content: '';
    display: block;
    width: 200px; height: 1px;
    background: linear-gradient(90deg, transparent, var(--gold), transparent);
    margin: 2rem auto 0;
  }
  .hero-sub {
    font-size: .72rem; letter-spacing: .22em; text-transform: uppercase;
    color: var(--gold); margin-bottom: 1.4rem;
    display: flex; align-items: center; justify-content: center; gap: .8rem;
  }
  .hero-sub::before, .hero-sub::after {
    content: ''; flex: 1; max-width: 80px; height: 1px;
    background: linear-gradient(90deg, transparent, var(--gold));
  }
  .hero-sub::after { transform: scaleX(-1); }
  .hero h1 {
    font-family: 'Cinzel', Georgia, serif;
    font-size: clamp(2rem, 5vw, 3.5rem);
    font-weight: 700;
    background: linear-gradient(135deg, var(--gold3) 0%, var(--gold) 50%, var(--saffron) 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
    letter-spacing: .02em;
    margin-bottom: .8rem;
  }
  .hero-desc { color: var(--muted); font-size: .9rem; font-weight: 300; }
  .hero-desc span { color: var(--gold2); }

  /* ── Kundali info pills ── */
  .info-pills {
    display: flex; flex-wrap: wrap; gap: .6rem;
    justify-content: center; margin-top: 1.5rem;
  }
  .pill {
    background: var(--bg2); border: 1px solid var(--border);
    padding: .35rem .9rem; border-radius: 2rem;
    font-size: .78rem; color: var(--text);
  }
  .pill strong { color: var(--gold2); }

  /* ── Section cards ── */
  .section-card {
    background: var(--bg1);
    border: 1px solid var(--border);
    border-radius: 1rem;
    margin-bottom: 2.2rem;
    overflow: hidden;
    box-shadow: 0 4px 30px rgba(0,0,0,.4);
    transition: box-shadow .3s;
  }
  .section-card:hover { box-shadow: 0 6px 40px rgba(212,168,67,.12); }

  .section-header {
    background: linear-gradient(90deg, var(--bg3) 0%, var(--bg2) 100%);
    padding: 1.1rem 1.8rem;
    display: flex; align-items: center; gap: 1rem;
    border-bottom: 1px solid var(--border);
    position: relative; overflow: hidden;
  }
  .section-header::before {
    content: '';
    position: absolute; left: 0; top: 0; bottom: 0; width: 3px;
    background: linear-gradient(180deg, var(--gold), var(--saffron));
  }
  .section-num {
    font-family: 'Cinzel', serif;
    font-size: .7rem; font-weight: 600;
    color: var(--muted); letter-spacing: .1em;
    min-width: 2rem;
  }
  .section-icon { font-size: 1.5rem; }
  .section-title {
    font-family: 'Cinzel', serif;
    font-size: .95rem; font-weight: 600;
    color: var(--gold2); letter-spacing: .04em;
    flex: 1;
  }

  /* ── Body content ── */
  .section-body { padding: 1.6rem 2rem; }
  .section-body p { margin-bottom: .8rem; color: var(--text); }
  .sub-heading {
    font-family: 'Cinzel', serif;
    font-size: .8rem; font-weight: 600;
    letter-spacing: .12em; text-transform: uppercase;
    color: var(--gold); margin: 1.4rem 0 .6rem;
    display: flex; align-items: center; gap: .5rem;
  }
  .sub-heading::after {
    content: ''; flex: 1; height: 1px;
    background: linear-gradient(90deg, var(--border), transparent);
  }
  .planet-heading {
    font-size: .95rem; font-weight: 500;
    color: var(--gold2); margin: 1.1rem 0 .35rem;
    padding-left: .6rem;
    border-left: 2px solid var(--saffron);
  }
  .insight-list { padding-left: 0; margin-bottom: .9rem; list-style: none; }
  .insight-list li {
    padding: .35rem 0 .35rem 1.6rem;
    position: relative; color: var(--text);
    border-bottom: 1px solid rgba(255,255,255,.04);
    font-size: .9rem;
  }
  .insight-list li::before {
    content: '◈'; position: absolute; left: 0;
    color: var(--gold); font-size: .7rem; top: .55rem;
  }
  .numbered { margin-bottom: .5rem; }
  .banner {
    text-align: center; padding: .8rem 1rem;
    background: linear-gradient(135deg, rgba(212,168,67,.08), rgba(224,123,57,.06));
    border: 1px solid var(--border); border-radius: .5rem;
    color: var(--gold2); font-weight: 600; font-size: 1rem;
    margin: 1rem 0; letter-spacing: .04em;
  }
  .spacer { height: .6rem; }

  /* ── Planetary table ── */
  .table-wrap { overflow-x: auto; margin: 1rem 0; border-radius: .5rem; border: 1px solid var(--border); }
  .planet-table {
    border-collapse: collapse; width: 100%;
    font-size: .85rem; font-family: 'Inter', monospace;
  }
  .planet-table td {
    padding: .55rem 1rem;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  .planet-table tr:last-child td { border-bottom: none; }
  .planet-table tr:nth-child(odd) td { background: rgba(255,255,255,.02); }
  .planet-table tr:hover td { background: rgba(212,168,67,.06); }
  .planet-table td:first-child { color: var(--gold2); font-weight: 500; }

  /* ── TOC ── */
  .toc-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: .6rem; margin-bottom: 2.5rem;
  }
  .toc-item {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: .6rem; padding: .75rem 1rem;
    text-decoration: none; color: var(--text);
    display: flex; align-items: center; gap: .6rem;
    font-size: .82rem; transition: all .2s;
  }
  .toc-item:hover {
    border-color: var(--gold); color: var(--gold2);
    background: var(--bg3); transform: translateY(-1px);
  }
  .toc-icon { font-size: 1rem; }

  /* ── Preamble card ── */
  .preamble-card {
    background: var(--bg2);
    border: 1px solid var(--border); border-radius: 1rem;
    padding: 1.4rem 1.8rem; margin-bottom: 2rem;
    font-size: .88rem; color: var(--muted);
  }

  /* ── Footer ── */
  footer {
    text-align: center; padding: 2.5rem 0 1rem;
    color: var(--muted); font-size: .78rem;
    border-top: 1px solid var(--border); margin-top: 3rem;
  }
  .footer-logo {
    font-family: 'Cinzel', serif;
    font-size: 1.2rem; color: var(--gold);
    margin-bottom: .6rem;
  }
  footer p { color: var(--muted); }
  footer span { color: var(--gold2); }

  @media (max-width: 640px) {
    .wrapper { padding: 0 1rem 4rem; }
    .section-body { padding: 1.2rem 1.2rem; }
    .hero h1 { font-size: 1.8rem; }
    .toc-grid { grid-template-columns: 1fr 1fr; }
    #topnav { padding: .5rem 1rem; }
  }
"""

ULTRA_JS = """
  // Scroll progress bar
  const bar = document.getElementById('progress-bar');
  function updateBar() {
    const h = document.documentElement;
    const pct = (h.scrollTop / (h.scrollHeight - h.clientHeight)) * 100;
    if (bar) bar.style.width = pct + '%';
  }
  document.addEventListener('scroll', updateBar, { passive: true });
"""


def build_ultra_html(report_text):
    sections = parse_sections(report_text)
    preamble_title, preamble_body = sections[0]
    real_sections = sections[1:]

    # Build navigation
    nav_links = " ".join(
        f'<a href="#{section_id(t)}">{section_icon(t)} {html_escape_module.escape(t.split("—")[0].strip())}</a>'
        for t, _ in real_sections
    )

    # Build TOC grid
    toc_items = "\n".join(
        f'''<a class="toc-item" href="#{section_id(t)}">
          <span class="toc-icon">{section_icon(t)}</span>
          <span>{html_escape_module.escape(t)}</span>
        </a>'''
        for t, _ in real_sections
    )

    # Extract kundali info pills from preamble
    pill_html = ""
    for line in preamble_body.split("\n"):
        m = re.match(r"\s+(\w[\w ]+):\s+(.+)", line)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            pill_html += f'<div class="pill"><strong>{html_escape_module.escape(key)}:</strong> {html_escape_module.escape(val)}</div>\n'

    # Build section cards
    section_parts = []
    for idx, (title, body) in enumerate(real_sections, start=1):
        sid = section_id(title)
        icon = section_icon(title)
        m = re.match(r"SECTION (\d+)\s*[—-]\s*(.+)", title)
        num_label = f"SECTION {m.group(1)}" if m else f"SECTION {idx}"
        clean_title = m.group(2).strip() if m else title
        content = text_to_html_content(body)
        section_parts.append(f"""
  <div class="section-card" id="{sid}">
    <div class="section-header">
      <span class="section-num">{html_escape_module.escape(num_label)}</span>
      <span class="section-icon">{icon}</span>
      <span class="section-title">{html_escape_module.escape(clean_title)}</span>
    </div>
    <div class="section-body">
{content}
    </div>
  </div>""")

    preamble_content = text_to_html_content(preamble_body)
    sections_html = "\n".join(section_parts)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Elite Vedic Astrology Report — Ultra</title>
  <style>
{ULTRA_CSS}
  </style>
</head>
<body>

<div id="progress-bar" aria-hidden="true"></div>

<nav id="topnav" aria-label="Quick navigation">
  <span>🔱 Jyotish</span>
  {nav_links}
</nav>

<div class="wrapper">

  <header class="hero">
    <div class="hero-sub">Elite Vedic Astrology</div>
    <h1>🔱 Jyotish Ultra Report 🔱</h1>
    <p class="hero-desc">Powered by <span>Multi-Layer Jyotish Reasoning Engine</span></p>
    <div class="info-pills">
{pill_html}
    </div>
  </header>

  <div class="toc-grid" aria-label="Sections">
{toc_items}
  </div>

  <div class="preamble-card">
{preamble_content}
  </div>

{sections_html}

  <footer>
    <div class="footer-logo">🔱 Jyotish Vidya</div>
    <p>Generated by <span>Elite Vedic Astrology Engine</span> · Multi-Layer Jyotish Reasoning</p>
  </footer>

</div>

<script>
{ULTRA_JS}
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("Generating text reports…")
    content = generate_txt_files()

    print("Building astrology_report_final.html…")
    final_html = build_final_html(content)
    with open("astrology_report_final.html", "w", encoding="utf-8") as f:
        f.write(final_html)
    print(f"  ✔ astrology_report_final.html ({len(final_html):,} bytes)")

    print("Building astrology_report_ultra.html…")
    ultra_html = build_ultra_html(content)
    with open("astrology_report_ultra.html", "w", encoding="utf-8") as f:
        f.write(ultra_html)
    print(f"  ✔ astrology_report_ultra.html ({len(ultra_html):,} bytes)")

    print("\nDone! All files written successfully.")


if __name__ == "__main__":
    main()
