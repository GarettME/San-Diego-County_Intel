"""
San Diego County Motivated Seller Lead Scraper
===============================================
Scrapes public records from the SD County Assessor/Recorder/Clerk's office
(ARCC Acclaim portal) and scores each lead based on distress signals.

Distress scoring model:
  - Tax delinquency   : +30 points
  - Code violation    : +25 points
  - Probate filing    : +20 points
  - Multiple liens    : +15 points
  - Divorce/bankruptcy: +10 points

Output:
  /data/output.json      — Raw leads + scores as JSON
  /dashboard/index.html  — Visual HTML dashboard sorted by score
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
BASE_URL = "https://arcc-acclaim.sdcounty.ca.gov"
DISCLAIMER_URL = f"{BASE_URL}/search/Disclaimer"
SEARCH_URL = f"{BASE_URL}/search/SearchTypeDocType"

# Document types that indicate motivated-seller distress signals
DISTRESS_DOC_TYPES = [
    "NOTICE OF DEFAULT",
    "NOTICE OF TRUSTEE SALE",
    "LIEN",
    "TAX DEED",
    "PROBATE",
    "LIS PENDENS",
    "BANKRUPTCY",
    "DIVORCE",
    "CODE ENFORCEMENT",
    "DELINQUENT TAX",
]

# Maximum pages to scrape per run (safety limit for production)
MAX_PAGES = 50

# Seconds between HTTP requests — be polite to the county server
REQUEST_DELAY = 1.5

# Output paths (relative to project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
OUTPUT_JSON = DATA_DIR / "output.json"
DASHBOARD_HTML = DASHBOARD_DIR / "index.html"

# ─── Data Model ─────────────────────────────────────────────────────────────
@dataclass
class Lead:
    """Represents a single public-records lead."""
    document_number: str = ""
    file_date: str = ""
    doc_type: str = ""
    grantor: str = ""          # Seller / current owner
    grantee: str = ""          # Buyer / lien holder / trustee
    legal_description: str = ""
    property_address: str = ""

    # Distress flags (populated during scoring)
    has_tax_delinquency: bool = False
    has_code_violation: bool = False
    has_probate: bool = False
    has_multiple_liens: bool = False
    has_divorce_bankruptcy: bool = False

    seller_score: int = 0
    score_reasons: list = field(default_factory=list)

    # Metadata
    source_url: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ─── HTTP Session ────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    """
    Build a persistent requests session with browser-like headers.
    The county portal checks for a valid User-Agent and may reject bots.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE_URL,
    })
    return session


def accept_disclaimer(session: requests.Session) -> bool:
    """
    The ARCC portal shows a legal disclaimer on first visit.
    POST the acceptance form so subsequent requests are allowed.
    Returns True on success.
    """
    try:
        log.info("Accepting disclaimer at %s", DISCLAIMER_URL)
        resp = session.get(DISCLAIMER_URL, timeout=30)
        resp.raise_for_status()

        # Parse hidden form fields (CSRF tokens, view state, etc.)
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form")
        payload: dict = {}

        if form:
            for inp in form.find_all("input"):
                name = inp.get("name", "")
                value = inp.get("value", "")
                if name:
                    payload[name] = value
            # Mark acceptance — the button text / name varies; try common patterns
            for btn in form.find_all(["input", "button"], {"type": "submit"}):
                bname = btn.get("name", "")
                if bname:
                    payload[bname] = btn.get("value", "Accept")
                    break

        action = form.get("action", DISCLAIMER_URL) if form else DISCLAIMER_URL
        post_url = urljoin(BASE_URL, action)

        time.sleep(REQUEST_DELAY)
        resp2 = session.post(post_url, data=payload, timeout=30)
        resp2.raise_for_status()
        log.info("Disclaimer accepted (status %d)", resp2.status_code)
        return True

    except Exception as exc:
        log.warning("Could not accept disclaimer: %s — continuing anyway.", exc)
        return False


# ─── Scraping ────────────────────────────────────────────────────────────────
def fetch_search_page(
    session: requests.Session,
    doc_type: str,
    page: int = 1,
) -> Optional[BeautifulSoup]:
    """
    Fetch one page of search results for a given document type.
    Returns parsed BeautifulSoup or None on failure.
    """
    params = {
        "DocType": doc_type,
        "RecordingDateFrom": "",
        "RecordingDateTo": "",
        "Page": page,
    }
    url = f"{SEARCH_URL}?{urlencode(params)}"

    try:
        time.sleep(REQUEST_DELAY)
        log.debug("GET %s", url)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")

    except requests.exceptions.Timeout:
        log.error("Timeout fetching page %d for doc type '%s'", page, doc_type)
    except requests.exceptions.HTTPError as exc:
        log.error("HTTP %s fetching page %d for '%s'", exc.response.status_code, page, doc_type)
    except Exception as exc:
        log.error("Unexpected error on page %d for '%s': %s", page, doc_type, exc)

    return None


def parse_results_table(soup: BeautifulSoup, doc_type: str) -> list[Lead]:
    """
    Parse the HTML results table returned by the ARCC search portal.
    The portal renders results in a <table> with class 'results' or similar.
    Handles slight layout variations gracefully.
    """
    leads: list[Lead] = []

    # The portal typically wraps results in a table — try multiple selectors
    table = (
        soup.find("table", {"class": re.compile(r"result", re.I)})
        or soup.find("table", {"id": re.compile(r"result", re.I)})
        or soup.find("table")  # fallback: first table on page
    )

    if not table:
        log.debug("No results table found for doc type '%s'", doc_type)
        return leads

    rows = table.find_all("tr")
    if len(rows) < 2:
        return leads  # Header-only table — no data

    # Detect column positions from header row
    header_row = rows[0]
    headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

    col = {
        "doc_num":  _find_col(headers, ["doc", "document", "instrument", "number"]),
        "date":     _find_col(headers, ["date", "recorded", "filed"]),
        "doc_type": _find_col(headers, ["type", "doc type", "document type"]),
        "grantor":  _find_col(headers, ["grantor", "seller", "from"]),
        "grantee":  _find_col(headers, ["grantee", "buyer", "to"]),
        "legal":    _find_col(headers, ["legal", "description", "parcel"]),
        "address":  _find_col(headers, ["address", "situs", "property"]),
    }

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        try:
            lead = Lead(
                doc_type=doc_type,
                document_number=_cell(cells, col["doc_num"]),
                file_date=_cell(cells, col["date"]),
                grantor=_cell(cells, col["grantor"]),
                grantee=_cell(cells, col["grantee"]),
                legal_description=_cell(cells, col["legal"]),
                property_address=_cell(cells, col["address"]),
                source_url=SEARCH_URL,
            )

            # Some portals embed a detail link with the address
            link = row.find("a", href=True)
            if link:
                lead.source_url = urljoin(BASE_URL, link["href"])

            leads.append(lead)

        except Exception as exc:
            log.warning("Could not parse row: %s — skipping.", exc)
            continue

    return leads


def get_total_pages(soup: BeautifulSoup) -> int:
    """
    Detect total page count from pagination controls.
    Returns 1 if pagination is not found.
    """
    # Common patterns: "Page 1 of 12", "Showing 1-25 of 300 records"
    text = soup.get_text(" ")
    match = re.search(r"[Pp]age\s+\d+\s+of\s+(\d+)", text)
    if match:
        return min(int(match.group(1)), MAX_PAGES)

    match = re.search(r"(\d+)\s+record", text)
    if match:
        count = int(match.group(1))
        return min((count // 25) + 1, MAX_PAGES)  # assume ~25 rows/page

    return 1


# ─── Scoring ─────────────────────────────────────────────────────────────────
def score_lead(lead: Lead, all_leads: list[Lead]) -> Lead:
    """
    Calculate the seller distress score (0–100) for a single lead.
    Mutates the lead in-place and returns it.
    """
    score = 0
    reasons: list[str] = []
    doc = lead.doc_type.upper()

    # Tax delinquency (+30)
    if any(k in doc for k in ["TAX", "DELINQUENT", "TAX DEED", "DELINQUENT TAX"]):
        lead.has_tax_delinquency = True
        score += 30
        reasons.append("Tax delinquency (+30)")

    # Code violation (+25)
    if any(k in doc for k in ["CODE", "VIOLATION", "NUISANCE", "ABATEMENT"]):
        lead.has_code_violation = True
        score += 25
        reasons.append("Code violation (+25)")

    # Probate (+20)
    if any(k in doc for k in ["PROBATE", "ESTATE", "DECEDENT", "ADMINISTRATION"]):
        lead.has_probate = True
        score += 20
        reasons.append("Probate filing (+20)")

    # Divorce / bankruptcy (+10)
    if any(k in doc for k in ["DIVORCE", "DISSOLUTION", "BANKRUPTCY", "BANKRUPT"]):
        lead.has_divorce_bankruptcy = True
        score += 10
        reasons.append("Divorce/bankruptcy (+10)")

    # Multiple liens (+15) — check if this grantor appears in multiple lien records
    lien_keywords = ["LIEN", "NOTICE OF DEFAULT", "TRUSTEE", "JUDGMENT"]
    if any(k in doc for k in lien_keywords):
        same_owner_count = sum(
            1 for l in all_leads
            if l.grantor
            and l.grantor.upper() == lead.grantor.upper()
            and any(k in l.doc_type.upper() for k in lien_keywords)
        )
        if same_owner_count > 1:
            lead.has_multiple_liens = True
            score += 15
            reasons.append(f"Multiple liens ({same_owner_count} records, +15)")

    lead.seller_score = min(score, 100)  # cap at 100
    lead.score_reasons = reasons
    return lead


# ─── Main Orchestration ───────────────────────────────────────────────────────
def scrape_all() -> list[Lead]:
    """
    Main entry point. Iterates over all distress document types,
    paginates through results, and returns a flat list of leads.
    """
    session = build_session()
    accept_disclaimer(session)

    all_leads: list[Lead] = []

    for doc_type in DISTRESS_DOC_TYPES:
        log.info("── Scraping doc type: %s", doc_type)
        page = 1

        while page <= MAX_PAGES:
            log.info("   Page %d …", page)
            soup = fetch_search_page(session, doc_type, page)

            if soup is None:
                log.warning("   Skipping remaining pages for '%s'", doc_type)
                break

            page_leads = parse_results_table(soup, doc_type)
            log.info("   Found %d records on page %d", len(page_leads), page)
            all_leads.extend(page_leads)

            total_pages = get_total_pages(soup)
            if page >= total_pages:
                break

            page += 1

    log.info("Total raw records collected: %d", len(all_leads))
    return all_leads


def deduplicate(leads: list[Lead]) -> list[Lead]:
    """
    Remove duplicate records by document number.
    Keeps the first occurrence.
    """
    seen: set[str] = set()
    unique: list[Lead] = []
    for lead in leads:
        key = lead.document_number or id(lead)
        if key not in seen:
            seen.add(str(key))
            unique.append(lead)
    log.info("After deduplication: %d records", len(unique))
    return unique


# ─── Persistence ─────────────────────────────────────────────────────────────
def save_json(leads: list[Lead]) -> None:
    """Serialise leads to /data/output.json."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_leads": len(leads),
        "leads": [asdict(l) for l in leads],
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    log.info("Saved JSON → %s", OUTPUT_JSON)


def generate_dashboard(leads: list[Lead]) -> None:
    """
    Generate a self-contained HTML dashboard at /dashboard/index.html.
    Leads are already sorted by score descending when passed in.
    Embeds the JSON data directly so the dashboard works offline.
    """
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    # Inject data as a JS variable
    leads_json = json.dumps([asdict(l) for l in leads], indent=2, default=str)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>SD County Motivated Seller Leads</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet"/>
<style>
  :root {{
    --bg: #0a0d14;
    --surface: #111520;
    --border: #1e2535;
    --accent: #e8ff47;
    --accent2: #ff4757;
    --text: #d4dbe8;
    --text-dim: #5a6475;
    --green: #39d98a;
    --orange: #ff7b2e;
    --mono: 'Space Mono', monospace;
    --sans: 'Syne', sans-serif;
  }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    overflow-x: hidden;
  }}

  /* ── Header ── */
  header {{
    border-bottom: 1px solid var(--border);
    padding: 2rem 3rem;
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 1rem;
    flex-wrap: wrap;
  }}
  .logo-block h1 {{
    font-size: clamp(1.6rem, 3vw, 2.4rem);
    font-weight: 800;
    letter-spacing: -0.04em;
    line-height: 1;
  }}
  .logo-block h1 span {{ color: var(--accent); }}
  .logo-block p {{
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--text-dim);
    margin-top: 0.4rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }}
  .stats-row {{
    display: flex;
    gap: 2rem;
    flex-wrap: wrap;
  }}
  .stat {{
    text-align: right;
  }}
  .stat .num {{
    font-family: var(--mono);
    font-size: 1.8rem;
    font-weight: 700;
    color: var(--accent);
    line-height: 1;
  }}
  .stat .lbl {{
    font-size: 0.65rem;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 0.15rem;
  }}

  /* ── Controls ── */
  .controls {{
    padding: 1.5rem 3rem;
    display: flex;
    gap: 1rem;
    align-items: center;
    flex-wrap: wrap;
    border-bottom: 1px solid var(--border);
  }}
  .search-box {{
    flex: 1;
    min-width: 200px;
    max-width: 380px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.6rem 1rem;
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.8rem;
    outline: none;
    transition: border-color 0.2s;
  }}
  .search-box:focus {{ border-color: var(--accent); }}
  .filter-btn {{
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--text-dim);
    padding: 0.6rem 1.1rem;
    border-radius: 4px;
    font-family: var(--mono);
    font-size: 0.72rem;
    cursor: pointer;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    transition: all 0.15s;
  }}
  .filter-btn:hover, .filter-btn.active {{
    border-color: var(--accent);
    color: var(--accent);
    background: rgba(232, 255, 71, 0.06);
  }}
  #count-display {{
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--text-dim);
    margin-left: auto;
  }}

  /* ── Grid ── */
  .grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 1px;
    background: var(--border);
    border-top: none;
  }}

  /* ── Card ── */
  .card {{
    background: var(--surface);
    padding: 1.4rem 1.6rem;
    cursor: pointer;
    transition: background 0.15s;
    position: relative;
    overflow: hidden;
  }}
  .card:hover {{ background: #161b28; }}
  .card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    background: var(--score-color, var(--text-dim));
  }}

  .card-top {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 0.8rem;
  }}
  .doc-type {{
    font-family: var(--mono);
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--text-dim);
    background: var(--border);
    padding: 0.2rem 0.5rem;
    border-radius: 2px;
  }}
  .score-badge {{
    font-family: var(--mono);
    font-size: 0.8rem;
    font-weight: 700;
    padding: 0.2rem 0.6rem;
    border-radius: 2px;
    border: 1px solid var(--score-color, var(--text-dim));
    color: var(--score-color, var(--text-dim));
    background: rgba(255,255,255,0.03);
  }}

  .grantor {{
    font-size: 1.05rem;
    font-weight: 600;
    letter-spacing: -0.02em;
    color: var(--text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .address {{
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--text-dim);
    margin-top: 0.25rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}

  .card-meta {{
    margin-top: 1rem;
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.4rem 1rem;
  }}
  .meta-item .meta-label {{
    font-family: var(--mono);
    font-size: 0.58rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--text-dim);
  }}
  .meta-item .meta-value {{
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--text);
    margin-top: 0.1rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}

  .tags {{
    margin-top: 0.9rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
  }}
  .tag {{
    font-family: var(--mono);
    font-size: 0.58rem;
    padding: 0.15rem 0.45rem;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    background: rgba(255,71,87,0.12);
    color: var(--accent2);
    border: 1px solid rgba(255,71,87,0.25);
  }}
  .tag.tax   {{ background: rgba(57,217,138,0.1); color: var(--green); border-color: rgba(57,217,138,0.25); }}
  .tag.code  {{ background: rgba(255,123,46,0.1); color: var(--orange); border-color: rgba(255,123,46,0.25); }}
  .tag.prob  {{ background: rgba(232,255,71,0.1); color: var(--accent); border-color: rgba(232,255,71,0.25); }}
  .tag.lien  {{ background: rgba(255,71,87,0.12); color: var(--accent2); border-color: rgba(255,71,87,0.25); }}
  .tag.div   {{ background: rgba(147,112,219,0.12); color: #b39ddb; border-color: rgba(147,112,219,0.25); }}

  /* ── Empty state ── */
  .empty {{
    grid-column: 1/-1;
    text-align: center;
    padding: 5rem 2rem;
    color: var(--text-dim);
    font-family: var(--mono);
    font-size: 0.8rem;
  }}

  /* ── Footer ── */
  footer {{
    padding: 1.5rem 3rem;
    border-top: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--text-dim);
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 0.5rem;
  }}
</style>
</head>
<body>

<header>
  <div class="logo-block">
    <h1>SD<span> Leads</span></h1>
    <p>San Diego County · Motivated Seller Intelligence</p>
  </div>
  <div class="stats-row" id="header-stats"></div>
</header>

<div class="controls">
  <input class="search-box" type="text" id="search" placeholder="Search name, address, doc #…"/>
  <button class="filter-btn" data-filter="all">All</button>
  <button class="filter-btn" data-filter="tax">Tax</button>
  <button class="filter-btn" data-filter="code">Code</button>
  <button class="filter-btn" data-filter="probate">Probate</button>
  <button class="filter-btn" data-filter="lien">Liens</button>
  <button class="filter-btn" data-filter="div">Divorce/BK</button>
  <span id="count-display"></span>
</div>

<div class="grid" id="grid"></div>

<footer>
  <span>Data sourced from ARCC Acclaim — San Diego County Public Records</span>
  <span id="footer-ts"></span>
</footer>

<script>
const RAW = {leads_json};

function scoreColor(s) {{
  if (s >= 70) return '#e8ff47';
  if (s >= 40) return '#ff7b2e';
  if (s >= 20) return '#ff4757';
  return '#5a6475';
}}

function tagHtml(lead) {{
  let t = '';
  if (lead.has_tax_delinquency)    t += '<span class="tag tax">Tax Delinquency</span>';
  if (lead.has_code_violation)     t += '<span class="tag code">Code Violation</span>';
  if (lead.has_probate)            t += '<span class="tag prob">Probate</span>';
  if (lead.has_multiple_liens)     t += '<span class="tag lien">Multiple Liens</span>';
  if (lead.has_divorce_bankruptcy) t += '<span class="tag div">Divorce/BK</span>';
  return t;
}}

function renderCards(leads) {{
  const grid = document.getElementById('grid');
  document.getElementById('count-display').textContent = `${{leads.length}} leads`;
  if (!leads.length) {{
    grid.innerHTML = '<div class="empty">No leads match your filter.</div>';
    return;
  }}
  grid.innerHTML = leads.map(l => {{
    const c = scoreColor(l.seller_score);
    return `
    <div class="card" style="--score-color:${{c}}"
         title="${{(l.score_reasons||[]).join(' | ')}}">
      <div class="card-top">
        <span class="doc-type">${{l.doc_type||'—'}}</span>
        <span class="score-badge">${{l.seller_score}}</span>
      </div>
      <div class="grantor">${{l.grantor||'Unknown Owner'}}</div>
      <div class="address">${{l.property_address||l.legal_description||'No address'}}</div>
      <div class="card-meta">
        <div class="meta-item">
          <div class="meta-label">Doc #</div>
          <div class="meta-value">${{l.document_number||'—'}}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Filed</div>
          <div class="meta-value">${{l.file_date||'—'}}</div>
        </div>
        <div class="meta-item">
          <div class="meta-label">Grantee</div>
          <div class="meta-value">${{l.grantee||'—'}}</div>
        </div>
      </div>
      <div class="tags">${{tagHtml(l)}}</div>
    </div>`;
  }}).join('');
}}

function initStats(leads) {{
  const total  = leads.length;
  const high   = leads.filter(l => l.seller_score >= 70).length;
  const avgScore = total ? Math.round(leads.reduce((a,l) => a + l.seller_score, 0) / total) : 0;
  document.getElementById('header-stats').innerHTML = `
    <div class="stat"><div class="num">${{total}}</div><div class="lbl">Total Leads</div></div>
    <div class="stat"><div class="num">${{high}}</div><div class="lbl">High Score ≥70</div></div>
    <div class="stat"><div class="num">${{avgScore}}</div><div class="lbl">Avg Score</div></div>
  `;
  const ts = RAW.generated_at ? new Date(RAW.generated_at).toLocaleString() : '';
  document.getElementById('footer-ts').textContent = 'Generated: ' + ts;
}}

let currentFilter = 'all';
let currentSearch = '';

function applyFilters() {{
  let leads = [...RAW.leads];
  if (currentFilter === 'tax')     leads = leads.filter(l => l.has_tax_delinquency);
  if (currentFilter === 'code')    leads = leads.filter(l => l.has_code_violation);
  if (currentFilter === 'probate') leads = leads.filter(l => l.has_probate);
  if (currentFilter === 'lien')    leads = leads.filter(l => l.has_multiple_liens);
  if (currentFilter === 'div')     leads = leads.filter(l => l.has_divorce_bankruptcy);
  if (currentSearch) {{
    const q = currentSearch.toLowerCase();
    leads = leads.filter(l =>
      (l.grantor||'').toLowerCase().includes(q) ||
      (l.grantee||'').toLowerCase().includes(q) ||
      (l.property_address||'').toLowerCase().includes(q) ||
      (l.document_number||'').toLowerCase().includes(q)
    );
  }}
  renderCards(leads);
}}

document.querySelectorAll('.filter-btn').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentFilter = btn.dataset.filter;
    applyFilters();
  }});
}});

document.getElementById('search').addEventListener('input', e => {{
  currentSearch = e.target.value.trim();
  applyFilters();
}});

// Init
initStats(RAW.leads || []);
document.querySelector('[data-filter="all"]').classList.add('active');
applyFilters();
</script>
</body>
</html>
"""

    DASHBOARD_HTML.write_text(html, encoding="utf-8")
    log.info("Saved dashboard → %s", DASHBOARD_HTML)


# ─── Entry Point ─────────────────────────────────────────────────────────────
def main() -> None:
    log.info("╔══════════════════════════════════════════════╗")
    log.info("║  SD County Motivated Seller Lead Scraper     ║")
    log.info("╚══════════════════════════════════════════════╝")

    # 1. Scrape
    raw_leads = scrape_all()

    # 2. Deduplicate
    leads = deduplicate(raw_leads)

    # 3. Score — pass full lead list for multi-lien cross-check
    for lead in leads:
        score_lead(lead, leads)

    # 4. Sort by score descending
    leads.sort(key=lambda l: l.seller_score, reverse=True)

    # 5. Save outputs
    save_json(leads)
    generate_dashboard(leads)

    # 6. Summary
    log.info("─" * 50)
    log.info("  Leads saved  : %d", len(leads))
    high = [l for l in leads if l.seller_score >= 70]
    log.info("  High scores  : %d  (score ≥ 70)", len(high))
    if leads:
        log.info("  Top lead     : %s  [score=%d]", leads[0].grantor, leads[0].seller_score)
    log.info("  JSON output  : %s", OUTPUT_JSON)
    log.info("  Dashboard    : %s", DASHBOARD_HTML)
    log.info("─" * 50)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def _find_col(headers: list[str], keywords: list[str]) -> Optional[int]:
    """Return the column index whose header contains any of the keywords."""
    for i, h in enumerate(headers):
        if any(kw in h for kw in keywords):
            return i
    return None


def _cell(cells, idx: Optional[int]) -> str:
    """Safely extract text from a table cell by index."""
    if idx is None or idx >= len(cells):
        return ""
    return cells[idx].get_text(" ", strip=True)


if __name__ == "__main__":
    main()
