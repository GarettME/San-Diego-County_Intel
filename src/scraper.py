"""
San Diego County Motivated Seller Lead Scraper
===============================================
Pulls data from the SD County Open Data Portal (Socrata API) — no login,
no browser required, no blocking. Uses three real public datasets:

  1. Building Permits  — SD County unincorporated (gs2m-invt)
  2. Building Permits  — City of San Diego (dyzh-7eat)
  3. Code Enforcement  — City of San Diego (scsb-hfcn)

Socrata API pattern:
  https://data.sandiegocounty.gov/resource/<DATASET_ID>.json
  https://data.sandiego.gov/resource/<DATASET_ID>.json

Distress scoring model:
  - Tax delinquency   : +30 points
  - Code violation    : +25 points
  - Probate filing    : +20 points
  - Multiple liens    : +15 points
  - Divorce/bankruptcy: +10 points

FIXES in this version:
  - City permits now fetched from data.sandiego.gov (not sandiegocounty.gov)
  - $where filters use lowercase field names matching Socrata schema
  - $order fields lowercased to match actual column names
  - Added field-name discovery logging so you can see what columns exist
  - Fallback: if filtered query returns 0 rows, retries without $where filter
  - Distress keyword scan broadened to catch more signals from raw text
"""

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Socrata API Config ───────────────────────────────────────────────────────
# FIX: Two separate Socrata domains — county vs city
COUNTY_DOMAIN    = "data.sandiegocounty.gov"
CITY_DOMAIN      = "data.sandiego.gov"
COUNTY_BASE      = f"https://{COUNTY_DOMAIN}/resource"
CITY_BASE        = f"https://{CITY_DOMAIN}/resource"

PAGE_SIZE     = 1000   # Socrata max rows per request
MAX_RECORDS   = 5000   # Cap per dataset to keep runs fast
REQUEST_DELAY = 0.5    # seconds between API calls

# Output paths
PROJECT_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR       = PROJECT_ROOT / "data"
DASHBOARD_DIR  = PROJECT_ROOT / "dashboard"
OUTPUT_JSON    = DATA_DIR / "output.json"
DASHBOARD_HTML = DASHBOARD_DIR / "index.html"

# ─── Distress keyword maps ────────────────────────────────────────────────────
TAX_KEYWORDS        = ["tax default","delinquent","tax lien","tax deed","ttc","treasurer"]
CODE_KEYWORDS       = ["code violation","code enforcement","unpermitted","illegal","abatement",
                       "nuisance","blight","unsafe","substandard","red tag","stop work"]
PROBATE_KEYWORDS    = ["probate","estate","decedent","trust","trustee sale","successor"]
LIEN_KEYWORDS       = ["lien","notice of default","lis pendens","mechanic","judgment lien"]
DIVORCE_BK_KEYWORDS = ["divorce","dissolution","bankruptcy","bankrupt","chapter 7","chapter 13"]

# FIX: All status strings lowercased to match what Socrata actually returns
DISTRESS_STATUSES = [
    "expired",
    "cancelled",
    "revoked",
    "denied",
    "voided",
    "application expired",
    "permit expired",
    "application cancelled",
    "issued - not finaled",
    "stop work",
    "withdrawn",
    "incomplete",
]

# ─── Data Model ──────────────────────────────────────────────────────────────
@dataclass
class Lead:
    document_number:   str = ""
    file_date:         str = ""
    doc_type:          str = ""
    grantor:           str = ""
    grantee:           str = ""
    legal_description: str = ""
    property_address:  str = ""

    has_tax_delinquency:    bool = False
    has_code_violation:     bool = False
    has_probate:            bool = False
    has_multiple_liens:     bool = False
    has_divorce_bankruptcy: bool = False

    seller_score:  int  = 0
    score_reasons: list = field(default_factory=list)
    source_url:    str  = ""
    scraped_at:    str  = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ─── HTTP helpers ─────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "SDLeadScraper/2.1 (public data research)",
        "Accept":     "application/json",
    })
    return s


def socrata_get(session: requests.Session, url: str, params: dict) -> list[dict]:
    """Single paginated Socrata API call. Returns list of records or []."""
    try:
        time.sleep(REQUEST_DELAY)
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Socrata sometimes returns an error dict instead of a list
        if isinstance(data, dict) and "error" in data:
            log.error("Socrata API error: %s — url: %s params: %s", data, url, params)
            return []
        return data
    except requests.exceptions.HTTPError as e:
        log.error("HTTP %s — %s | params=%s", e.response.status_code, url, params)
        try:
            log.error("Response body: %s", e.response.text[:400])
        except Exception:
            pass
    except requests.exceptions.ConnectionError:
        log.error("Connection error — %s", url)
    except requests.exceptions.Timeout:
        log.error("Timeout — %s", url)
    except Exception as e:
        log.error("Unexpected error — %s: %s", url, e)
    return []


def fetch_all_pages(session: requests.Session, base_url: str,
                    extra_params: dict = None,
                    max_records: int = MAX_RECORDS) -> list[dict]:
    """
    Paginate through a Socrata endpoint using $limit / $offset.
    If extra_params contains a $where clause and the first page returns 0 rows,
    retries without the filter so we get *something* rather than nothing.
    """
    all_records = []
    offset = 0

    while len(all_records) < max_records:
        limit  = min(PAGE_SIZE, max_records - len(all_records))
        params = {"$limit": limit, "$offset": offset}
        if extra_params:
            params.update(extra_params)

        batch = socrata_get(session, base_url, params)

        # FIX: If filtered query returns nothing on first page, retry without $where
        if not batch and offset == 0 and extra_params and "$where" in extra_params:
            log.warning("   $where filter returned 0 rows — retrying without filter to test endpoint…")
            fallback_params = {k: v for k, v in params.items() if k != "$where"}
            fallback_params.pop("$order", None)   # also drop $order in case field name is wrong
            batch = socrata_get(session, base_url, fallback_params)
            if batch:
                log.warning("   Fallback returned %d rows — your $where field names may be wrong!", len(batch))
                log.warning("   Sample record keys: %s", list(batch[0].keys()))
                # Log a sample status value so you can fix the filter
                for key in batch[0]:
                    if "status" in key.lower():
                        log.warning("   Status field '%s' sample value: %s", key, batch[0][key])
                # Return fallback data so we still get leads (keyword scan will filter distress)
                all_records.extend(batch)
                break
            else:
                log.error("   Endpoint returned 0 rows even without filters. Dataset may be unavailable.")
                break

        if not batch:
            break

        all_records.extend(batch)
        log.info("   Fetched %d records (total so far: %d)", len(batch), len(all_records))

        if len(batch) < limit:
            break   # last page

        offset += limit

    return all_records


# ─── Dataset-specific parsers ─────────────────────────────────────────────────

def parse_county_permit(record: dict) -> Optional[Lead]:
    """
    Parse one record from the SD County Building Permits dataset (gs2m-invt).
    Field names from this dataset are mixed-case; we try both cases.
    """
    try:
        # FIX: Try lowercase first (Socrata normalises to lowercase in most responses)
        status = (record.get("statuscurrent") or record.get("StatusCurrent") or "").lower().strip()
        desc   = (record.get("description")   or record.get("Description")   or "").lower()
        ptype  = (
            record.get("permittypedesc")   or record.get("PermitTypeDesc")   or
            record.get("permittypemapped") or record.get("PermitTypeMapped") or
            record.get("permittype")       or record.get("PermitType")       or ""
        ).lower()

        addr = _join(
            record.get("originaladdress1") or record.get("OriginalAddress1") or "",
            record.get("originalcity")     or record.get("OriginalCity")     or "",
            "CA",
            record.get("originalzip")      or record.get("OriginalZip")      or "",
        )

        lead = Lead(
            document_number   = record.get("permitnum")   or record.get("PermitNum")   or "",
            file_date         = _format_date(
                                    record.get("applieddate") or record.get("AppliedDate") or ""),
            doc_type          = f"PERMIT — {ptype.upper()}" if ptype else "BUILDING PERMIT",
            grantor           = "",   # county permit data doesn't include owner name
            grantee           = _join(
                                    record.get("contractorfullname")    or record.get("ContractorFullName")    or "",
                                    record.get("contractorcompanyname") or record.get("ContractorCompanyName") or "",
                                ),
            legal_description = f"PIN: {record.get('pin') or record.get('PIN') or ''}",
            property_address  = addr,
            source_url        = f"https://{COUNTY_DOMAIN}/Housing-and-Infrastructure/Building-Permits/gs2m-invt",
        )

        combined = f"{status} {desc} {ptype}"

        # FIX: Check lowercase status against lowercase DISTRESS_STATUSES list
        lead.has_code_violation     = _matches(combined, CODE_KEYWORDS) or \
                                      any(s in status for s in DISTRESS_STATUSES)
        lead.has_tax_delinquency    = _matches(combined, TAX_KEYWORDS)
        lead.has_probate            = _matches(combined, PROBATE_KEYWORDS)
        lead.has_divorce_bankruptcy = _matches(combined, DIVORCE_BK_KEYWORDS)

        return lead

    except Exception as e:
        log.debug("Skipping county permit record: %s", e)
        return None


def parse_city_permit(record: dict) -> Optional[Lead]:
    """
    Parse one record from the City of SD Building Permits dataset (dyzh-7eat).
    Fetched from data.sandiego.gov — different field names than county dataset.
    """
    try:
        status = (record.get("status") or "").lower().strip()
        desc   = (record.get("description")       or record.get("work_description") or "").lower()
        ptype  = (record.get("permit_type")        or record.get("work_description") or "").lower()

        addr = _join(
            record.get("address")             or "",
            record.get("city", "San Diego"),
            "CA",
            record.get("zip")                 or "",
        )

        lead = Lead(
            document_number   = record.get("permit_number") or record.get("project_id") or "",
            file_date         = _format_date(
                                    record.get("date_application_filed") or
                                    record.get("date_issued")            or ""),
            doc_type          = f"PERMIT — {ptype.upper()}" if ptype else "BUILDING PERMIT",
            grantor           = record.get("owner_name") or "",
            grantee           = record.get("contractor_name") or "",
            legal_description = record.get("apn") or "",
            property_address  = addr,
            # FIX: correct source URL domain for city dataset
            source_url        = f"https://{CITY_DOMAIN}/datasets/building-permits/",
        )

        combined = f"{status} {desc} {ptype}"

        lead.has_code_violation     = _matches(combined, CODE_KEYWORDS) or \
                                      any(s in status for s in DISTRESS_STATUSES)
        lead.has_tax_delinquency    = _matches(combined, TAX_KEYWORDS)
        lead.has_probate            = _matches(combined, PROBATE_KEYWORDS)
        lead.has_divorce_bankruptcy = _matches(combined, DIVORCE_BK_KEYWORDS)

        return lead

    except Exception as e:
        log.debug("Skipping city permit record: %s", e)
        return None


def parse_code_enforcement(record: dict) -> Optional[Lead]:
    """
    Parse one record from the City of SD Code Enforcement dataset (scsb-hfcn).
    Every record here is automatically a code violation lead.
    """
    try:
        case_type = (
            record.get("case_type")       or
            record.get("violation_type")  or
            "CODE ENFORCEMENT"
        ).upper()
        status = (record.get("status") or "").lower()
        desc   = (record.get("violation_description") or record.get("description") or "").lower()

        addr = _join(
            record.get("address")         or "",
            record.get("city", "San Diego"),
            "CA",
            record.get("zip")             or "",
        )

        lead = Lead(
            document_number   = record.get("case_number") or record.get("record_id") or "",
            file_date         = _format_date(
                                    record.get("date_opened") or record.get("open_date") or ""),
            doc_type          = f"CODE ENFORCEMENT — {case_type}",
            grantor           = record.get("owner") or "",
            grantee           = "",
            legal_description = record.get("apn") or "",
            property_address  = addr,
            source_url        = f"https://{CITY_DOMAIN}/datasets/code-enforcement-violations/",
        )

        lead.has_code_violation = True   # Every record is a code enforcement case

        combined = f"{desc} {status} {case_type}"
        lead.has_tax_delinquency    = _matches(combined, TAX_KEYWORDS)
        lead.has_probate            = _matches(combined, PROBATE_KEYWORDS)
        lead.has_divorce_bankruptcy = _matches(combined, DIVORCE_BK_KEYWORDS)

        return lead

    except Exception as e:
        log.debug("Skipping code enforcement record: %s", e)
        return None


# ─── Scoring ──────────────────────────────────────────────────────────────────
def score_lead(lead: Lead, all_leads: list["Lead"]) -> Lead:
    score   = 0
    reasons = []

    if lead.has_tax_delinquency:
        score += 30
        reasons.append("Tax delinquency (+30)")

    if lead.has_code_violation:
        score += 25
        reasons.append("Code violation (+25)")

    if lead.has_probate:
        score += 20
        reasons.append("Probate filing (+20)")

    if lead.has_divorce_bankruptcy:
        score += 10
        reasons.append("Divorce/bankruptcy (+10)")

    if lead.property_address:
        addr_key = lead.property_address.lower().split(",")[0].strip()
        same_addr = [
            l for l in all_leads
            if l is not lead
            and l.property_address.lower().split(",")[0].strip() == addr_key
        ]
        if len(same_addr) >= 1:
            lead.has_multiple_liens = True
            score += 15
            reasons.append(f"Multiple records same address ({len(same_addr)+1} total, +15)")

    lead.seller_score  = min(score, 100)
    lead.score_reasons = reasons
    return lead


# ─── Main orchestration ───────────────────────────────────────────────────────
def scrape_all() -> list[Lead]:
    session   = build_session()
    all_leads: list[Lead] = []

    # ── 1. SD County Building Permits (unincorporated areas) ──────────────────
    log.info("── Fetching SD County Building Permits (gs2m-invt)…")
    url = f"{COUNTY_BASE}/gs2m-invt.json"
    # FIX: Use lowercase field names; Socrata schema uses lowercase
    params = {
        "$where": (
            "statuscurrent in('Expired','Application Expired','Cancelled',"
            "'Revoked','Denied','Issued - Not Finaled')"
        ),
        "$order": "issueddate DESC",
    }
    records = fetch_all_pages(session, url, extra_params=params)
    log.info("   Parsing %d county permit records…", len(records))
    for r in records:
        lead = parse_county_permit(r)
        if lead:
            all_leads.append(lead)

    # ── 2. City of SD Building Permits ────────────────────────────────────────
    # FIX: Use CITY_BASE (data.sandiego.gov), NOT COUNTY_BASE
    log.info("── Fetching City of SD Building Permits (dyzh-7eat)…")
    url = f"{CITY_BASE}/dyzh-7eat.json"
    params = {
        "$where": (
            "status in('Expired','Application Expired','Cancelled',"
            "'Revoked','Denied','Issued - Not Finaled')"
        ),
        "$order": "date_issued DESC",
    }
    records = fetch_all_pages(session, url, extra_params=params)
    log.info("   Parsing %d city permit records…", len(records))
    for r in records:
        lead = parse_city_permit(r)
        if lead:
            all_leads.append(lead)

    # ── 3. City Code Enforcement ──────────────────────────────────────────────
    log.info("── Fetching City Code Enforcement (scsb-hfcn)…")
    url = f"{CITY_BASE}/scsb-hfcn.json"
    params = {"$order": "date_opened DESC"}
    records = fetch_all_pages(session, url, extra_params=params)
    log.info("   Parsing %d code enforcement records…", len(records))
    for r in records:
        lead = parse_code_enforcement(r)
        if lead:
            all_leads.append(lead)

    log.info("Total raw leads collected: %d", len(all_leads))
    return all_leads


def deduplicate(leads: list[Lead]) -> list[Lead]:
    seen: set[str] = set()
    unique = []
    for lead in leads:
        key = (lead.document_number or lead.property_address or str(id(lead))).strip().lower()
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    log.info("After dedup: %d unique leads", len(unique))
    return unique


def filter_has_distress(leads: list[Lead]) -> list[Lead]:
    filtered = [l for l in leads if l.seller_score > 0]
    log.info("Leads with distress signals: %d", len(filtered))
    return filtered


# ─── Output ───────────────────────────────────────────────────────────────────
def save_json(leads: list[Lead]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_leads":  len(leads),
        "leads":        [asdict(l) for l in leads],
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    log.info("Saved → %s (%d leads)", OUTPUT_JSON, len(leads))


def generate_dashboard(leads: list[Lead]) -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    leads_json = json.dumps([asdict(l) for l in leads], indent=2, default=str)
    generated  = datetime.now(timezone.utc).isoformat()

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
    --bg:#0a0d14;--surface:#111520;--border:#1e2535;
    --accent:#e8ff47;--accent2:#ff4757;--text:#d4dbe8;--text-dim:#5a6475;
    --green:#39d98a;--orange:#ff7b2e;
    --mono:'Space Mono',monospace;--sans:'Syne',sans-serif;
  }}
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh}}
  header{{border-bottom:1px solid var(--border);padding:2rem 3rem;display:flex;align-items:flex-end;justify-content:space-between;gap:1rem;flex-wrap:wrap}}
  .logo-block h1{{font-size:clamp(1.6rem,3vw,2.4rem);font-weight:800;letter-spacing:-.04em;line-height:1}}
  .logo-block h1 span{{color:var(--accent)}}
  .logo-block p{{font-family:var(--mono);font-size:.72rem;color:var(--text-dim);margin-top:.4rem;letter-spacing:.08em;text-transform:uppercase}}
  .stats-row{{display:flex;gap:2rem;flex-wrap:wrap}}
  .stat .num{{font-family:var(--mono);font-size:1.8rem;font-weight:700;color:var(--accent);line-height:1}}
  .stat .lbl{{font-size:.65rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;margin-top:.15rem}}
  .controls{{padding:1.5rem 3rem;display:flex;gap:1rem;align-items:center;flex-wrap:wrap;border-bottom:1px solid var(--border)}}
  .search-box{{flex:1;min-width:200px;max-width:380px;background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:.6rem 1rem;color:var(--text);font-family:var(--mono);font-size:.8rem;outline:none;transition:border-color .2s}}
  .search-box:focus{{border-color:var(--accent)}}
  .filter-btn{{background:var(--surface);border:1px solid var(--border);color:var(--text-dim);padding:.6rem 1.1rem;border-radius:4px;font-family:var(--mono);font-size:.72rem;cursor:pointer;text-transform:uppercase;letter-spacing:.05em;transition:all .15s}}
  .filter-btn:hover,.filter-btn.active{{border-color:var(--accent);color:var(--accent);background:rgba(232,255,71,.06)}}
  #count-display{{font-family:var(--mono);font-size:.72rem;color:var(--text-dim);margin-left:auto}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:1px;background:var(--border)}}
  .card{{background:var(--surface);padding:1.4rem 1.6rem;cursor:pointer;transition:background .15s;position:relative;overflow:hidden}}
  .card:hover{{background:#161b28}}
  .card::before{{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:var(--score-color,var(--text-dim))}}
  .card-top{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:.8rem}}
  .doc-type{{font-family:var(--mono);font-size:.65rem;text-transform:uppercase;letter-spacing:.1em;color:var(--text-dim);background:var(--border);padding:.2rem .5rem;border-radius:2px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .score-badge{{font-family:var(--mono);font-size:.8rem;font-weight:700;padding:.2rem .6rem;border-radius:2px;border:1px solid var(--score-color,var(--text-dim));color:var(--score-color,var(--text-dim));background:rgba(255,255,255,.03)}}
  .grantor{{font-size:1.05rem;font-weight:600;letter-spacing:-.02em;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .address{{font-family:var(--mono);font-size:.72rem;color:var(--text-dim);margin-top:.25rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .card-meta{{margin-top:1rem;display:grid;grid-template-columns:1fr 1fr;gap:.4rem 1rem}}
  .meta-label{{font-family:var(--mono);font-size:.58rem;text-transform:uppercase;letter-spacing:.1em;color:var(--text-dim)}}
  .meta-value{{font-family:var(--mono);font-size:.72rem;color:var(--text);margin-top:.1rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .tags{{margin-top:.9rem;display:flex;flex-wrap:wrap;gap:.35rem}}
  .tag{{font-family:var(--mono);font-size:.58rem;padding:.15rem .45rem;border-radius:2px;text-transform:uppercase;letter-spacing:.06em}}
  .tag.tax{{background:rgba(57,217,138,.1);color:var(--green);border:1px solid rgba(57,217,138,.25)}}
  .tag.code{{background:rgba(255,123,46,.1);color:var(--orange);border:1px solid rgba(255,123,46,.25)}}
  .tag.prob{{background:rgba(232,255,71,.1);color:var(--accent);border:1px solid rgba(232,255,71,.25)}}
  .tag.lien{{background:rgba(255,71,87,.12);color:var(--accent2);border:1px solid rgba(255,71,87,.25)}}
  .tag.div{{background:rgba(147,112,219,.12);color:#b39ddb;border:1px solid rgba(147,112,219,.25)}}
  .empty{{grid-column:1/-1;text-align:center;padding:5rem 2rem;color:var(--text-dim);font-family:var(--mono);font-size:.8rem}}
  footer{{padding:1.5rem 3rem;border-top:1px solid var(--border);font-family:var(--mono);font-size:.65rem;color:var(--text-dim);display:flex;justify-content:space-between;flex-wrap:wrap;gap:.5rem}}
</style>
</head>
<body>
<header>
  <div class="logo-block">
    <h1>SD<span> Leads</span></h1>
    <p>San Diego County · Motivated Seller Intelligence · Live Open Data</p>
  </div>
  <div class="stats-row" id="header-stats"></div>
</header>
<div class="controls">
  <input class="search-box" type="text" id="search" placeholder="Search address, doc #…"/>
  <button class="filter-btn" data-filter="all">All</button>
  <button class="filter-btn" data-filter="tax">Tax</button>
  <button class="filter-btn" data-filter="code">Code</button>
  <button class="filter-btn" data-filter="probate">Probate</button>
  <button class="filter-btn" data-filter="lien">Multi-Lien</button>
  <button class="filter-btn" data-filter="div">Divorce/BK</button>
  <span id="count-display"></span>
</div>
<div class="grid" id="grid"></div>
<footer>
  <span>Source: SD County &amp; City Open Data Portals (Socrata API) — Public Records</span>
  <span id="footer-ts"></span>
</footer>
<script>
const RAW = {leads_json};
const META_GENERATED = "{generated}";

function scoreColor(s) {{
  if (s>=70) return '#e8ff47';
  if (s>=40) return '#ff7b2e';
  if (s>=20) return '#ff4757';
  return '#5a6475';
}}
function tagHtml(l) {{
  let t='';
  if(l.has_tax_delinquency)    t+='<span class="tag tax">Tax Delinquency</span>';
  if(l.has_code_violation)     t+='<span class="tag code">Code Violation</span>';
  if(l.has_probate)            t+='<span class="tag prob">Probate</span>';
  if(l.has_multiple_liens)     t+='<span class="tag lien">Multi-Lien</span>';
  if(l.has_divorce_bankruptcy) t+='<span class="tag div">Divorce/BK</span>';
  return t;
}}
function renderCards(leads) {{
  const grid=document.getElementById('grid');
  document.getElementById('count-display').textContent=`${{leads.length}} leads`;
  if(!leads.length){{grid.innerHTML='<div class="empty">No leads match your filter.</div>';return;}}
  grid.innerHTML=leads.map(l=>{{
    const c=scoreColor(l.seller_score);
    return `<div class="card" style="--score-color:${{c}}" title="${{(l.score_reasons||[]).join(' | ')}}">
      <div class="card-top">
        <span class="doc-type">${{l.doc_type||'—'}}</span>
        <span class="score-badge">${{l.seller_score}}</span>
      </div>
      <div class="grantor">${{l.grantor||l.grantee||'Property Record'}}</div>
      <div class="address">${{l.property_address||l.legal_description||'No address'}}</div>
      <div class="card-meta">
        <div><div class="meta-label">Doc #</div><div class="meta-value">${{l.document_number||'—'}}</div></div>
        <div><div class="meta-label">Filed</div><div class="meta-value">${{l.file_date||'—'}}</div></div>
        <div><div class="meta-label">Grantee</div><div class="meta-value">${{l.grantee||'—'}}</div></div>
      </div>
      <div class="tags">${{tagHtml(l)}}</div>
    </div>`;
  }}).join('');
}}
function initStats(leads) {{
  const total=leads.length;
  const high=leads.filter(l=>l.seller_score>=70).length;
  const avg=total?Math.round(leads.reduce((a,l)=>a+l.seller_score,0)/total):0;
  document.getElementById('header-stats').innerHTML=`
    <div class="stat"><div class="num">${{total}}</div><div class="lbl">Total Leads</div></div>
    <div class="stat"><div class="num">${{high}}</div><div class="lbl">High Score ≥70</div></div>
    <div class="stat"><div class="num">${{avg}}</div><div class="lbl">Avg Score</div></div>`;
  document.getElementById('footer-ts').textContent='Generated: '+new Date(META_GENERATED).toLocaleString();
}}
let currentFilter='all', currentSearch='';
function applyFilters() {{
  let leads=[...RAW];
  if(currentFilter==='tax')     leads=leads.filter(l=>l.has_tax_delinquency);
  if(currentFilter==='code')    leads=leads.filter(l=>l.has_code_violation);
  if(currentFilter==='probate') leads=leads.filter(l=>l.has_probate);
  if(currentFilter==='lien')    leads=leads.filter(l=>l.has_multiple_liens);
  if(currentFilter==='div')     leads=leads.filter(l=>l.has_divorce_bankruptcy);
  if(currentSearch) {{
    const q=currentSearch.toLowerCase();
    leads=leads.filter(l=>(l.property_address||'').toLowerCase().includes(q)||
      (l.document_number||'').toLowerCase().includes(q)||
      (l.grantor||'').toLowerCase().includes(q)||(l.grantee||'').toLowerCase().includes(q));
  }}
  renderCards(leads);
}}
document.querySelectorAll('.filter-btn').forEach(btn=>{{
  btn.addEventListener('click',()=>{{
    document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    currentFilter=btn.dataset.filter;
    applyFilters();
  }});
}});
document.getElementById('search').addEventListener('input',e=>{{currentSearch=e.target.value.trim();applyFilters();}});
initStats(RAW);
document.querySelector('[data-filter="all"]').classList.add('active');
applyFilters();
</script>
</body>
</html>"""

    DASHBOARD_HTML.write_text(html, encoding="utf-8")
    log.info("Dashboard saved → %s", DASHBOARD_HTML)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _matches(text: str, keywords: list[str]) -> bool:
    t = text.lower()
    return any(k in t for k in keywords)

def _join(*parts) -> str:
    return ", ".join(p.strip() for p in parts if p and p.strip())

def _format_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y %H:%M:%S %p",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(raw[:len(fmt)], fmt).strftime("%m/%d/%Y")
        except Exception:
            pass
    return raw[:10]


# ─── Entry point ──────────────────────────────────────────────────────────────
def main():
    log.info("╔══════════════════════════════════════════════╗")
    log.info("║  SD County Motivated Seller Lead Scraper v2.1 ║")
    log.info("║  Source: SD County Open Data (Socrata API)    ║")
    log.info("╚══════════════════════════════════════════════╝")

    leads = scrape_all()
    leads = deduplicate(leads)

    for lead in leads:
        score_lead(lead, leads)

    leads = filter_has_distress(leads)
    leads.sort(key=lambda l: l.seller_score, reverse=True)

    save_json(leads)
    generate_dashboard(leads)

    log.info("─" * 50)
    log.info("  Total leads  : %d", len(leads))
    log.info("  High (≥70)   : %d", sum(1 for l in leads if l.seller_score >= 70))
    log.info("  Medium (40+) : %d", sum(1 for l in leads if 40 <= l.seller_score < 70))
    if leads:
        log.info("  Top lead     : %s [score=%d]", leads[0].property_address, leads[0].seller_score)
    log.info("─" * 50)


if __name__ == "__main__":
    main()
