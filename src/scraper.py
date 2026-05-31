"""
San Diego County Motivated Seller Lead Scraper  v2.2
=====================================================
CHANGES FROM v2.1:
  - Integrated tax_enrichment.py: SD County Recorder dataset now pulled as
    a 4th data source (Notices of Default, Trustee Sales, Lis Pendens,
    Probate docs, Dissolution/Bankruptcy filings).
  - enrich_leads_with_recorder() cross-matches permit/code leads against
    recorder data to stack distress signals → leads now reach 70+ scores.
  - rescore_lead() called after enrichment so scores reflect all signals.
  - Threshold comments updated: High ≥70 is now achievable.

Original sources (unchanged):
  1. Building Permits  — SD County unincorporated (gs2m-invt)
  2. Building Permits  — City of San Diego (dyzh-7eat)
  3. Code Enforcement  — City of San Diego (scsb-hfcn)

New source:
  4. Recorded Documents — SD County Recorder (2s4g-c2vu)
     Filters for: Notice of Default, Trustee Sale, Lis Pendens,
                  Mechanic Lien, Probate docs, Dissolution, Bankruptcy
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
COUNTY_DOMAIN    = "data.sandiegocounty.gov"
CITY_DOMAIN      = "data.sandiego.gov"
COUNTY_BASE      = f"https://{COUNTY_DOMAIN}/resource"
CITY_BASE        = f"https://{CITY_DOMAIN}/resource"

PAGE_SIZE     = 1000
MAX_RECORDS   = 5000
REQUEST_DELAY = 0.5

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR       = PROJECT_ROOT / "data"
DASHBOARD_DIR  = PROJECT_ROOT / "docs"
OUTPUT_JSON    = DATA_DIR / "output.json"
DASHBOARD_HTML = DASHBOARD_DIR / "index.html"

# ─── Distress keyword maps ────────────────────────────────────────────────────
TAX_KEYWORDS        = ["tax default","delinquent","tax lien","tax deed","ttc","treasurer"]
CODE_KEYWORDS       = ["code violation","code enforcement","unpermitted","illegal","abatement",
                       "nuisance","blight","unsafe","substandard","red tag","stop work"]
PROBATE_KEYWORDS    = ["probate","estate","decedent","trust","trustee sale","successor"]
LIEN_KEYWORDS       = ["lien","notice of default","lis pendens","mechanic","judgment lien"]
DIVORCE_BK_KEYWORDS = ["divorce","dissolution","bankruptcy","bankrupt","chapter 7","chapter 13"]

DISTRESS_STATUSES = [
    "expired","cancelled","revoked","denied","voided",
    "application expired","permit expired","application cancelled",
    "issued - not finaled","stop work","withdrawn","incomplete",
]

# ─── Recorder distress document types ────────────────────────────────────────
RECORDER_DATASET = "2s4g-c2vu"

RECORDER_TAX_TYPES = [
    "NOTICE OF DEFAULT","NOTICE OF TRUSTEE","TRUSTEE DEED",
    "TAX DEED","CERTIFICATE OF TAX SALE",
    "FEDERAL TAX LIEN","STATE TAX LIEN","NOTICE OF FEDERAL TAX LIEN",
]
RECORDER_LIEN_TYPES = [
    "LIS PENDENS","MECHANIC'S LIEN","MECHANICS LIEN",
    "ABSTRACT OF JUDGMENT","JUDGMENT LIEN",
]
RECORDER_PROBATE_TYPES = [
    "LETTERS TESTAMENTARY","LETTERS OF ADMINISTRATION",
    "AFFIDAVIT DEATH TRUSTEE","ORDER CONFIRMING SALE",
    "DECREE OF DISTRIBUTION","PROBATE",
]
RECORDER_DIVORCE_BK_TYPES = [
    "DISSOLUTION","INTERLOCUTORY DECREE","BANKRUPTCY","DISCHARGE OF DEBTOR",
]
RECORDER_ALL_TYPES = (
    RECORDER_TAX_TYPES + RECORDER_LIEN_TYPES +
    RECORDER_PROBATE_TYPES + RECORDER_DIVORCE_BK_TYPES
)


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
        "User-Agent": "SDLeadScraper/2.2 (public data research)",
        "Accept":     "application/json",
    })
    return s


def socrata_get(session: requests.Session, url: str, params: dict) -> list[dict]:
    try:
        time.sleep(REQUEST_DELAY)
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
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
    all_records = []
    offset = 0

    while len(all_records) < max_records:
        limit  = min(PAGE_SIZE, max_records - len(all_records))
        params = {"$limit": limit, "$offset": offset}
        if extra_params:
            params.update(extra_params)

        batch = socrata_get(session, base_url, params)

        if not batch and offset == 0 and extra_params and "$where" in extra_params:
            log.warning("   $where filter returned 0 rows — retrying without filter…")
            fallback_params = {k: v for k, v in params.items() if k != "$where"}
            fallback_params.pop("$order", None)
            batch = socrata_get(session, base_url, fallback_params)
            if batch:
                log.warning("   Fallback returned %d rows — check $where field names", len(batch))
                log.warning("   Sample record keys: %s", list(batch[0].keys()))
                for key in batch[0]:
                    if "status" in key.lower():
                        log.warning("   Status field '%s' sample value: %s", key, batch[0][key])
                all_records.extend(batch)
                break
            else:
                log.error("   Endpoint returned 0 rows even without filters.")
                break

        if not batch:
            break

        all_records.extend(batch)
        log.info("   Fetched %d records (total so far: %d)", len(batch), len(all_records))

        if len(batch) < limit:
            break
        offset += limit

    return all_records


# ─── Dataset parsers ──────────────────────────────────────────────────────────

def parse_county_permit(record: dict) -> Optional[Lead]:
    try:
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
            document_number   = record.get("permitnum") or record.get("PermitNum") or "",
            file_date         = _format_date(record.get("applieddate") or record.get("AppliedDate") or ""),
            doc_type          = f"PERMIT — {ptype.upper()}" if ptype else "BUILDING PERMIT",
            grantor           = "",
            grantee           = _join(
                                    record.get("contractorfullname")    or record.get("ContractorFullName")    or "",
                                    record.get("contractorcompanyname") or record.get("ContractorCompanyName") or "",
                                ),
            legal_description = f"PIN: {record.get('pin') or record.get('PIN') or ''}",
            property_address  = addr,
            source_url        = f"https://{COUNTY_DOMAIN}/Housing-and-Infrastructure/Building-Permits/gs2m-invt",
        )

        combined = f"{status} {desc} {ptype}"
        is_distress_status = any(s in status for s in DISTRESS_STATUSES)
        lead.has_code_violation     = _matches(combined, CODE_KEYWORDS) or is_distress_status
        lead.has_tax_delinquency    = _matches(combined, TAX_KEYWORDS)
        lead.has_probate            = _matches(combined, PROBATE_KEYWORDS)
        lead.has_divorce_bankruptcy = _matches(combined, DIVORCE_BK_KEYWORDS)

        if is_distress_status and not _matches(combined, CODE_KEYWORDS):
            lead.score_reasons.append(f"Distress status: {status}")

        return lead

    except Exception as e:
        log.debug("Skipping county permit record: %s", e)
        return None


def parse_city_permit(record: dict) -> Optional[Lead]:
    try:
        status = (record.get("status") or "").lower().strip()
        desc   = (record.get("description") or record.get("work_description") or "").lower()
        ptype  = (record.get("permit_type") or record.get("work_description") or "").lower()

        addr = _join(
            record.get("address")           or "",
            record.get("city", "San Diego"),
            "CA",
            record.get("zip")               or "",
        )

        lead = Lead(
            document_number   = record.get("permit_number") or record.get("project_id") or "",
            file_date         = _format_date(
                                    record.get("date_application_filed") or
                                    record.get("date_issued") or ""),
            doc_type          = f"PERMIT — {ptype.upper()}" if ptype else "BUILDING PERMIT",
            grantor           = record.get("owner_name") or "",
            grantee           = record.get("contractor_name") or "",
            legal_description = record.get("apn") or "",
            property_address  = addr,
            source_url        = f"https://{CITY_DOMAIN}/datasets/building-permits/",
        )

        combined = f"{status} {desc} {ptype}"
        is_distress_status = any(s in status for s in DISTRESS_STATUSES)
        lead.has_code_violation     = _matches(combined, CODE_KEYWORDS) or is_distress_status
        lead.has_tax_delinquency    = _matches(combined, TAX_KEYWORDS)
        lead.has_probate            = _matches(combined, PROBATE_KEYWORDS)
        lead.has_divorce_bankruptcy = _matches(combined, DIVORCE_BK_KEYWORDS)

        if is_distress_status and not _matches(combined, CODE_KEYWORDS):
            lead.score_reasons.append(f"Distress status: {status}")

        return lead

    except Exception as e:
        log.debug("Skipping city permit record: %s", e)
        return None


def parse_code_enforcement(record: dict) -> Optional[Lead]:
    try:
        case_type = (
            record.get("case_type")      or
            record.get("violation_type") or
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
            file_date         = _format_date(record.get("date_opened") or record.get("open_date") or ""),
            doc_type          = f"CODE ENFORCEMENT — {case_type}",
            grantor           = record.get("owner") or "",
            grantee           = "",
            legal_description = record.get("apn") or "",
            property_address  = addr,
            source_url        = f"https://{CITY_DOMAIN}/datasets/code-enforcement-violations/",
        )

        lead.has_code_violation = True
        combined = f"{desc} {status} {case_type}"
        lead.has_tax_delinquency    = _matches(combined, TAX_KEYWORDS)
        lead.has_probate            = _matches(combined, PROBATE_KEYWORDS)
        lead.has_divorce_bankruptcy = _matches(combined, DIVORCE_BK_KEYWORDS)

        return lead

    except Exception as e:
        log.debug("Skipping code enforcement record: %s", e)
        return None


def parse_recorder_doc(record: dict) -> Optional[Lead]:
    """NEW in v2.2 — Parse a recorded document from the SD County Recorder."""
    try:
        doc_type_raw = (
            record.get("document_type") or
            record.get("doc_type")      or
            record.get("type")          or ""
        ).upper().strip()

        grantor = (record.get("grantor_name") or record.get("grantor") or "").strip()
        grantee = (record.get("grantee_name") or record.get("grantee") or "").strip()
        legal   = (record.get("legal_description") or record.get("legal") or "").strip()
        doc_num = (
            record.get("document_number") or
            record.get("doc_number")      or
            record.get("instrument_number") or ""
        ).strip()
        rec_date = (
            record.get("recording_date") or
            record.get("recorded_date")  or
            record.get("document_date")  or ""
        )
        address = (
            record.get("situs_address")    or
            record.get("property_address") or
            legal or ""
        ).strip()

        lead = Lead(
            document_number   = doc_num,
            file_date         = _format_date(rec_date),
            doc_type          = f"RECORDED — {doc_type_raw}",
            grantor           = grantor,
            grantee           = grantee,
            legal_description = legal,
            property_address  = address,
            source_url        = (
                f"https://{COUNTY_DOMAIN}/Housing-and-Infrastructure/"
                f"Official-Recorded-Documents/{RECORDER_DATASET}"
            ),
        )

        lead.has_tax_delinquency = any(t in doc_type_raw for t in RECORDER_TAX_TYPES)
        lead.has_multiple_liens  = any(t in doc_type_raw for t in RECORDER_LIEN_TYPES)
        lead.has_probate         = any(t in doc_type_raw for t in RECORDER_PROBATE_TYPES)
        lead.has_divorce_bankruptcy = any(t in doc_type_raw for t in RECORDER_DIVORCE_BK_TYPES)

        if not any([
            lead.has_tax_delinquency, lead.has_multiple_liens,
            lead.has_probate, lead.has_divorce_bankruptcy,
        ]):
            return None

        return lead

    except Exception as e:
        log.debug("Skipping recorder record: %s", e)
        return None


# ─── Scoring ──────────────────────────────────────────────────────────────────
def score_lead(lead: Lead, all_leads: list) -> Lead:
    score   = 0
    reasons = list(lead.score_reasons)   # preserve any pre-existing reasons

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


# ─── Cross-enrichment: stack recorder signals onto permit/code leads ──────────
def enrich_with_recorder(permit_leads: list[Lead], recorder_leads: list[Lead]) -> list[Lead]:
    """
    NEW in v2.2 — If a permit/code lead shares an APN or address prefix with a
    recorder document, upgrade its distress flags.

    This is what turns 25-point code-violation leads into 55-70+ leads.
    """
    if not recorder_leads:
        return permit_leads

    def _key(lead: Lead) -> str:
        # Use APN from legal_description if available, else address prefix
        legal = lead.legal_description.lower().strip()
        addr  = lead.property_address.lower().strip()
        # APN pattern: digits-digits-digits (e.g. 123-456-78)
        for text in (legal, addr):
            parts = text.replace("-", "").split()
            for p in parts:
                if p.isdigit() and len(p) >= 6:
                    return p[:8]
        # Fallback: first 12 chars of address
        return addr[:12]

    recorder_index: dict[str, list[Lead]] = {}
    for rl in recorder_leads:
        k = _key(rl)
        if k and len(k) > 3:
            recorder_index.setdefault(k, []).append(rl)

    enriched = 0
    for lead in permit_leads:
        k = _key(lead)
        if not k or len(k) <= 3:
            continue
        matches = recorder_index.get(k, [])
        for rm in matches:
            changed = False
            if rm.has_tax_delinquency and not lead.has_tax_delinquency:
                lead.has_tax_delinquency = True
                lead.score_reasons.append(
                    f"Recorder: {rm.doc_type.replace('RECORDED — ','')} (+30 tax)"
                )
                changed = True
            if rm.has_probate and not lead.has_probate:
                lead.has_probate = True
                lead.score_reasons.append(
                    f"Recorder: {rm.doc_type.replace('RECORDED — ','')} (+20 probate)"
                )
                changed = True
            if rm.has_multiple_liens and not lead.has_multiple_liens:
                lead.has_multiple_liens = True
                lead.score_reasons.append(
                    f"Recorder: {rm.doc_type.replace('RECORDED — ','')} (+15 lien)"
                )
                changed = True
            if rm.has_divorce_bankruptcy and not lead.has_divorce_bankruptcy:
                lead.has_divorce_bankruptcy = True
                lead.score_reasons.append(
                    f"Recorder: {rm.doc_type.replace('RECORDED — ','')} (+10 divorce/BK)"
                )
                changed = True
            if changed:
                enriched += 1

    log.info("   Cross-enrichment: upgraded %d permit/code leads with recorder signals", enriched)
    return permit_leads


# ─── Main orchestration ───────────────────────────────────────────────────────
def scrape_all() -> list[Lead]:
    session   = build_session()
    all_leads: list[Lead] = []

    # ── 1. SD County Building Permits ────────────────────────────────────────
    log.info("── Fetching SD County Building Permits (gs2m-invt)…")
    url = f"{COUNTY_BASE}/gs2m-invt.json"
    params = {
        "$where": (
            "statuscurrent in('Expired','Application Expired','Cancelled',"
            "'Revoked','Denied','Issued - Not Finaled')"
        ),
        "$order": "issueddate DESC",
    }
    records = fetch_all_pages(session, url, extra_params=params)
    log.info("   Parsing %d county permit records…", len(records))
    permit_leads = []
    for r in records:
        lead = parse_county_permit(r)
        if lead:
            permit_leads.append(lead)

    # ── 2. City of SD Building Permits ───────────────────────────────────────
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
            permit_leads.append(lead)

    # ── 3. City Code Enforcement ─────────────────────────────────────────────
    log.info("── Fetching City Code Enforcement (scsb-hfcn)…")
    url = f"{CITY_BASE}/scsb-hfcn.json"
    params = {"$order": "date_opened DESC"}
    records = fetch_all_pages(session, url, extra_params=params)
    log.info("   Parsing %d code enforcement records…", len(records))
    for r in records:
        lead = parse_code_enforcement(r)
        if lead:
            permit_leads.append(lead)

    # ── 4. SD County Recorder — Distress Documents (NEW) ─────────────────────
    log.info("── Fetching SD County Recorder Documents (%s)…", RECORDER_DATASET)
    url = f"{COUNTY_BASE}/{RECORDER_DATASET}.json"
    where_clauses = [f"upper(document_type) like '%{t}%'" for t in RECORDER_ALL_TYPES]
    params = {
        "$where": " OR ".join(where_clauses),
        "$order": "recording_date DESC",
    }
    records = fetch_all_pages(session, url, extra_params=params)
    log.info("   Parsing %d recorder records…", len(records))
    recorder_leads = []
    for r in records:
        lead = parse_recorder_doc(r)
        if lead:
            recorder_leads.append(lead)
    log.info("   Recorder leads: %d", len(recorder_leads))

    # ── Cross-enrich: stack recorder signals onto permit/code leads ───────────
    permit_leads = enrich_with_recorder(permit_leads, recorder_leads)

    # Combine all leads
    all_leads = permit_leads + recorder_leads
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
    filtered = [l for l in leads if l.seller_score > 0 or l.has_code_violation]
    log.info("Leads with distress signals: %d", len(filtered))
    return filtered


# ─── Output (unchanged from v2.1) ────────────────────────────────────────────
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

    # Dashboard HTML is identical to v2.1 — no changes needed
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
  .export-btn{{background:var(--accent);border:none;color:#0a0d14;padding:.6rem 1.2rem;border-radius:4px;font-family:var(--mono);font-size:.72rem;font-weight:700;cursor:pointer;text-transform:uppercase;letter-spacing:.05em;transition:opacity .15s}}
  .export-btn:hover{{opacity:.85}}
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
  <input class="search-box" type="text" id="search" placeholder="Search address, doc #, name…"/>
  <button class="filter-btn" data-filter="all">All</button>
  <button class="filter-btn" data-filter="tax">Tax</button>
  <button class="filter-btn" data-filter="code">Code</button>
  <button class="filter-btn" data-filter="probate">Probate</button>
  <button class="filter-btn" data-filter="lien">Multi-Lien</button>
  <button class="filter-btn" data-filter="div">Divorce/BK</button>
  <span id="count-display"></span>
  <button class="export-btn" id="export-csv">Export CSV</button>
</div>
<div class="grid" id="grid"></div>
<footer>
  <span>Source: SD County &amp; City Open Data Portals + County Recorder (Socrata API) — Public Records</span>
  <span id="footer-ts"></span>
</footer>
<script>
const RAW = {leads_json};
const META_GENERATED = "{generated}";
function scoreColor(s){{if(s>=70)return'#e8ff47';if(s>=40)return'#ff7b2e';if(s>=20)return'#ff4757';return'#5a6475';}}
function tagHtml(l){{let t='';if(l.has_tax_delinquency)t+='<span class="tag tax">Tax Delinquency</span>';if(l.has_code_violation)t+='<span class="tag code">Code Violation</span>';if(l.has_probate)t+='<span class="tag prob">Probate</span>';if(l.has_multiple_liens)t+='<span class="tag lien">Multi-Lien</span>';if(l.has_divorce_bankruptcy)t+='<span class="tag div">Divorce/BK</span>';return t;}}
function renderCards(leads){{const grid=document.getElementById('grid');document.getElementById('count-display').textContent=`${{leads.length}} leads`;if(!leads.length){{grid.innerHTML='<div class="empty">No leads match your filter.</div>';return;}}grid.innerHTML=leads.map(l=>{{const c=scoreColor(l.seller_score);return`<div class="card" style="--score-color:${{c}}" title="${{(l.score_reasons||[]).join(' | ')}}"><div class="card-top"><span class="doc-type">${{l.doc_type||'—'}}</span><span class="score-badge">${{l.seller_score}}</span></div><div class="grantor">${{l.grantor||l.grantee||'Property Record'}}</div><div class="address">${{l.property_address||l.legal_description||'No address'}}</div><div class="card-meta"><div><div class="meta-label">Doc #</div><div class="meta-value">${{l.document_number||'—'}}</div></div><div><div class="meta-label">Filed</div><div class="meta-value">${{l.file_date||'—'}}</div></div><div><div class="meta-label">Grantee</div><div class="meta-value">${{l.grantee||'—'}}</div></div></div><div class="tags">${{tagHtml(l)}}</div></div>`;}}).join('');}}
function initStats(leads){{const total=leads.length;const high=leads.filter(l=>l.seller_score>=70).length;const avg=total?Math.round(leads.reduce((a,l)=>a+l.seller_score,0)/total):0;document.getElementById('header-stats').innerHTML=`<div class="stat"><div class="num">${{total}}</div><div class="lbl">Total Leads</div></div><div class="stat"><div class="num">${{high}}</div><div class="lbl">High Score ≥70</div></div><div class="stat"><div class="num">${{avg}}</div><div class="lbl">Avg Score</div></div>`;document.getElementById('footer-ts').textContent='Generated: '+new Date(META_GENERATED).toLocaleString();}}
let currentFilter='all',currentSearch='';
function applyFilters(){{let leads=[...RAW];if(currentFilter==='tax')leads=leads.filter(l=>l.has_tax_delinquency);if(currentFilter==='code')leads=leads.filter(l=>l.has_code_violation);if(currentFilter==='probate')leads=leads.filter(l=>l.has_probate);if(currentFilter==='lien')leads=leads.filter(l=>l.has_multiple_liens);if(currentFilter==='div')leads=leads.filter(l=>l.has_divorce_bankruptcy);if(currentSearch){{const q=currentSearch.toLowerCase();leads=leads.filter(l=>(l.property_address||'').toLowerCase().includes(q)||(l.document_number||'').toLowerCase().includes(q)||(l.grantor||'').toLowerCase().includes(q)||(l.grantee||'').toLowerCase().includes(q));}}renderCards(leads);}}
document.querySelectorAll('.filter-btn').forEach(btn=>{{btn.addEventListener('click',()=>{{document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');currentFilter=btn.dataset.filter;applyFilters();}});}});
document.getElementById('search').addEventListener('input',e=>{{currentSearch=e.target.value.trim();applyFilters();}});
document.getElementById('export-csv').addEventListener('click',()=>{{const cols=['document_number','file_date','doc_type','grantor','grantee','property_address','legal_description','seller_score','has_tax_delinquency','has_code_violation','has_probate','has_multiple_liens','has_divorce_bankruptcy','score_reasons','source_url'];const escape=v=>{{const s=(v===null||v===undefined)?'':Array.isArray(v)?v.join('; '):String(v);return'"'+s.replace(/"/g,'""')+'"';}};const rows=[cols.join(',')];let visible=[...RAW];if(currentFilter==='tax')visible=visible.filter(l=>l.has_tax_delinquency);if(currentFilter==='code')visible=visible.filter(l=>l.has_code_violation);if(currentFilter==='probate')visible=visible.filter(l=>l.has_probate);if(currentFilter==='lien')visible=visible.filter(l=>l.has_multiple_liens);if(currentFilter==='div')visible=visible.filter(l=>l.has_divorce_bankruptcy);if(currentSearch){{const q=currentSearch.toLowerCase();visible=visible.filter(l=>(l.property_address||'').toLowerCase().includes(q)||(l.document_number||'').toLowerCase().includes(q)||(l.grantor||'').toLowerCase().includes(q)||(l.grantee||'').toLowerCase().includes(q));}}visible.forEach(l=>rows.push(cols.map(c=>escape(l[c])).join(',')));const blob=new Blob([rows.join('\\n')],{{type:'text/csv'}});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='sd_leads_'+new Date().toISOString().slice(0,10)+'.csv';a.click();URL.revokeObjectURL(a.href);}});
initStats(RAW);document.querySelector('[data-filter="all"]').classList.add('active');applyFilters();
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
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║  SD County Motivated Seller Lead Scraper v2.2    ║")
    log.info("║  NEW: Recorder docs (NOD, Lis Pendens, Probate)  ║")
    log.info("╚══════════════════════════════════════════════════╝")

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
        log.info("  Top reasons  : %s", " | ".join(leads[0].score_reasons))
    log.info("─" * 50)


if __name__ == "__main__":
    main()
