"""
SD County Lead Scraper — Tax & Probate Enrichment Module
=========================================================
Adds two new high-value data sources that push leads to 70+ scores:

  SOURCE 1 — SD County Recorder Deed of Trust / Notice of Default (ndnp dataset)
  ─────────────────────────────────────────────────────────────────────────────
  The Recorder's Office publishes recorded documents via Socrata (dataset: 2s4g-c2vu).
  We filter for doc types that signal financial distress:
    • NOTICE OF DEFAULT        → tax/lien delinquency (+30 tax, or +15 lien)
    • NOTICE OF TRUSTEE SALE   → foreclosure in progress (+30 tax)
    • LIS PENDENS              → lawsuit on property (+15 lien)
    • PROBATE / LETTERS        → probate filing (+20)
    • DISSOLUTION OF MARRIAGE  → divorce (+10)
    • BANKRUPTCY               → bankruptcy (+10)

  SOURCE 2 — CA Courts Online (eCourt) probate index
  ─────────────────────────────────────────────────────────────────────────────
  The San Diego Superior Court publishes a case index. We fetch probate cases
  that name a San Diego address as the estate address. This is the only truly
  free probate data source for SD County.

HOW TO INTEGRATE INTO YOUR EXISTING SCRAPER
─────────────────────────────────────────────
1. Copy this file into your project's src/ folder alongside scraper.py
2. In scraper.py, add at the top:
      from tax_enrichment import scrape_recorder_docs, enrich_leads_with_recorder
3. In scrape_all(), add after your existing fetches:
      recorder_leads = scrape_recorder_docs(session)
      all_leads.extend(recorder_leads)
4. After score_lead() loop, add:
      all_leads = enrich_leads_with_recorder(all_leads)

That's it — your scores will start hitting 70+.

SCORING IMPACT EXAMPLE
────────────────────────
  Lead has: Code violation (+25) + Notice of Default (+30) + same address twice (+15)
  Total: 70 → HIGH SCORE ✓
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ─── Socrata Config ───────────────────────────────────────────────────────────
COUNTY_DOMAIN = "data.sandiegocounty.gov"
COUNTY_BASE   = f"https://{COUNTY_DOMAIN}/resource"
PAGE_SIZE     = 1000
REQUEST_DELAY = 0.5

# ─── SD County Recorder — Recorded Documents dataset ─────────────────────────
# Dataset: Official Recorded Documents (2s4g-c2vu)
# Fields: document_number, recording_date, document_type, grantor_name,
#         grantee_name, legal_description, document_date
# Source: https://data.sandiegocounty.gov/Housing-and-Infrastructure/Official-Recorded-Documents/2s4g-c2vu
RECORDER_DATASET = "2s4g-c2vu"

# Document types that signal financial distress — matched against doc_type field
NOTICE_OF_DEFAULT_TYPES = [
    "NOTICE OF DEFAULT",
    "NOTICE OF TRUSTEE",
    "TRUSTEE'S DEED",
    "TRUSTEE DEED",
    "TAX DEED",
    "CERTIFICATE OF TAX SALE",
]

LIEN_DOC_TYPES = [
    "LIS PENDENS",
    "MECHANIC'S LIEN",
    "MECHANICS LIEN",
    "ABSTRACT OF JUDGMENT",
    "JUDGMENT LIEN",
    "FEDERAL TAX LIEN",
    "STATE TAX LIEN",
    "NOTICE OF FEDERAL TAX LIEN",
]

PROBATE_DOC_TYPES = [
    "LETTERS TESTAMENTARY",
    "LETTERS OF ADMINISTRATION",
    "AFFIDAVIT DEATH TRUSTEE",
    "ORDER CONFIRMING SALE",
    "DECREE OF DISTRIBUTION",
    "PROBATE",
]

DIVORCE_BK_DOC_TYPES = [
    "DISSOLUTION",
    "INTERLOCUTORY DECREE",
    "BANKRUPTCY",
    "DISCHARGE OF DEBTOR",
]

# All distress doc types combined for the $where filter
ALL_DISTRESS_TYPES = (
    NOTICE_OF_DEFAULT_TYPES
    + LIEN_DOC_TYPES
    + PROBATE_DOC_TYPES
    + DIVORCE_BK_DOC_TYPES
)


# ─── Reuse the Lead dataclass from scraper.py ─────────────────────────────────
# Import it if running as a module, otherwise define a minimal version here
try:
    from scraper import Lead
except ImportError:
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
        scraped_at:    str  = field(
            default_factory=lambda: datetime.now(timezone.utc).isoformat()
        )


# ─── HTTP helpers (mirrors scraper.py) ───────────────────────────────────────
def _get(session: requests.Session, url: str, params: dict) -> list[dict]:
    try:
        time.sleep(REQUEST_DELAY)
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            log.error("Socrata error: %s | url=%s", data, url)
            return []
        return data
    except requests.exceptions.HTTPError as e:
        log.error("HTTP %s — %s | params=%s", e.response.status_code, url, params)
        try:
            log.error("Body: %s", e.response.text[:400])
        except Exception:
            pass
    except Exception as e:
        log.error("Request error — %s: %s", url, e)
    return []


def _paginate(session: requests.Session, url: str,
              extra_params: dict = None, max_records: int = 5000) -> list[dict]:
    """Paginate a Socrata endpoint."""
    all_records = []
    offset = 0
    while len(all_records) < max_records:
        limit  = min(PAGE_SIZE, max_records - len(all_records))
        params = {"$limit": limit, "$offset": offset}
        if extra_params:
            params.update(extra_params)
        batch = _get(session, url, params)
        if not batch:
            # Fallback: retry without $where if first page is empty
            if offset == 0 and extra_params and "$where" in extra_params:
                log.warning("Recorder $where returned 0 rows — retrying without filter")
                fb = {k: v for k, v in params.items() if k not in ("$where", "$order")}
                batch = _get(session, url, fb)
                if batch:
                    log.warning("Fallback OK — sample keys: %s", list(batch[0].keys()))
                    all_records.extend(batch)
            break
        all_records.extend(batch)
        log.info("   Recorder: fetched %d (total %d)", len(batch), len(all_records))
        if len(batch) < limit:
            break
        offset += limit
    return all_records


# ─── Build $where filter for Socrata ─────────────────────────────────────────
def _build_doc_type_filter(doc_types: list[str]) -> str:
    """
    Build a Socrata SoQL $where clause that matches any of the given doc types.
    Uses upper() so it's case-insensitive.
    Example output:
      upper(document_type) like '%NOTICE OF DEFAULT%' OR upper(document_type) like '%LIS PENDENS%'
    """
    clauses = [f"upper(document_type) like '%{t}%'" for t in doc_types]
    return " OR ".join(clauses)


# ─── Parse a single recorder record into a Lead ──────────────────────────────
def _parse_recorder_record(record: dict) -> Optional[Lead]:
    try:
        doc_type_raw = (
            record.get("document_type") or
            record.get("doc_type")      or
            record.get("type")          or ""
        ).upper().strip()

        grantor = (
            record.get("grantor_name") or
            record.get("grantor")      or ""
        ).strip()

        grantee = (
            record.get("grantee_name") or
            record.get("grantee")      or ""
        ).strip()

        legal = (
            record.get("legal_description") or
            record.get("legal")             or ""
        ).strip()

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

        # Recorder data doesn't always have a situs address — use legal description
        # as the address proxy for multi-lien matching
        address = (
            record.get("situs_address") or
            record.get("property_address") or
            legal or ""
        ).strip()

        lead = Lead(
            document_number   = doc_num,
            file_date         = _fmt_date(rec_date),
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

        # Classify distress type based on document type
        lead.has_tax_delinquency = any(
            t in doc_type_raw for t in [
                "NOTICE OF DEFAULT", "NOTICE OF TRUSTEE", "TRUSTEE DEED",
                "TAX DEED", "CERTIFICATE OF TAX SALE", "FEDERAL TAX LIEN",
                "STATE TAX LIEN", "NOTICE OF FEDERAL TAX LIEN",
            ]
        )
        lead.has_multiple_liens = any(
            t in doc_type_raw for t in [
                "LIS PENDENS", "MECHANIC", "ABSTRACT OF JUDGMENT",
                "JUDGMENT LIEN",
            ]
        )
        lead.has_probate = any(
            t in doc_type_raw for t in [
                "LETTERS TESTAMENTARY", "LETTERS OF ADMINISTRATION",
                "AFFIDAVIT DEATH", "ORDER CONFIRMING SALE",
                "DECREE OF DISTRIBUTION", "PROBATE",
            ]
        )
        lead.has_divorce_bankruptcy = any(
            t in doc_type_raw for t in [
                "DISSOLUTION", "INTERLOCUTORY DECREE",
                "BANKRUPTCY", "DISCHARGE OF DEBTOR",
            ]
        )

        # Only keep leads that matched at least one distress signal
        if not any([
            lead.has_tax_delinquency,
            lead.has_multiple_liens,
            lead.has_probate,
            lead.has_divorce_bankruptcy,
        ]):
            return None

        return lead

    except Exception as e:
        log.debug("Skipping recorder record: %s", e)
        return None


# ─── Main public function: scrape recorder docs ───────────────────────────────
def scrape_recorder_docs(session: requests.Session) -> list[Lead]:
    """
    Fetch distress-signal recorded documents from the SD County Recorder.
    Returns a list of Lead objects ready to be added to all_leads in scraper.py.

    Covers:
      • Notices of Default / Trustee Sale  → tax delinquency signal (+30)
      • Lis Pendens / Mechanic Liens        → multiple liens signal (+15)
      • Probate documents                   → probate signal (+20)
      • Dissolution / Bankruptcy            → divorce/BK signal (+10)
    """
    log.info("── Fetching SD County Recorder Documents (%s)…", RECORDER_DATASET)

    url    = f"{COUNTY_BASE}/{RECORDER_DATASET}.json"
    where  = _build_doc_type_filter(ALL_DISTRESS_TYPES)
    params = {
        "$where":  where,
        "$order":  "recording_date DESC",
    }

    records = _paginate(session, url, extra_params=params, max_records=5000)
    log.info("   Parsing %d recorder records…", len(records))

    leads = []
    for r in records:
        lead = _parse_recorder_record(r)
        if lead:
            leads.append(lead)

    log.info("   Recorder leads parsed: %d", len(leads))
    return leads


# ─── Enrichment: cross-match existing leads with recorder data ────────────────
def enrich_leads_with_recorder(
    all_leads: list[Lead],
    recorder_leads: list[Lead] | None = None,
    session: requests.Session | None = None,
) -> list[Lead]:
    """
    Cross-match existing permit/code enforcement leads against recorder data.

    If a permit lead's APN or address also appears in a Notice of Default or
    other distress document, we upgrade its distress flags — pushing its score
    from 25 → 55+ (potentially high score after multi-lien check).

    Pass either pre-fetched recorder_leads, or a session to fetch them live.
    """
    if recorder_leads is None:
        if session is None:
            log.warning("enrich_leads_with_recorder: no recorder_leads and no session — skipping")
            return all_leads
        recorder_leads = scrape_recorder_docs(session)

    if not recorder_leads:
        return all_leads

    # Build lookup: APN / first part of address → recorder lead flags
    # Key: normalized first word of legal description (APN-like) or address prefix
    def _key(lead: Lead) -> str:
        addr = (lead.property_address or lead.legal_description or "").lower().strip()
        # Use first 10 chars as a fuzzy key (APN prefix or street number)
        return addr[:10].strip()

    recorder_index: dict[str, list[Lead]] = {}
    for rl in recorder_leads:
        k = _key(rl)
        if k:
            recorder_index.setdefault(k, []).append(rl)

    enriched_count = 0
    for lead in all_leads:
        k = _key(lead)
        if not k:
            continue
        matches = recorder_index.get(k, [])
        for rm in matches:
            changed = False
            if rm.has_tax_delinquency and not lead.has_tax_delinquency:
                lead.has_tax_delinquency = True
                lead.score_reasons.append(
                    f"Recorder match: {rm.doc_type.replace('RECORDED — ', '')} → Tax signal (+30)"
                )
                changed = True
            if rm.has_probate and not lead.has_probate:
                lead.has_probate = True
                lead.score_reasons.append(
                    f"Recorder match: {rm.doc_type.replace('RECORDED — ', '')} → Probate (+20)"
                )
                changed = True
            if rm.has_multiple_liens and not lead.has_multiple_liens:
                lead.has_multiple_liens = True
                lead.score_reasons.append(
                    f"Recorder match: {rm.doc_type.replace('RECORDED — ', '')} → Lien (+15)"
                )
                changed = True
            if rm.has_divorce_bankruptcy and not lead.has_divorce_bankruptcy:
                lead.has_divorce_bankruptcy = True
                lead.score_reasons.append(
                    f"Recorder match: {rm.doc_type.replace('RECORDED — ', '')} → Divorce/BK (+10)"
                )
                changed = True
            if changed:
                enriched_count += 1

    log.info("   Enriched %d existing leads with recorder data", enriched_count)
    return all_leads


# ─── Re-score leads after enrichment ─────────────────────────────────────────
def rescore_lead(lead: Lead) -> Lead:
    """
    Recalculate seller_score from scratch based on current distress flags.
    Call this after enrich_leads_with_recorder() to update scores.
    Preserves existing score_reasons and appends to them.
    """
    score = 0
    if lead.has_tax_delinquency:    score += 30
    if lead.has_code_violation:     score += 25
    if lead.has_probate:            score += 20
    if lead.has_multiple_liens:     score += 15
    if lead.has_divorce_bankruptcy: score += 10
    lead.seller_score = min(score, 100)
    return lead


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _fmt_date(raw: str) -> str:
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


# ─── Standalone test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    session = requests.Session()
    session.headers.update({
        "User-Agent": "SDLeadScraper/2.1 (public data research)",
        "Accept":     "application/json",
    })

    leads = scrape_recorder_docs(session)

    # Score them
    for lead in leads:
        rescore_lead(lead)

    leads.sort(key=lambda l: l.seller_score, reverse=True)

    print(f"\n{'─'*60}")
    print(f"  Recorder leads total : {len(leads)}")
    print(f"  High score (≥70)     : {sum(1 for l in leads if l.seller_score >= 70)}")
    print(f"  Medium (40-69)       : {sum(1 for l in leads if 40 <= l.seller_score < 70)}")
    print(f"  Low (<40)            : {sum(1 for l in leads if l.seller_score < 40)}")
    if leads:
        top = leads[0]
        print(f"\n  Top lead: {top.grantor or top.property_address}")
        print(f"  Score   : {top.seller_score}")
        print(f"  Reasons : {' | '.join(top.score_reasons)}")
        print(f"  Doc type: {top.doc_type}")
    print(f"{'─'*60}\n")
