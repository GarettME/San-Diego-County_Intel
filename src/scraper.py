"""
San Diego County Motivated Seller Lead Scraper  v3.0
=====================================================
COMPLETE REWRITE — data source changed.

WHY:
  v2.x relied on Socrata Open Data APIs that are all dead or frozen:
    - data.sandiego.gov (City permits + code enforcement) migrated OFF
      Socrata to a static S3/CKAN portal → every /resource/<id>.json 404s.
    - County permit datasets (gs2m-invt, dyzh-7eat) are frozen historical
      sets (last updated 2012 and 2023-12-05 respectively).
    - The County Recorder Socrata dataset (2s4g-c2vu) never existed.
  The old scraper "succeeded" only because it swallowed the errors and
  re-committed the same fallback 523 rows every run.

NEW SOURCE (mirrors the working Ventura County scraper's approach):
  San Diego County Assessor-Recorder-County Clerk "Official Records" search,
  an AcclaimWeb portal (arcc-acclaim.sdcounty.ca.gov), updated DAILY.
  We drive it with Playwright, searching recorded distress documents
  (Notice of Default, Lis Pendens, tax/judgment liens, probate, etc.) over
  a rolling date window — so leads are genuinely fresh each day.

NOTE ON ACCESS:
  The SD portal is behind Akamai bot protection that blocks datacenter IPs
  (incl. GitHub Actions runners). Set PROXY_SERVER/PROXY_USERNAME/
  PROXY_PASSWORD (a residential proxy) so the CI run reaches it. The
  scraper FAILS LOUDLY (exit 1) if it is blocked or the form never loads,
  so it never silently re-commits stale data again.

PORTABILITY / TESTING:
  ACCLAIM_BASE selects the portal. It defaults to San Diego but can point at
  any AcclaimWeb instance (they share identical DOM), which is how this is
  tested end-to-end against a reachable county before deploying for SD.

Output schema is unchanged from v2.x, so docs/index.html keeps working.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG", "").lower() == "true" else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ─── Portal config ────────────────────────────────────────────────────────────
# Default: San Diego County ARCC Official Records (AcclaimWeb).
# NOTE: SD serves the landing page at /AcclaimWeb but the search app itself
# lives at the domain ROOT (e.g. /search/SearchTypeDocType). Prepending
# /AcclaimWeb to the search path 404s, so the base must be the bare origin.
ACCLAIM_BASE = os.getenv("ACCLAIM_BASE", "https://arcc-acclaim.sdcounty.ca.gov").rstrip("/")
SOURCE_NAME  = os.getenv("SOURCE_NAME", "San Diego County Assessor-Recorder-County Clerk")

DOCTYPE_SEARCH_URL = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
DISCLAIMER_PATH    = "/Search/Disclaimer"

# 7-day rolling window. The portal returns rows OLDEST-first and paginates ~11
# rows/page, so MAX_PAGES must be high enough to walk the entire window —
# otherwise the cap silently drops the FRESHEST leads (the whole point of the
# tool). A busy week is ~900 rows / ~85 real pages; 300 leaves generous slack.
# We then sort newest-first in main(). Override either via env if needed.
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "300"))
NAV_TIMEOUT   = int(os.getenv("NAV_TIMEOUT_MS", "45000"))

# Proxy (residential) — only used if PROXY_SERVER is set.
PROXY_SERVER   = os.getenv("PROXY_SERVER", "").strip()
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "").strip()

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR       = PROJECT_ROOT / "data"
DASHBOARD_DIR  = PROJECT_ROOT / "docs"
OUTPUT_JSON    = DATA_DIR / "output.json"
DASHBOARD_HTML = DASHBOARD_DIR / "index.html"

# ─── Distress document types ──────────────────────────────────────────────────
# Each entry: substring to look for in the AcclaimWeb checkbox `title`
# (e.g. "NOTICE OF DEFAULT - ...") → (distress flag, human category).
# "flag" maps onto the Lead boolean fields used for scoring + the dashboard.
DISTRESS_DOCTYPES: list[tuple[str, str, str]] = [
    # keyword (uppercase substring)      flag                      label
    ("NOTICE OF DEFAULT",                "tax",     "Notice of Default"),
    ("NOTICE OF TRUSTEE",                "tax",     "Notice of Trustee Sale"),
    ("TRUSTEE SALE",                     "tax",     "Trustee Sale"),
    ("TRUSTEE'S SALE",                   "tax",     "Trustee Sale"),
    ("TRUSTEE DEED",                     "tax",     "Trustee's Deed"),
    ("TAX DEED",                         "tax",     "Tax Deed"),
    ("TAX LIEN",                         "tax",     "Tax Lien"),
    ("LIS PENDENS",                      "lien",    "Lis Pendens"),
    ("MECHANIC",                         "lien",    "Mechanic's Lien"),
    ("ABSTRACT OF JUDGMENT",             "lien",    "Abstract of Judgment"),
    ("JUDGMENT LIEN",                    "lien",    "Judgment Lien"),
    ("LIEN",                             "lien",    "Lien"),
    ("PROBATE",                          "probate", "Probate"),
    ("LETTERS TESTAMENTARY",             "probate", "Letters Testamentary"),
    ("LETTERS OF ADMINISTRATION",        "probate", "Letters of Administration"),
    ("AFFIDAVIT DEATH",                  "probate", "Affidavit - Death"),
    ("DECREE OF DISTRIBUTION",           "probate", "Decree of Distribution"),
    ("DISSOLUTION",                      "divorce", "Dissolution of Marriage"),
    ("BANKRUPTCY",                       "divorce", "Bankruptcy"),
]

# Flag → which Lead boolean it sets
FLAG_TO_FIELD = {
    "tax":     "has_tax_delinquency",
    "lien":    "has_multiple_liens",
    "probate": "has_probate",
    "divorce": "has_divorce_bankruptcy",
}

# Skip the "resolution" variants of distress docs — releases, terminations,
# satisfactions, assignments and corrections are NOT motivated-seller signals.
EXCLUDE_DOCTYPE_KEYWORDS = [
    "RELEASE", "TERMINATION", "SATISFACTION", "DISCHARGE", "ASSIGNMENT",
    "CORRECTION", "MODIFICATION", "AMENDMENT", "RESCISSION", "WITHDRAWAL",
    "CANCELLATION", "SUBORDINATION", "REVOCATION", "EXPUNGE",
]

# Built at runtime from the live portal: grid document-type CODE → (flag, label)
# e.g. "FTL" → ("tax", "Federal Tax Lien"), "LIS" → ("lien", "Lis Pendens").
# Filled in by _select_distress_doctypes(); used by _apply_distress_flags().
DOCTYPE_CODE_MAP: dict[str, tuple[str, str]] = {}


def _classify_title(title: str) -> Optional[tuple[str, str, str]]:
    """
    Given an AcclaimWeb doc-type `title` ("CODE - FULL DESCRIPTION"), decide
    whether it's a distress lead type we want. Returns (code, flag, label) or
    None if it should be skipped.
    """
    t = title.upper().strip()
    if any(x in t for x in EXCLUDE_DOCTYPE_KEYWORDS):
        return None
    for keyword, flag, label in DISTRESS_DOCTYPES:
        if keyword in t:
            code = t.split(" - ", 1)[0].strip() if " - " in t else t
            return code, flag, label
    return None


# ─── Data Model (schema unchanged → dashboard stays compatible) ───────────────
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


# ════════════════════════════════════════════════════════════════════════════
# ACCLAIMWEB SCRAPER
# ════════════════════════════════════════════════════════════════════════════

class PortalBlockedError(RuntimeError):
    """Raised when the portal denies access (e.g. Akamai 403) — fail loudly."""


async def scrape_recorder(date_from: str, date_to: str) -> list[Lead]:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    leads: list[Lead] = []

    launch_kwargs: dict = {
        "headless": True,
        "args": ["--no-sandbox", "--disable-dev-shm-usage"],
    }
    # Use the system Chromium if Playwright's own download is unavailable.
    for exe in ("/usr/lib/chromium/chromium", "/usr/bin/chromium", "/usr/bin/chromium-browser"):
        if os.getenv("CHROMIUM_PATH"):
            launch_kwargs["executable_path"] = os.getenv("CHROMIUM_PATH"); break
        if os.path.exists(exe):
            launch_kwargs["executable_path"] = exe; break

    if PROXY_SERVER:
        proxy: dict = {"server": PROXY_SERVER}
        if PROXY_USERNAME:
            proxy["username"] = PROXY_USERNAME
            proxy["password"] = PROXY_PASSWORD
        launch_kwargs["proxy"] = proxy
        log.info("Using residential proxy: %s", PROXY_SERVER)
    else:
        log.warning("No PROXY_SERVER set — the SD portal will likely 403 from a datacenter IP.")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(user_agent=CHROME_UA,
                                             viewport={"width": 1366, "height": 900})
        page = await context.new_page()
        page.set_default_timeout(NAV_TIMEOUT)

        # ── Load the Document-Type search page (may show the disclaimer) ──────
        log.info("Opening %s", DOCTYPE_SEARCH_URL)
        try:
            resp = await page.goto(DOCTYPE_SEARCH_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except PWTimeout as e:
            raise PortalBlockedError(f"Timed out loading portal: {e}")

        if resp is not None and resp.status in (401, 403, 407):
            raise PortalBlockedError(f"Portal returned HTTP {resp.status} (bot/IP block). "
                                     f"A residential proxy is required.")
        body = (await page.content()).lower()
        if "access denied" in body or "akamai" in body or "edgesuite" in body:
            raise PortalBlockedError("Portal returned an Akamai 'Access Denied' page. "
                                     "A residential proxy is required.")

        # ── Accept disclaimer if present ──────────────────────────────────────
        await _accept_disclaimer(page)

        # ── Wait for the search form ──────────────────────────────────────────
        try:
            await page.wait_for_selector("#RecordDateFrom", timeout=NAV_TIMEOUT)
        except PWTimeout:
            raise PortalBlockedError("Search form (#RecordDateFrom) never appeared — "
                                     "portal blocked or markup changed.")
        log.info("Search form loaded.")

        # ── Select distress document types ────────────────────────────────────
        code_map = await _select_distress_doctypes(page)
        if not code_map:
            log.warning("No distress doc types matched the portal's list — "
                        "the search will run across all types and filter afterward.")

        # ── Fill the recording-date range ─────────────────────────────────────
        await _fill_dates(page, date_from, date_to)

        # ── Submit ────────────────────────────────────────────────────────────
        log.info("Submitting search for %s → %s …", date_from, date_to)
        await page.click("#btnSearch")
        await _wait_for_results(page)

        # ── Parse all result pages ────────────────────────────────────────────
        leads = await _parse_all_pages(page)
        await browser.close()

    return leads


async def _accept_disclaimer(page) -> None:
    """The AcclaimWeb disclaimer is a form whose submit button is #btnButton."""
    try:
        btn = page.locator("#btnButton")
        if await btn.is_visible(timeout=4000):
            await btn.click()
            await page.wait_for_load_state("domcontentloaded")
            log.info("Disclaimer accepted.")
    except Exception:
        # Some instances skip the disclaimer once a session cookie exists.
        log.debug("No disclaimer button shown — continuing.")


async def _select_distress_doctypes(page) -> dict[str, tuple[str, str]]:
    """
    Open the doc-type picker, check every checkbox whose `title` is a distress
    lead type, then commit via the portal's own GetDocTypeString() (the commit
    handler behind the "Doc Type List" tab's Done button — note this is NOT
    GetDocTypeStringFromGroup(), which belongs to the *category* tab).

    Populates and returns DOCTYPE_CODE_MAP: grid CODE → (flag, label).

    AcclaimWeb renders each type as:
      <input name="DocTypeInfoCheckBox" title="FTL - FEDERAL TAX LIEN" value="62" type="checkbox">
    """
    # Open the picker (a Telerik window).
    for opener in ("#DocTypesDisplay-input", "#DocTypesDisplay", "#DocTypesWin"):
        try:
            el = page.locator(opener).first
            if await el.is_visible(timeout=2500):
                await el.click()
                break
        except Exception:
            continue

    # Switch to the "Doc Type List" tab so the checkboxes are present.
    for tab in ("a[href='#DocumentTypesList-2']", "li:has-text('Doc Type List') a"):
        try:
            t = page.locator(tab).first
            if await t.is_visible(timeout=2000):
                await t.click()
                break
        except Exception:
            continue
    await asyncio.sleep(0.8)

    # Read every available type title up-front, decide which to keep here in
    # Python (so exclusions + classification live in one place), then check
    # exactly those values in the DOM.
    titles = await page.evaluate(
        """() => Array.from(document.querySelectorAll("input[name='DocTypeInfoCheckBox']"))
                     .map(b => ({value: b.value, title: (b.getAttribute('title') || '')}))"""
    )

    DOCTYPE_CODE_MAP.clear()
    wanted_values: list[str] = []
    for item in titles:
        classified = _classify_title(item["title"])
        if classified:
            code, flag, label = classified
            wanted_values.append(item["value"])
            DOCTYPE_CODE_MAP[code.upper()] = (flag, label)

    if not wanted_values:
        return DOCTYPE_CODE_MAP

    # Tick the chosen checkboxes, then commit with the portal's own function.
    await page.evaluate(
        """(values) => {
            const want = new Set(values);
            document.querySelectorAll("input[name='DocTypeInfoCheckBox']").forEach(b => {
                if (want.has(b.value)) b.checked = true;
            });
            if (typeof GetDocTypeString === 'function') GetDocTypeString();
        }""",
        wanted_values,
    )
    await asyncio.sleep(0.3)

    # Verify the hidden field actually populated; if not, fall back to the
    # visible Done button (covers markup variants on other AcclaimWeb versions).
    hidden = await page.evaluate("() => (document.querySelector('#DocTypes') || {}).value || ''")
    if not hidden or hidden == "undefined":
        for done in ("input[onclick*='GetDocTypeString']", "input[value='Done']"):
            try:
                d = page.locator(done).first
                if await d.is_visible(timeout=1500):
                    await d.click()
                    break
            except Exception:
                continue

    log.info("Selected %d distress document types (%d distinct codes).",
             len(wanted_values), len(DOCTYPE_CODE_MAP))
    log.debug("   codes: %s", sorted(DOCTYPE_CODE_MAP.keys()))
    return DOCTYPE_CODE_MAP


async def _fill_dates(page, date_from: str, date_to: str) -> None:
    for sel, val in (("#RecordDateFrom", date_from), ("#RecordDateTo", date_to)):
        try:
            box = page.locator(sel)
            await box.click()
            await box.fill("")
            await box.type(val, delay=20)
            await page.keyboard.press("Tab")
            await asyncio.sleep(0.2)
        except Exception as e:
            log.warning("Could not fill %s: %s", sel, e)


async def _wait_for_results(page) -> bool:
    """
    Poll until the grid actually contains DATA rows (a cell holding a mm/dd/yyyy
    record date) — the Telerik grid loads asynchronously, and a naive wait on
    any <td> matches the filter-control row before data arrives. Returns True if
    data rows appeared, False on a confirmed empty result or timeout.
    """
    start = asyncio.get_event_loop().time()
    deadline = start + (NAV_TIMEOUT / 1000.0)
    # Don't honor an "empty" signal until this grace period passes — the grid
    # transiently shows a 0-row skeleton while the AJAX results load (~6s).
    EMPTY_GRACE_S = 7.0
    while asyncio.get_event_loop().time() < deadline:
        state = await page.evaluate(
            r"""() => {
                const g = document.querySelector('#SearchGridDiv');
                if (!g) return {rows: 0, empty: false};
                // Telerik splits header + data into separate tables; the data
                // lives in .t-grid-content (fall back to the densest tbody).
                let content = g.querySelector('.t-grid-content');
                let rows = 0;
                if (content) {
                    rows = [...content.querySelectorAll('tbody tr')]
                             .filter(r => r.querySelector('td')).length;
                } else {
                    for (const tb of g.querySelectorAll('tbody')) {
                        rows = Math.max(rows, [...tb.querySelectorAll('tr')]
                                 .filter(r => r.querySelector('td')).length);
                    }
                }
                // Pager reads "Displaying items 1 - 11 of 6653"; trust its total.
                const m = (g.innerText || '').match(/of\s+([\d,]+)/i);
                const total = m ? parseInt(m[1].replace(/,/g, ''), 10) : null;
                const txt = (g.innerText || '').toLowerCase();
                const empty = total === 0 ||
                              txt.includes('no records to display') ||
                              txt.includes('no items to display') ||
                              txt.includes('no records found');
                return {rows, empty};
            }"""
        )
        if state["rows"] > 0:
            await asyncio.sleep(0.8)  # let the rest of the page settle
            return True
        if state["empty"] and (asyncio.get_event_loop().time() - start) >= EMPTY_GRACE_S:
            return False
        await asyncio.sleep(0.5)
    log.info("Timed out waiting for result rows.")
    return False


def _row_key(lead: Lead) -> str:
    """Stable identity for a result row, for end-of-results detection."""
    dn = (lead.document_number or "").strip().upper()
    return dn or (f"{lead.file_date}|{lead.doc_type}|{lead.grantor}|"
                  f"{lead.legal_description}").upper()


async def _parse_all_pages(page) -> list[Lead]:
    leads: list[Lead] = []
    seen_keys: set[str] = set()
    seen_pages = 0
    while seen_pages < MAX_PAGES:
        html = await page.content()
        page_leads = _parse_grid(html)
        seen_pages += 1

        new_leads = [l for l in page_leads if _row_key(l) not in seen_keys]
        seen_keys.update(_row_key(l) for l in new_leads)
        log.info("Page %d: %d rows (%d new, running total %d)",
                 seen_pages, len(page_leads), len(new_leads), len(leads) + len(new_leads))

        # An empty page means we're past the last page of results — stop.
        if not page_leads:
            if seen_pages == 1:
                log.info("Result grid present but no data rows.")
            break
        leads.extend(new_leads)

        # End-of-results: AcclaimWeb's pager often leaves the next-arrow enabled
        # past the final page and re-serves a short tail of already-seen rows.
        # Once a non-empty page contributes nothing new we've reached the end —
        # stop here instead of clicking through hundreds of duplicate pages.
        if not new_leads:
            log.info("Page %d added no new rows — end of results, stopping.", seen_pages)
            break

        # Advance to the next page. The next-arrow carries t-state-disabled on
        # the last page; treat "disabled or absent" as the end.
        moved = False
        for nxt in (
            "#SearchGridDiv .t-arrow-next:not(.t-state-disabled)",
            "#SearchGridDiv a[title='Go to the next page']:not(.t-state-disabled)",
            ".t-grid-pager .t-arrow-next:not(.t-state-disabled)",
        ):
            try:
                btn = page.locator(nxt).first
                if await btn.is_visible(timeout=1200):
                    await btn.click()
                    await _wait_for_results(page)
                    moved = True
                    break
            except Exception:
                continue
        if not moved:
            break
    return leads


def _parse_grid(html: str) -> list[Lead]:
    """
    Parse the AcclaimWeb result table inside #SearchGridDiv. Columns are mapped
    by HEADER LABEL (resilient to per-county column-order differences):
      FIRST NAME / GRANTOR → grantor   GRANTEE → grantee
      DOC LEGAL / LEGAL    → legal      RECORD DATE / DATE → file_date
      DOCUMENT TYPE        → doc_type   FEE NUMBER / INSTRUMENT / DOC # → document_number
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    grid = soup.select_one("#SearchGridDiv")
    if grid is None:
        return []

    # Headers come from whichever table actually has <th> cells (the Telerik
    # header table); data rows come from .t-grid-content (a separate table).
    headers: list[str] = []
    for t in grid.find_all("table"):
        ths = t.find_all("th")
        if ths:
            headers = [th.get_text(" ", strip=True).upper() for th in ths]
            break
    if not headers:
        return []

    content = grid.select_one(".t-grid-content")
    if content is not None:
        data_rows = content.select("tbody tr")
    else:
        # Fallback: the tbody with the most rows is the data table.
        bodies = grid.find_all("tbody")
        data_rows = max((tb.find_all("tr") for tb in bodies), key=len, default=[])

    def col(*names) -> Optional[int]:
        for i, h in enumerate(headers):
            if any(n in h for n in names):
                return i
        return None

    idx_name    = col("GRANTOR", "FIRST NAME", "NAME")
    idx_grantee = col("GRANTEE", "SECOND NAME")
    idx_legal   = col("LEGAL")
    idx_date    = col("RECORD DATE", "DATE")
    idx_type    = col("DOCUMENT TYPE", "DOC TYPE")
    idx_docnum  = col("FEE NUMBER", "INSTRUMENT", "DOC NUMBER", "DOCUMENT NUMBER", "RECORDING")

    leads: list[Lead] = []
    for tr in data_rows:
        cells = tr.find_all("td")
        if not cells:
            continue
        vals = [c.get_text(" ", strip=True) for c in cells]

        def get(i):
            return vals[i] if (i is not None and i < len(vals)) else ""

        grantor  = get(idx_name)
        doc_type = get(idx_type)
        docnum   = get(idx_docnum)
        if not (grantor or docnum):
            continue  # skip filler / empty rows

        lead = Lead(
            document_number   = docnum,
            file_date         = _fmt_date(get(idx_date)),
            doc_type          = doc_type.upper(),
            grantor           = grantor,
            grantee           = get(idx_grantee),
            legal_description = get(idx_legal),
            property_address  = "",
            source_url        = DOCTYPE_SEARCH_URL,
        )
        _apply_distress_flags(lead)
        leads.append(lead)
    return leads


def _apply_distress_flags(lead: Lead) -> None:
    """
    Set distress booleans from the document type. The grid shows the short CODE
    (e.g. "FTL", "LIS"), so first look the code up in DOCTYPE_CODE_MAP (built
    from the live portal); fall back to full-name keyword matching.
    """
    dt = (lead.doc_type or "").upper().strip()

    mapped = DOCTYPE_CODE_MAP.get(dt) or DOCTYPE_CODE_MAP.get(dt.split()[0] if dt else "")
    if mapped:
        flag, label = mapped
        setattr(lead, FLAG_TO_FIELD[flag], True)
        lead.doc_type = label.upper()  # expand cryptic code → readable label
        return

    for keyword, flag, _label in DISTRESS_DOCTYPES:
        if keyword in dt:
            setattr(lead, FLAG_TO_FIELD[flag], True)


# ════════════════════════════════════════════════════════════════════════════
# SCORING / DEDUP / FILTER
# ════════════════════════════════════════════════════════════════════════════

# Per-document-type base weights, most→least motivated from a seller's angle.
# The FIRST substring found in lead.doc_type wins, so strong/specific signals
# are listed before generic ones. Foreclosure (Notice of Default / trustee's
# sale) and probate (heirs who typically want a fast sale) deliberately outrank
# plain tax liens — those are common and a much weaker direct sell signal, and
# many are business/income liens unrelated to the property at all.
DOCTYPE_SCORE_WEIGHTS: list[tuple[str, int, str]] = [
    ("NOTICE OF TRUSTEE",         55, "Notice of Trustee's Sale — imminent foreclosure (+55)"),
    ("TRUSTEE'S SALE",            55, "Trustee's Sale — imminent foreclosure (+55)"),
    ("TRUSTEE SALE",              55, "Trustee Sale — imminent foreclosure (+55)"),
    ("NOTICE OF DEFAULT",         50, "Notice of Default — pre-foreclosure (+50)"),
    ("LETTERS TESTAMENTARY",      40, "Probate — Letters Testamentary (+40)"),
    ("LETTERS OF ADMINISTRATION", 40, "Probate — Letters of Administration (+40)"),
    ("DECREE OF DISTRIBUTION",    40, "Probate — Decree of Distribution (+40)"),
    ("AFFIDAVIT DEATH",           40, "Affidavit of Death — likely inherited property (+40)"),
    ("AFFIDAVIT - DEATH",         40, "Affidavit of Death — likely inherited property (+40)"),
    ("PROBATE",                   40, "Probate filing — heirs likely to sell (+40)"),
    ("LIS PENDENS",               30, "Lis Pendens — pending litigation (+30)"),
    ("BANKRUPTCY",                25, "Bankruptcy (+25)"),
    ("DISSOLUTION",               20, "Divorce / dissolution of marriage (+20)"),
    ("TRUSTEE'S DEED",            20, "Trustee's Deed (+20)"),
    ("TRUSTEE DEED",              20, "Trustee's Deed (+20)"),
    ("TAX DEED",                  20, "Tax Deed (+20)"),
    ("ABSTRACT OF JUDGMENT",      18, "Abstract of Judgment (+18)"),
    ("JUDGMENT LIEN",             18, "Judgment Lien (+18)"),
    ("MECHANIC",                  15, "Mechanic's Lien (+15)"),
    ("TAX LIEN",                  12, "Tax lien — financial distress (+12)"),
    ("LIEN",                      10, "Lien (+10)"),
]

# Used only if the doc-type label matches none of the weighted keywords above,
# so a flagged lead never silently scores zero.
_FLAG_FALLBACK_SCORE: list[tuple[str, int]] = [
    ("has_probate",            40),
    ("has_divorce_bankruptcy", 20),
    ("has_tax_delinquency",    12),
    ("has_multiple_liens",     10),
]


def _doctype_base_score(lead: Lead) -> tuple[int, Optional[str]]:
    """Base score from the document type itself (the strongest single signal)."""
    dt = (lead.doc_type or "").upper()
    for keyword, pts, reason in DOCTYPE_SCORE_WEIGHTS:
        if keyword in dt:
            return pts, reason
    for field_name, pts in _FLAG_FALLBACK_SCORE:
        if getattr(lead, field_name):
            label = field_name.replace("has_", "").replace("_", " ")
            return pts, f"Distress signal: {label} (+{pts})"
    return 0, None


def score_lead(lead: Lead, all_leads: list) -> Lead:
    score = 0
    reasons = list(lead.score_reasons)

    base, reason = _doctype_base_score(lead)
    if base:
        score += base
        if reason:
            reasons.append(reason)

    if lead.has_code_violation:
        score += 25; reasons.append("Code violation (+25)")

    # Same grantor appearing under multiple distress docs → stacked distress,
    # a strong motivated-seller signal that can push a foreclosure into "High".
    if lead.grantor:
        key = lead.grantor.lower().strip()
        same = [l for l in all_leads if l is not lead and l.grantor.lower().strip() == key]
        if same:
            lead.has_multiple_liens = True
            score += 20
            reasons.append(f"Multiple distress records, same party ({len(same)+1}, +20)")

    lead.seller_score = min(score, 100)
    lead.score_reasons = reasons
    return lead


def deduplicate(leads: list[Lead]) -> list[Lead]:
    seen: set[str] = set()
    unique = []
    for lead in leads:
        key = (lead.document_number or f"{lead.grantor}:{lead.file_date}:{lead.doc_type}").strip().lower()
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    log.info("After dedup: %d unique leads", len(unique))
    return unique


def filter_has_distress(leads: list[Lead]) -> list[Lead]:
    filtered = [l for l in leads if l.seller_score > 0]
    log.info("Leads with distress signals: %d", len(filtered))
    return filtered


# ════════════════════════════════════════════════════════════════════════════
# OUTPUT  (unchanged schema → docs/index.html dashboard stays compatible)
# ════════════════════════════════════════════════════════════════════════════

def save_json(leads: list[Lead]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source":       SOURCE_NAME,
        "total_leads":  len(leads),
        "leads":        [asdict(l) for l in leads],
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    log.info("Saved → %s (%d leads)", OUTPUT_JSON, len(leads))


def generate_dashboard(leads: list[Lead]) -> None:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    leads_json = json.dumps([asdict(l) for l in leads], indent=2, default=str)
    generated  = datetime.now(timezone.utc).isoformat()
    html = _DASHBOARD_TEMPLATE.replace("__LEADS_JSON__", leads_json).replace("__GENERATED__", generated)
    DASHBOARD_HTML.write_text(html, encoding="utf-8")
    log.info("Dashboard saved → %s", DASHBOARD_HTML)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _fmt_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S %p", "%m/%d/%Y %I:%M %p"):
        try:
            return datetime.strptime(raw[:len(fmt)], fmt).strftime("%m/%d/%Y")
        except Exception:
            pass
    m = re.search(r"\d{2}/\d{2}/\d{4}", raw)
    return m.group(0) if m else raw[:10]


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║  SD County Motivated Seller Lead Scraper v3.0    ║")
    log.info("║  Source: ARCC Official Records (AcclaimWeb)       ║")
    log.info("╚══════════════════════════════════════════════════╝")

    today     = datetime.now(timezone.utc).date()
    date_from = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")
    log.info("Recording-date window: %s → %s (%d days)", date_from, date_to, LOOKBACK_DAYS)

    try:
        leads = asyncio.run(scrape_recorder(date_from, date_to))
    except PortalBlockedError as e:
        log.error("PORTAL BLOCKED: %s", e)
        log.error("Refusing to write stale data. Exiting non-zero so CI fails visibly.")
        sys.exit(1)

    if not leads:
        # A real, reachable search that returns nothing is possible but
        # unusual over a 30-day window — treat as failure so it never silently
        # commits an empty/stale file.
        log.error("Search returned 0 rows over a %d-day window — treating as failure.", LOOKBACK_DAYS)
        sys.exit(1)

    leads = deduplicate(leads)
    for lead in leads:
        score_lead(lead, leads)
    leads = filter_has_distress(leads)
    # Highest score first, then newest recording date first (parse MM/DD/YYYY;
    # unparseable dates sort last). Keeps the freshest, hottest leads on top.
    def _date_key(l) -> datetime:
        try:
            return datetime.strptime(l.file_date, "%m/%d/%Y")
        except (ValueError, TypeError):
            return datetime.min
    leads.sort(key=lambda l: (l.seller_score, _date_key(l)), reverse=True)

    save_json(leads)
    generate_dashboard(leads)

    log.info("─" * 50)
    log.info("  Total leads  : %d", len(leads))
    log.info("  High (≥70)   : %d", sum(1 for l in leads if l.seller_score >= 70))
    log.info("  Medium (40+) : %d", sum(1 for l in leads if 40 <= l.seller_score < 70))
    if leads:
        top = leads[0]
        log.info("  Top lead     : %s [%s] score=%d", top.grantor, top.doc_type, top.seller_score)
    log.info("─" * 50)


# Dashboard HTML (carried over from v2.x — same look, same fields) ─────────────
_DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>SD County Motivated Seller Leads</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet"/>
<style>
  :root{--bg:#0a0d14;--surface:#111520;--border:#1e2535;--accent:#e8ff47;--accent2:#ff4757;--text:#d4dbe8;--text-dim:#5a6475;--green:#39d98a;--orange:#ff7b2e;--mono:'Space Mono',monospace;--sans:'Syne',sans-serif;}
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh}
  header{border-bottom:1px solid var(--border);padding:2rem 3rem;display:flex;align-items:flex-end;justify-content:space-between;gap:1rem;flex-wrap:wrap}
  .logo-block h1{font-size:clamp(1.6rem,3vw,2.4rem);font-weight:800;letter-spacing:-.04em;line-height:1}
  .logo-block h1 span{color:var(--accent)}
  .logo-block p{font-family:var(--mono);font-size:.72rem;color:var(--text-dim);margin-top:.4rem;letter-spacing:.08em;text-transform:uppercase}
  .stats-row{display:flex;gap:2rem;flex-wrap:wrap}
  .stat .num{font-family:var(--mono);font-size:1.8rem;font-weight:700;color:var(--accent);line-height:1}
  .stat .lbl{font-size:.65rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.1em;margin-top:.15rem}
  .controls{padding:1.5rem 3rem;display:flex;gap:1rem;align-items:center;flex-wrap:wrap;border-bottom:1px solid var(--border)}
  .search-box{flex:1;min-width:200px;max-width:380px;background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:.6rem 1rem;color:var(--text);font-family:var(--mono);font-size:.8rem;outline:none}
  .search-box:focus{border-color:var(--accent)}
  .filter-btn{background:var(--surface);border:1px solid var(--border);color:var(--text-dim);padding:.6rem 1.1rem;border-radius:4px;font-family:var(--mono);font-size:.72rem;cursor:pointer;text-transform:uppercase;letter-spacing:.05em}
  .filter-btn:hover,.filter-btn.active{border-color:var(--accent);color:var(--accent);background:rgba(232,255,71,.06)}
  #count-display{font-family:var(--mono);font-size:.72rem;color:var(--text-dim);margin-left:auto}
  .export-btn{background:var(--accent);border:none;color:#0a0d14;padding:.6rem 1.2rem;border-radius:4px;font-family:var(--mono);font-size:.72rem;font-weight:700;cursor:pointer;text-transform:uppercase;letter-spacing:.05em}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:1px;background:var(--border)}
  .card{background:var(--surface);padding:1.4rem 1.6rem;cursor:pointer;position:relative;overflow:hidden}
  .card:hover{background:#161b28}
  .card::before{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:var(--score-color,var(--text-dim))}
  .card-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:.8rem}
  .doc-type{font-family:var(--mono);font-size:.65rem;text-transform:uppercase;letter-spacing:.1em;color:var(--text-dim);background:var(--border);padding:.2rem .5rem;border-radius:2px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .score-badge{font-family:var(--mono);font-size:.8rem;font-weight:700;padding:.2rem .6rem;border-radius:2px;border:1px solid var(--score-color,var(--text-dim));color:var(--score-color,var(--text-dim))}
  .grantor{font-size:1.05rem;font-weight:600;letter-spacing:-.02em;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .address{font-family:var(--mono);font-size:.72rem;color:var(--text-dim);margin-top:.25rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .card-meta{margin-top:1rem;display:grid;grid-template-columns:1fr 1fr;gap:.4rem 1rem}
  .meta-label{font-family:var(--mono);font-size:.58rem;text-transform:uppercase;letter-spacing:.1em;color:var(--text-dim)}
  .meta-value{font-family:var(--mono);font-size:.72rem;color:var(--text);margin-top:.1rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .tags{margin-top:.9rem;display:flex;flex-wrap:wrap;gap:.35rem}
  .tag{font-family:var(--mono);font-size:.58rem;padding:.15rem .45rem;border-radius:2px;text-transform:uppercase;letter-spacing:.06em}
  .tag.tax{background:rgba(57,217,138,.1);color:var(--green);border:1px solid rgba(57,217,138,.25)}
  .tag.code{background:rgba(255,123,46,.1);color:var(--orange);border:1px solid rgba(255,123,46,.25)}
  .tag.prob{background:rgba(232,255,71,.1);color:var(--accent);border:1px solid rgba(232,255,71,.25)}
  .tag.lien{background:rgba(255,71,87,.12);color:var(--accent2);border:1px solid rgba(255,71,87,.25)}
  .tag.div{background:rgba(147,112,219,.12);color:#b39ddb;border:1px solid rgba(147,112,219,.25)}
  .empty{grid-column:1/-1;text-align:center;padding:5rem 2rem;color:var(--text-dim);font-family:var(--mono);font-size:.8rem}
  footer{padding:1.5rem 3rem;border-top:1px solid var(--border);font-family:var(--mono);font-size:.65rem;color:var(--text-dim);display:flex;justify-content:space-between;flex-wrap:wrap;gap:.5rem}
</style>
</head>
<body>
<header>
  <div class="logo-block">
    <h1>SD<span> Leads</span></h1>
    <p>San Diego County · Motivated Seller Intelligence · Recorder Distress Filings</p>
  </div>
  <div class="stats-row" id="header-stats"></div>
</header>
<div class="controls">
  <input class="search-box" type="text" id="search" placeholder="Search name, doc #, legal…"/>
  <button class="filter-btn" data-filter="all">All</button>
  <button class="filter-btn" data-filter="tax">Default/Tax</button>
  <button class="filter-btn" data-filter="probate">Probate</button>
  <button class="filter-btn" data-filter="lien">Lien/Lis Pendens</button>
  <button class="filter-btn" data-filter="div">Divorce/BK</button>
  <span id="count-display"></span>
  <button class="export-btn" id="export-csv">Export CSV</button>
</div>
<div class="grid" id="grid"></div>
<footer>
  <span>Source: San Diego County Assessor-Recorder-County Clerk — Official Records (Public)</span>
  <span id="footer-ts"></span>
</footer>
<script>
const RAW = __LEADS_JSON__;
const META_GENERATED = "__GENERATED__";
function scoreColor(s){if(s>=70)return'#e8ff47';if(s>=40)return'#ff7b2e';if(s>=20)return'#ff4757';return'#5a6475';}
function tagHtml(l){let t='';if(l.has_tax_delinquency)t+='<span class="tag tax">Default/Tax</span>';if(l.has_code_violation)t+='<span class="tag code">Code Violation</span>';if(l.has_probate)t+='<span class="tag prob">Probate</span>';if(l.has_multiple_liens)t+='<span class="tag lien">Lien</span>';if(l.has_divorce_bankruptcy)t+='<span class="tag div">Divorce/BK</span>';return t;}
function renderCards(leads){const grid=document.getElementById('grid');document.getElementById('count-display').textContent=`${leads.length} leads`;if(!leads.length){grid.innerHTML='<div class="empty">No leads match your filter.</div>';return;}grid.innerHTML=leads.map(l=>{const c=scoreColor(l.seller_score);return`<div class="card" style="--score-color:${c}" title="${(l.score_reasons||[]).join(' | ')}"><div class="card-top"><span class="doc-type">${l.doc_type||'—'}</span><span class="score-badge">${l.seller_score}</span></div><div class="grantor">${l.grantor||l.grantee||'Property Record'}</div><div class="address">${l.legal_description||l.property_address||'No legal description'}</div><div class="card-meta"><div><div class="meta-label">Doc #</div><div class="meta-value">${l.document_number||'—'}</div></div><div><div class="meta-label">Recorded</div><div class="meta-value">${l.file_date||'—'}</div></div></div><div class="tags">${tagHtml(l)}</div></div>`;}).join('');}
function initStats(leads){const total=leads.length;const high=leads.filter(l=>l.seller_score>=70).length;const avg=total?Math.round(leads.reduce((a,l)=>a+l.seller_score,0)/total):0;document.getElementById('header-stats').innerHTML=`<div class="stat"><div class="num">${total}</div><div class="lbl">Total Leads</div></div><div class="stat"><div class="num">${high}</div><div class="lbl">High Score ≥70</div></div><div class="stat"><div class="num">${avg}</div><div class="lbl">Avg Score</div></div>`;document.getElementById('footer-ts').textContent='Generated: '+new Date(META_GENERATED).toLocaleString();}
let currentFilter='all',currentSearch='';
function applyFilters(){let leads=[...RAW];if(currentFilter==='tax')leads=leads.filter(l=>l.has_tax_delinquency);if(currentFilter==='probate')leads=leads.filter(l=>l.has_probate);if(currentFilter==='lien')leads=leads.filter(l=>l.has_multiple_liens);if(currentFilter==='div')leads=leads.filter(l=>l.has_divorce_bankruptcy);if(currentSearch){const q=currentSearch.toLowerCase();leads=leads.filter(l=>(l.legal_description||'').toLowerCase().includes(q)||(l.document_number||'').toLowerCase().includes(q)||(l.grantor||'').toLowerCase().includes(q)||(l.grantee||'').toLowerCase().includes(q));}renderCards(leads);}
document.querySelectorAll('.filter-btn').forEach(btn=>{btn.addEventListener('click',()=>{document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');currentFilter=btn.dataset.filter;applyFilters();});});
document.getElementById('search').addEventListener('input',e=>{currentSearch=e.target.value.trim();applyFilters();});
document.getElementById('export-csv').addEventListener('click',()=>{const cols=['document_number','file_date','doc_type','grantor','grantee','legal_description','property_address','seller_score','has_tax_delinquency','has_code_violation','has_probate','has_multiple_liens','has_divorce_bankruptcy','score_reasons','source_url'];const esc=v=>{const s=(v===null||v===undefined)?'':Array.isArray(v)?v.join('; '):String(v);return'"'+s.replace(/"/g,'""')+'"';};const rows=[cols.join(',')];let visible=[...RAW];if(currentFilter==='tax')visible=visible.filter(l=>l.has_tax_delinquency);if(currentFilter==='probate')visible=visible.filter(l=>l.has_probate);if(currentFilter==='lien')visible=visible.filter(l=>l.has_multiple_liens);if(currentFilter==='div')visible=visible.filter(l=>l.has_divorce_bankruptcy);if(currentSearch){const q=currentSearch.toLowerCase();visible=visible.filter(l=>(l.legal_description||'').toLowerCase().includes(q)||(l.document_number||'').toLowerCase().includes(q)||(l.grantor||'').toLowerCase().includes(q)||(l.grantee||'').toLowerCase().includes(q));}visible.forEach(l=>rows.push(cols.map(c=>esc(l[c])).join(',')));const blob=new Blob([rows.join('\\n')],{type:'text/csv'});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='sd_leads_'+new Date().toISOString().slice(0,10)+'.csv';a.click();URL.revokeObjectURL(a.href);});
initStats(RAW);document.querySelector('[data-filter="all"]').classList.add('active');applyFilters();
</script>
</body>
</html>"""


if __name__ == "__main__":
    main()
