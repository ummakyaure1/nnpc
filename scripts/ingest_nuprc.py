#!/usr/bin/env python3
"""
NUPRC regulatory ingestion pipeline.

Flow: download PDF -> send the actual document to Claude -> structured extraction
via forced tool use -> validate/normalise -> POST to Lovable unverified staging.
"""
import base64
import calendar
import hashlib
import io
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from collections import deque
from urllib.parse import unquote, urldefrag, urljoin, urlparse

import anthropic
import requests
from pypdf import PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("ingest_nuprc")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
APP_INGESTION_URL = os.environ.get("APP_INGESTION_URL", "").strip().strip("'\"").strip()
INGESTION_SECRET_KEY = os.environ.get("INGESTION_SECRET_KEY", "").strip()
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "").strip() or "claude-haiku-5-5"

def _csv_env(name: str) -> list:
    return [v.strip() for v in os.environ.get(name, "").split(",") if v.strip()]


# Source selection:
#   - NUPRC_PDF_URLS (manual input): direct .pdf links are processed as-is; any other
#     link is treated as a listing page and crawled for PDFs.
#   - Otherwise the crawler starts at the NUPRC reports archive and follows its sub-pages.
MANUAL_PDF_URLS = _csv_env("NUPRC_PDF_URLS")
NUPRC_REPORTS_INDEX = os.environ.get("NUPRC_REPORTS_INDEX", "").strip() or "https://www.nuprc.gov.ng/reports"
NUPRC_LISTING_PAGES = _csv_env("NUPRC_LISTING_PAGES") or [NUPRC_REPORTS_INDEX]
NUPRC_BASE_URL = (os.environ.get("NUPRC_BASE_URL", "").strip() or "https://www.nuprc.gov.ng").rstrip("/")
NUPRC_REPORT_KEYWORDS = [k.lower() for k in _csv_env("NUPRC_REPORT_KEYWORDS")] or ["production"]
NUPRC_MAX_REPORTS = int(os.environ.get("NUPRC_MAX_REPORTS", "").strip() or "1")
NUPRC_CRAWL_DEPTH = int(os.environ.get("NUPRC_CRAWL_DEPTH", "").strip() or "2")
NUPRC_MAX_PAGES = int(os.environ.get("NUPRC_MAX_PAGES", "").strip() or "60")
USE_BROWSER = os.environ.get("USE_BROWSER", "1").strip().lower() in ("1", "true", "yes")
DISCOVERY_KEYWORDS = ["production status report", "production report", "production"]
DRY_RUN = os.environ.get("DRY_RUN", "").strip().lower() in ("1", "true", "yes")
MAX_OUTPUT_TOKENS = int(os.environ.get("MAX_OUTPUT_TOKENS", "").strip() or "32000")
STAGING_TARGET_TABLE = os.environ.get("STAGING_TARGET_TABLE", "").strip() or "production_raw"

TARGET_ASSETS = [
    "OML 111", "OML 65", "OML 20", "OML 42", "OML 18", "OML 11",
    "OML 130", "OML 115", "OML 127", "OML 54", "OML 53",
]

# Claude PDF input limits: 32 MB request size, 100 pages for full visual PDF support.
MAX_PDF_BYTES = 30 * 1024 * 1024
MAX_PDF_PAGES = 100
MAX_TEXT_CHARS = 400_000  # cap for the text-only fallback

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (NNPC-Asset-Intelligence ingestion bot)"}

# ---------------------------------------------------------------------------
# Structured output schema (forced tool use = guaranteed JSON, no fence-stripping)
# ---------------------------------------------------------------------------
ENTITY_TYPES = ["licence", "field", "terminal", "crude_stream", "company", "region", "national", "other"]
MEASURES = ["crude_oil", "condensate", "oil_and_condensate",
            "gas_produced", "gas_utilised", "gas_flared", "gas_other"]
OIL_MEASURES = {"crude_oil", "condensate", "oil_and_condensate"}

# Compact "wide" format: one entry per table row, with every month inside it.
# This keeps output small enough for multi-month reports; the script expands it to one
# record per row per month afterwards.
EXTRACTION_TOOL = {
    "name": "record_report_data",
    "description": "Record every data row of the report, with all its monthly values, plus a summary of the report.",
    "input_schema": {
        "type": "object",
        "properties": {
            "report_summary": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "row_granularity": {"type": "string"},
                    "periods_covered": {"type": "string"},
                    "units": {"type": "string"},
                    "licences_mentioned": {"type": "array", "items": {"type": "string"}},
                    "notes": {"type": "string"},
                },
                "required": ["title", "row_granularity", "periods_covered", "units", "licences_mentioned"],
            },
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "entity_type": {"type": "string", "enum": ENTITY_TYPES},
                        "entity_name": {"type": "string", "description": "Row label as printed, e.g. 'Forcados'. Use 'Nigeria' for national figures."},
                        "licence": {"type": ["string", "null"], "description": "OML only if explicitly named for this row."},
                        "measure": {"type": "string", "enum": MEASURES},
                        "measure_label": {"type": "string", "description": "Row/column label as printed, e.g. 'Blend Total', 'Unblended Condensate', 'Gas Flared'."},
                        "unit": {"type": "string", "description": "Unit as printed, e.g. 'thousand bbl/d', 'MMscf'."},
                        "values": {
                            "type": "object",
                            "description": "Keys are months as YYYY-MM; values are numbers exactly as printed. Omit months with no value.",
                            "additionalProperties": {"type": "number"},
                        },
                        "page": {"type": ["integer", "null"]},
                    },
                    "required": ["entity_type", "entity_name", "measure", "measure_label", "unit", "values"],
                },
            },
        },
        "required": ["report_summary", "rows"],
    },
}

SYSTEM_PROMPT = (
    "You are a meticulous oil and gas regulatory data analyst. You transcribe figures from "
    "the document provided, exactly as printed. Never estimate, infer, convert, or invent numbers."
)

USER_PROMPT = f"""Transcribe the production data in the attached NUPRC report.

1. Fill report_summary: what rows represent, periods covered, units, and every OML/OPL/PML
   identifier mentioned anywhere in the document.
2. Add ONE entry per data row, with all of that row's months inside "values"
   (e.g. {{"2026-01": 245.3, "2026-02": 251.0}}). Do not create one entry per month.
3. Terminals with sub-rows (Crude Oil / Condensate / Blend Total) become separate entries with
   measure crude_oil / condensate / oil_and_condensate and the printed sub-row name in measure_label.
4. National tables (e.g. rows are months, columns are gas produced / utilised / flared) become
   entity_type "national", entity_name "Nigeria", one entry per column, keyed by month.
   National summary rows (e.g. "Daily Average: Crude Oil", "Total") are also "national".
5. Copy numbers and units exactly as printed; strip thousands separators. Do not convert.
6. Omit months shown as blank or dashes. Skip percentage columns and year-total / year-average
   columns; only monthly values belong in "values".
7. Set "licence" only when a row explicitly names an OML. Never guess which OML a terminal
   or company belongs to.

Licences of particular interest: {", ".join(TARGET_ASSETS)}.

Call the record_report_data tool."""


# ---------------------------------------------------------------------------
# Step 0: discover report URLs
# ---------------------------------------------------------------------------
_HREF_RE = re.compile(r"""(?:href|src|data-href|data-url|data-file)\s*=\s*["']([^"'#]+)""", re.IGNORECASE)
# Catches PDF URLs embedded in scripts/JSON (e.g. "https:\/\/site\/file.pdf")
_ABS_PDF_RE = re.compile(r"""https?:(?://|\\/\\/)[^\s"'<>()]+?\.pdf\b""", re.IGNORECASE)

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}
_MONTH_NAMES = {**_MONTHS, **{full: _MONTHS[full[:3]] for full in [
    "january", "february", "march", "april", "june", "july", "august",
    "september", "sept", "october", "november", "december"]}}
_YEAR_MONTH_RE = re.compile(r"((?:19|20)\d{2})[\s_\-./](0?[1-9]|1[0-2])(?!\d)")
_YEAR_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")


def _clean_url(raw: str) -> str:
    return raw.replace("\\/", "/").replace("\\u002F", "/").replace("&amp;", "&").strip()


def _is_pdf_link(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def guess_report_period(url: str) -> str:
    """Best-effort YYYY-MM from the file name, used to rank reports newest first.

    Handles 'JAN-SEP_2026', '2026-Monthly-Gas-Data_August', 'Production_2026-08'.
    Year-only names return 'YYYY-00' (treated as a full-year report).
    """
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1]).lower()
    years = _YEAR_RE.findall(name)
    if not years:
        return "0000-00"
    year = max(years)
    numeric = [f"{y}-{int(m):02d}" for y, m in _YEAR_MONTH_RE.findall(name)]
    if numeric:
        return max(numeric)
    tokens = re.split(r"[^a-z]+", name)
    months = [_MONTH_NAMES[t] for t in tokens if t in _MONTH_NAMES]
    return f"{year}-{max(months):02d}" if months else f"{year}-00"


def extract_links(html: str, base_url: str):
    pages, pdfs = set(), set()
    for raw in _HREF_RE.findall(html):
        url = urldefrag(urljoin(base_url, _clean_url(raw)))[0]
        if not url.startswith("http"):
            continue
        (pdfs if _is_pdf_link(url) else pages).add(url)
    for raw in _ABS_PDF_RE.findall(html):
        pdfs.add(_clean_url(raw))
    return pages, pdfs


def fetch_html_static(url: str):
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Could not load {url}: {e}")
        return None
    if "html" not in r.headers.get("Content-Type", "").lower():
        return None
    return r.text


class BrowserFetcher:
    """Headless Chromium for pages that load their report lists with JavaScript."""

    def __enter__(self):
        from playwright.sync_api import sync_playwright  # imported lazily; optional dependency
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch()
        self._page = self._browser.new_page(user_agent=HTTP_HEADERS["User-Agent"])
        return self

    def __call__(self, url: str):
        try:
            self._page.goto(url, wait_until="networkidle", timeout=45_000)
            return self._page.content()
        except Exception as e:
            log.warning(f"Browser could not load {url}: {e}")
            return None

    def __exit__(self, *exc):
        self._browser.close()
        self._pw.stop()


def crawl_for_pdfs(start_urls: list, fetch_html) -> dict:
    """Breadth-first crawl limited to the start pages' host and path prefix. Returns {pdf_url: page_found_on}."""
    hosts = {urlparse(u).netloc for u in start_urls}
    prefixes = {urlparse(u).path.rstrip("/") or "/" for u in start_urls}
    queue = deque((u, 0) for u in start_urls)
    visited, found, seen_files = set(), {}, set()

    while queue and len(visited) < NUPRC_MAX_PAGES:
        url, depth = queue.popleft()
        if url in visited:
            continue
        visited.add(url)
        html = fetch_html(url)
        if html is None:
            continue
        pages, pdfs = extract_links(html, url)
        log.info(f"  scanned {url}: {len(pdfs)} PDF link(s)")
        for pdf in pdfs:
            key = urlparse(pdf)._replace(query="").geturl()  # same file with/without ?v=1
            if key not in seen_files:
                seen_files.add(key)
                found[pdf] = url
        if depth < NUPRC_CRAWL_DEPTH:
            for link in sorted(pages):
                parsed = urlparse(link)
                if (parsed.netloc in hosts and link not in visited
                        and any(parsed.path.startswith(p) for p in prefixes)):
                    queue.append((link, depth + 1))

    log.info(f"Crawl finished: {len(visited)} page(s) visited, {len(found)} PDF link(s) found.")
    return found


def filter_and_rank(found: dict) -> list:
    """Keep PDFs whose URL or source page matches the report keywords; newest first."""
    def is_match(pdf, page):
        if any(k in unquote(pdf).lower() for k in NUPRC_REPORT_KEYWORDS):
            return True
        # File name lacks the keyword but sits on a matching report page: accept only if
        # the file name looks dated (skips footer links such as privacy.pdf).
        on_report_page = any(k in unquote(page).lower() for k in NUPRC_REPORT_KEYWORDS)
        return on_report_page and guess_report_period(pdf) != "0000-00"

    matches = [(pdf, page) for pdf, page in found.items() if is_match(pdf, page)]
    return sorted(matches, key=lambda m: (guess_report_period(m[0]), m[0]), reverse=True)


def _series_name(page: str) -> str:
    """The report series is the page the PDF was listed on, e.g. 'oil-production-report'."""
    return urlparse(page).path.rstrip("/").rsplit("/", 1)[-1] or "unknown" if page else "unknown"


def select_newest_per_series(matches: list, per_series: int) -> list:
    groups = {}
    for pdf, page in matches:  # already newest first
        groups.setdefault(_series_name(page), []).append(pdf)
    selected = []
    for series in sorted(groups):
        pdfs = groups[series]
        log.info(f"Series '{series}': {len(pdfs)} report(s)")
        for i, pdf in enumerate(pdfs[:15], start=1):
            marker = "  <- selected" if i <= per_series else ""
            log.info(f"  {i:>2}. [{guess_report_period(pdf)}] {pdf}{marker}")
        selected.extend(pdfs[:per_series])
    return selected


def discover_via_wp_media_api() -> list:
    """Last-resort fallback: search a WordPress media library, if the site exposes one."""
    endpoint = f"{NUPRC_BASE_URL}/wp-json/wp/v2/media"
    for keyword in DISCOVERY_KEYWORDS:
        try:
            r = requests.get(endpoint, params={"search": keyword, "mime_type": "application/pdf",
                                               "per_page": 30, "orderby": "date", "order": "desc"},
                             headers=HTTP_HEADERS, timeout=30)
            items = r.json() if r.status_code == 200 else []
        except (requests.RequestException, ValueError):
            return []
        urls = [i.get("source_url", "") for i in items if isinstance(i, dict)]
        urls = [u for u in urls if _is_pdf_link(u)]
        if urls:
            return urls
    return []


def resolve_pdf_urls() -> list:
    direct = [u for u in MANUAL_PDF_URLS if _is_pdf_link(u)]
    manual_pages = [u for u in MANUAL_PDF_URLS if not _is_pdf_link(u)]
    if direct and not manual_pages:
        log.info(f"Using {len(direct)} manually supplied PDF link(s).")
        return direct

    start_pages = manual_pages or NUPRC_LISTING_PAGES
    log.info(f"Crawling for reports from: {', '.join(start_pages)}")
    log.info(f"Report keywords: {', '.join(NUPRC_REPORT_KEYWORDS)}")

    candidates = filter_and_rank(crawl_for_pdfs(start_pages, fetch_html_static))

    if not candidates and USE_BROWSER:
        log.info("No PDFs in static HTML; retrying with a headless browser (JavaScript-rendered pages).")
        try:
            with BrowserFetcher() as browser:
                candidates = filter_and_rank(crawl_for_pdfs(start_pages, browser))
        except ImportError:
            log.warning("Playwright is not installed; skipping browser crawl.")
        except Exception as e:
            log.warning(f"Browser crawl failed: {e}")

    if not candidates:
        candidates = filter_and_rank({u: "" for u in discover_via_wp_media_api()})

    if not candidates and not direct:
        raise RuntimeError(
            f"No PDFs matching {NUPRC_REPORT_KEYWORDS} found under {', '.join(start_pages)}. "
            "Run with dry_run to see what the crawler visited, or paste a direct PDF link."
        )

    log.info(f"{len(candidates)} matching report(s) found. Newest first, per series:")
    selected = direct + select_newest_per_series(candidates, NUPRC_MAX_REPORTS)
    log.info(f"Selected {len(selected)} report(s) for extraction.")
    return selected


# ---------------------------------------------------------------------------
# Step 1: download
# ---------------------------------------------------------------------------
def download_pdf(url: str, attempts: int = 3) -> bytes:
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            log.info(f"Downloading PDF (attempt {attempt}): {url}")
            r = requests.get(url, headers=HTTP_HEADERS, timeout=60)
            r.raise_for_status()
            content = r.content
            if not content.startswith(b"%PDF"):
                ctype = r.headers.get("Content-Type", "unknown")
                raise ValueError(f"URL did not return a PDF (Content-Type: {ctype}). The link may have moved.")
            log.info(f"Downloaded {len(content) / 1024:.1f} KB")
            return content
        except (requests.RequestException, ValueError) as e:
            last_err = e
            # 4xx (other than 429) and non-PDF responses won't fix themselves
            status = getattr(getattr(e, "response", None), "status_code", None)
            if isinstance(e, ValueError) or (status and 400 <= status < 500 and status != 429):
                break
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to download {url}: {last_err}")


# ---------------------------------------------------------------------------
# Step 2: build the document block for Claude
# ---------------------------------------------------------------------------
def build_document_block(pdf_bytes: bytes) -> dict:
    try:
        page_count = len(PdfReader(io.BytesIO(pdf_bytes)).pages)
    except Exception as e:
        log.warning(f"pypdf could not read page count ({e}); sending PDF as-is.")
        page_count = 0

    if len(pdf_bytes) <= MAX_PDF_BYTES and page_count <= MAX_PDF_PAGES:
        log.info(f"Sending native PDF to Claude ({page_count} pages).")
        return {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": base64.standard_b64encode(pdf_bytes).decode("ascii"),
            },
        }

    # Fallback for oversized reports: plain-text extraction with page markers.
    log.warning(f"PDF too large for native input ({page_count} pages); falling back to text extraction.")
    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts = [f"--- Page {i} ---\n{page.extract_text() or ''}" for i, page in enumerate(reader.pages, start=1)]
    text = "\n".join(parts)[:MAX_TEXT_CHARS]
    return {
        "type": "document",
        "source": {"type": "text", "media_type": "text/plain", "data": text},
    }


# ---------------------------------------------------------------------------
# Step 3: extract with Claude
# ---------------------------------------------------------------------------
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY or None, max_retries=3, timeout=600.0)


_MAX_TOKENS_LIMIT_RE = re.compile(r"max_tokens: \d+ > (\d+)")


def _call_claude(document_block: dict, max_tokens: int):
    with claude.messages.stream(
        model=ANTHROPIC_MODEL,
        max_tokens=max_tokens,
        system=SYSTEM_PROMPT,
        tools=[EXTRACTION_TOOL],
        tool_choice={"type": "tool", "name": EXTRACTION_TOOL["name"]},
        messages=[{
            "role": "user",
            "content": [document_block, {"type": "text", "text": USER_PROMPT}],
        }],
    ) as stream:
        return stream.get_final_message()


def extract_with_claude(document_block: dict) -> list:
    log.info(f"Calling Claude model: {ANTHROPIC_MODEL} (max_tokens={MAX_OUTPUT_TOKENS})")
    max_tokens = MAX_OUTPUT_TOKENS
    try:
        response = _call_claude(document_block, max_tokens)
    except anthropic.BadRequestError as e:
        # If MAX_OUTPUT_TOKENS exceeds the model's ceiling, retry once at the ceiling.
        m = _MAX_TOKENS_LIMIT_RE.search(str(e))
        if not m:
            raise
        max_tokens = int(m.group(1))
        log.warning(f"MAX_OUTPUT_TOKENS={MAX_OUTPUT_TOKENS} exceeds the model limit; retrying with {max_tokens}. "
                    "Lower the MAX_OUTPUT_TOKENS repository variable to silence this.")
        response = _call_claude(document_block, max_tokens)

    if response.stop_reason == "max_tokens":
        raise RuntimeError(
            f"Claude hit the {max_tokens}-token output limit, so the extraction is incomplete and "
            "nothing was staged. Raise the MAX_OUTPUT_TOKENS variable (max 128000 for Haiku 5.5)."
        )

    usage = response.usage
    log.info(f"Token usage: input={usage.input_tokens}, output={usage.output_tokens}")

    for block in response.content:
        if block.type == "tool_use" and block.name == EXTRACTION_TOOL["name"]:
            summary = block.input.get("report_summary") or {}
            log.info("Report summary from Claude:")
            for k in ("title", "row_granularity", "periods_covered", "units", "licences_mentioned", "notes"):
                if summary.get(k):
                    log.info(f"  {k}: {summary[k]}")
            return summary, block.input.get("rows", []) or []
    raise RuntimeError("Claude did not return a tool_use block.")


# ---------------------------------------------------------------------------
# Step 4: validate and normalise
# ---------------------------------------------------------------------------
_TITLE_RE = re.compile(r"\bOML\s*-?\s*(\d+)\b", re.IGNORECASE)
_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# Normalised unit -> (factor to target unit, is_daily_rate). Targets: bopd for oil, MMscf/d for gas.
_OIL_UNITS = {
    **{u: (1.0, True) for u in ["bopd", "bpd", "bbl/d", "bbls/d", "b/d", "bbl/day", "bbls/day",
                                "barrels/day", "barrels/d", "bbld", "bblsd", "stb/d"]},
    **{u: (1000.0, True) for u in ["kbopd", "kbpd", "kbbl/d", "kb/d"]},
    **{u: (1.0, False) for u in ["bbl", "bbls", "barrels", "bbl/month", "bbls/month", "barrels/month", "stb"]},
}
_GAS_UNITS = {
    **{u: (1.0, True) for u in ["mmscf/d", "mmscfd", "mmscf/day", "mmcf/d", "mmcfd", "mmscfpd"]},
    **{u: (1000.0, True) for u in ["bscf/d", "bcf/d", "bscfd", "bcfd"]},
    **{u: (1.0, False) for u in ["mmscf", "mmcf", "mmscf/month", "mmcf/month"]},
    **{u: (1000.0, False) for u in ["bscf", "bcf", "bscf/month"]},
    **{u: (0.001, False) for u in ["mscf", "mcf"]},
}
_MULTIPLIERS = [("thousand", 1e3), ("million", 1e6), ("000", 1e3)]


def _normalise_unit(unit: str):
    """'Thousand barrels per day (provisional)' -> ('barrels/day', 1000)."""
    u = re.sub(r"\(.*?\)", "", (unit or "").lower())
    u = re.sub(r"[\s.'’,]", "", u)
    multiplier = 1.0
    for word, factor in _MULTIPLIERS:
        if u.startswith(word):
            u, multiplier = u[len(word):], multiplier * factor
    u = u.replace("per", "/").replace("//", "/")
    return u, multiplier


def _to_number(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value >= 0 else None
    try:
        n = float(str(value).replace(",", "").strip())
        return n if n >= 0 else None
    except ValueError:
        return None


def to_daily_rate(value, unit, period: str, table: dict):
    """Convert an as-printed figure to a daily rate. Returns (rate, conversion_note)."""
    key, multiplier = _normalise_unit(unit)
    if key not in table:
        return None, f"unrecognised unit {unit!r}; left unconverted"
    factor, is_daily = table[key]
    factor *= multiplier
    notes = [f"x{factor:g} from {unit}"] if factor != 1.0 else []
    if is_daily:
        return round(value * factor, 3), "; ".join(notes) or None
    year, month = map(int, period.split("-"))
    days = calendar.monthrange(year, month)[1]
    return round(value * factor / days, 3), "; ".join(notes + [f"monthly volume / {days} days"])


def expand_rows(rows: list, source_url: str, source_sha256: str, report_title: str) -> list:
    """Wide rows from Claude -> one validated record per row per month."""
    clean, seen = [], set()
    extracted_at = datetime.now(timezone.utc).isoformat()

    for row in rows:
        name = str(row.get("entity_name") or "").strip()
        etype = row.get("entity_type") if row.get("entity_type") in ENTITY_TYPES else "other"
        measure = row.get("measure")
        label = str(row.get("measure_label") or "").strip()
        unit = row.get("unit")
        values = row.get("values") if isinstance(row.get("values"), dict) else {}
        if not name or measure not in MEASURES:
            log.warning(f"Dropped row with missing name or measure: {name!r} / {measure!r}")
            continue

        lic_match = _TITLE_RE.search(f"{row.get('licence') or ''} {name if etype == 'licence' else ''}")
        licence = f"OML {int(lic_match.group(1))}" if lic_match else None
        is_oil = measure in OIL_MEASURES

        for period, raw_value in values.items():
            period = str(period).strip()
            value = _to_number(raw_value)
            if not _PERIOD_RE.match(period):
                log.warning(f"Dropped {name}/{label}: invalid period {period!r}")
                continue
            if value is None:
                continue
            key = (etype, name.lower(), measure, label.lower(), period)
            if key in seen:
                continue
            seen.add(key)

            rate, note = to_daily_rate(value, unit, period, _OIL_UNITS if is_oil else _GAS_UNITS)
            clean.append({
                "entity_type": etype,
                "entity_name": name,
                "licence": licence,
                "is_target_asset": licence in TARGET_ASSETS,
                "measure": measure,
                "measure_label": label,
                "period": period,
                "oil_bpd": rate if is_oil else None,
                "gas_mmscf_d": None if is_oil else rate,
                "value_as_printed": value,
                "unit_as_printed": unit,
                "conversion_notes": note,
                "report_title": report_title,
                "source_url": source_url,
                "source_page": row.get("page"),
                "source_sha256": source_sha256,
                "extraction_model": ANTHROPIC_MODEL,
                "extracted_at": extracted_at,
            })
    return clean


# ---------------------------------------------------------------------------
# Step 5: post to Lovable staging
# ---------------------------------------------------------------------------
def send_to_lovable_webhook(records: list, attempts: int = 3) -> None:
    payload = [
        {"target_table": STAGING_TARGET_TABLE, "proposed_data": r, "source_url": r["source_url"]}
        for r in records
    ]
    headers = {"Content-Type": "application/json", "x-ingestion-key": INGESTION_SECRET_KEY}

    for attempt in range(1, attempts + 1):
        log.info(f"Posting {len(payload)} records to staging (attempt {attempt})...")
        try:
            r = requests.post(APP_INGESTION_URL, json=payload, headers=headers, timeout=60)
        except requests.RequestException as e:
            log.warning(f"Network error posting to webhook: {e}")
        else:
            if 200 <= r.status_code < 300:
                log.info(f"Staging accepted records (HTTP {r.status_code}).")
                return
            body = r.text[:500]
            if r.status_code in (401, 403):
                raise RuntimeError(f"Webhook rejected the ingestion key (HTTP {r.status_code}): {body}")
            if 400 <= r.status_code < 500 and r.status_code != 429:
                raise RuntimeError(f"Webhook rejected payload (HTTP {r.status_code}): {body}")
            log.warning(f"Webhook returned HTTP {r.status_code}: {body}")
        time.sleep(2 ** attempt)
    raise RuntimeError("Failed to post records to staging after retries.")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def process_url(url: str) -> int:
    pdf_bytes = download_pdf(url)
    sha = hashlib.sha256(pdf_bytes).hexdigest()
    summary, rows = extract_with_claude(build_document_block(pdf_bytes))
    records = expand_rows(rows, url, sha, summary.get("title") or "")
    log.info(f"Claude returned {len(rows)} table rows -> {len(records)} monthly records")
    if not records:
        log.warning(f"No usable records in {url}")
        return 0

    by_type, by_measure = {}, {}
    for r in records:
        by_type[r["entity_type"]] = by_type.get(r["entity_type"], 0) + 1
        by_measure[r["measure"]] = by_measure.get(r["measure"], 0) + 1
    targets = sorted({r["licence"] for r in records if r["is_target_asset"]})
    unconverted = sorted({r["unit_as_printed"] for r in records
                          if r["conversion_notes"] and "unrecognised" in r["conversion_notes"]})
    log.info(f"  by entity type: {by_type}")
    log.info(f"  by measure: {by_measure}")
    log.info(f"  target OMLs with explicit rows: {targets or 'none'}")
    if unconverted:
        log.warning(f"  units not recognised (kept as printed, rate left empty): {unconverted}")
    send_to_lovable_webhook(records)
    return len(records)


def main() -> int:
    missing = [n for n, v in {
        "APP_INGESTION_URL": APP_INGESTION_URL,
        "INGESTION_SECRET_KEY": INGESTION_SECRET_KEY,
        "ANTHROPIC_API_KEY": ANTHROPIC_API_KEY,
    }.items() if not v]
    if missing:
        log.error(f"Missing required environment variables: {', '.join(missing)}")
        return 1
    parsed = urlparse(APP_INGESTION_URL)
    if parsed.scheme != "https" or not parsed.netloc or " " in APP_INGESTION_URL:
        log.error(
            "APP_INGESTION_URL is not a valid https:// address "
            f"(length {len(APP_INGESTION_URL)}, starts with https://: {APP_INGESTION_URL.startswith('https://')}). "
            "Re-save the secret as the bare URL with no quotes, brackets or spaces."
        )
        return 1

    try:
        pdf_urls = resolve_pdf_urls()
    except RuntimeError as e:
        log.error(str(e))
        return 1

    if DRY_RUN:
        log.info("DRY_RUN enabled: discovery only, no Claude calls or webhook posts.")
        return 0

    total, failures = 0, []
    for url in pdf_urls:
        try:
            total += process_url(url)
        except anthropic.NotFoundError as e:
            log.error(f"Model '{ANTHROPIC_MODEL}' not found. Set the ANTHROPIC_MODEL repo variable. ({e})")
            return 1
        except Exception as e:
            log.exception(f"Failed processing {url}: {e}")
            failures.append(url)

    log.info(f"Done. {total} records staged from {len(pdf_urls) - len(failures)}/{len(pdf_urls)} documents.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
