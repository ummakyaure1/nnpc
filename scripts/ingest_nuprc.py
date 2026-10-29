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
APP_INGESTION_URL = os.environ.get("APP_INGESTION_URL", "").strip()
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
ENTITY_TYPES = ["licence", "field", "terminal", "crude_stream", "company", "region", "total", "other"]

EXTRACTION_TOOL = {
    "name": "record_report_data",
    "description": "Record every production figure in the report at the granularity the report uses, plus a summary of its structure.",
    "input_schema": {
        "type": "object",
        "properties": {
            "report_summary": {
                "type": "object",
                "description": "What this report contains, so the pipeline can be tuned to it.",
                "properties": {
                    "title": {"type": "string"},
                    "row_granularity": {
                        "type": "string",
                        "description": "What each data row represents, e.g. 'terminal', 'crude stream', 'company', 'OML'.",
                    },
                    "periods_covered": {"type": "string", "description": "e.g. 'January to September 2026'."},
                    "units": {"type": "string", "description": "Units exactly as printed, e.g. 'bopd', 'MMscf'."},
                    "licences_mentioned": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Every OML/OPL/PML identifier that appears anywhere in the report.",
                    },
                    "notes": {"type": "string"},
                },
                "required": ["title", "row_granularity", "periods_covered", "units", "licences_mentioned"],
            },
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "entity_type": {"type": "string", "enum": ENTITY_TYPES},
                        "entity_name": {
                            "type": "string",
                            "description": "Row label exactly as printed, e.g. 'Forcados', 'Bonny Light', 'OML 111'.",
                        },
                        "licence": {
                            "type": ["string", "null"],
                            "description": "OML identifier only if the row explicitly names one, else null.",
                        },
                        "operator": {"type": ["string", "null"], "description": "Company, if shown for the row."},
                        "period": {"type": "string", "description": "Month as YYYY-MM."},
                        "oil_value": {"type": ["number", "null"], "description": "Oil/condensate figure exactly as printed."},
                        "oil_unit": {"type": ["string", "null"], "description": "Unit as printed, e.g. 'bopd', 'bbl'."},
                        "gas_value": {"type": ["number", "null"], "description": "Gas figure exactly as printed."},
                        "gas_unit": {"type": ["string", "null"], "description": "Unit as printed, e.g. 'MMscf/d', 'MMscf', 'Bscf'."},
                        "page": {"type": ["integer", "null"]},
                        "evidence": {"type": "string", "description": "Up to 15 words copied from the row or its header."},
                        "notes": {"type": ["string", "null"]},
                    },
                    "required": ["entity_type", "entity_name", "period", "oil_value", "oil_unit",
                                 "gas_value", "gas_unit", "evidence"],
                },
            },
        },
        "required": ["report_summary", "records"],
    },
}

SYSTEM_PROMPT = (
    "You are a meticulous oil and gas regulatory data analyst. You transcribe figures from "
    "the document provided, exactly as printed. Never estimate, infer, convert, or invent numbers. "
    "If a value is not shown, use null."
)

USER_PROMPT = f"""Transcribe the production data in the attached NUPRC report.

1. Fill report_summary: what each row represents, the periods covered, the units, and every
   OML/OPL/PML identifier mentioned anywhere in the document.
2. Add one record per row per month, at the granularity the report itself uses (terminal,
   crude stream, company, field or licence). Do not skip rows because they are not OMLs.
3. Copy values and units exactly as printed. Do not convert units or derive daily rates;
   the pipeline does that. Strip thousands separators so 1,234.5 becomes 1234.5.
4. Set "licence" only when the row explicitly names an OML. Never guess which OML a
   terminal or company belongs to.
5. Mark grand totals and subtotals with entity_type "total".

Licences of particular interest (record their rows if present, but include all rows):
{", ".join(TARGET_ASSETS)}.

Call the record_report_data tool with your results."""


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
        log.warning(f"Claude hit the {max_tokens}-token output limit; extraction may be incomplete. "
                    "Raise MAX_OUTPUT_TOKENS if this repeats.")

    usage = response.usage
    log.info(f"Token usage: input={usage.input_tokens}, output={usage.output_tokens}")

    for block in response.content:
        if block.type == "tool_use" and block.name == EXTRACTION_TOOL["name"]:
            summary = block.input.get("report_summary") or {}
            log.info("Report summary from Claude:")
            for k in ("title", "row_granularity", "periods_covered", "units", "licences_mentioned", "notes"):
                if summary.get(k):
                    log.info(f"  {k}: {summary[k]}")
            return block.input.get("records", []) or []
    raise RuntimeError("Claude did not return a tool_use block.")


# ---------------------------------------------------------------------------
# Step 4: validate and normalise
# ---------------------------------------------------------------------------
_TITLE_RE = re.compile(r"\bOML\s*-?\s*(\d+)\b", re.IGNORECASE)
_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# Unit tables: normalised unit -> (factor to the target unit, is_daily_rate)
_OIL_UNITS = {
    **{u: (1.0, True) for u in ["bopd", "bpd", "bbl/d", "bbls/d", "b/d", "bbl/day", "bbls/day",
                                "barrels/day", "barrelsperday", "bbld", "bblsd", "stb/d"]},
    **{u: (1000.0, True) for u in ["kbopd", "kbpd", "kbbl/d", "mbopd", "mbpd"]},
    **{u: (1.0, False) for u in ["bbl", "bbls", "barrels", "bbl/month", "bbls/month", "barrels/month", "stb"]},
}
_GAS_UNITS = {
    **{u: (1.0, True) for u in ["mmscf/d", "mmscfd", "mmscf/day", "mmcf/d", "mmcfd", "mmscfpd"]},
    **{u: (1000.0, True) for u in ["bscf/d", "bcf/d", "bscfd", "bcfd"]},
    **{u: (1.0, False) for u in ["mmscf", "mmcf", "mmscf/month", "mmcf/month"]},
    **{u: (1000.0, False) for u in ["bscf", "bcf", "bscf/month"]},
    **{u: (0.001, False) for u in ["mscf", "mcf"]},
}


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
    if value is None:
        return None, None
    key = re.sub(r"[\s.'’]", "", (unit or "").lower()).replace("per", "/").replace("//", "/")
    if key not in table:
        return None, f"unrecognised unit {unit!r}; left unconverted"
    factor, is_daily = table[key]
    if is_daily:
        return round(value * factor, 3), None if factor == 1.0 else f"scaled x{factor:g} from {unit}"
    year, month = map(int, period.split("-"))
    days = calendar.monthrange(year, month)[1]
    return round(value * factor / days, 3), f"monthly {unit} / {days} days"


def validate_records(raw_records: list, source_url: str, source_sha256: str) -> list:
    clean, seen = [], set()
    extracted_at = datetime.now(timezone.utc).isoformat()

    for raw in raw_records:
        name = str(raw.get("entity_name") or "").strip()
        etype = raw.get("entity_type") if raw.get("entity_type") in ENTITY_TYPES else "other"
        period = str(raw.get("period") or "").strip()
        if not name:
            log.warning(f"Dropped record with no entity name: {raw}")
            continue
        if not _PERIOD_RE.match(period):
            log.warning(f"Dropped {name}: invalid period {period!r}")
            continue

        oil_value, gas_value = _to_number(raw.get("oil_value")), _to_number(raw.get("gas_value"))
        if oil_value is None and gas_value is None:
            continue

        lic_match = _TITLE_RE.search(f"{raw.get('licence') or ''} {name if etype == 'licence' else ''}")
        licence = f"OML {int(lic_match.group(1))}" if lic_match else None

        key = (etype, name.lower(), period)
        if key in seen:
            log.warning(f"Duplicate {etype} '{name}' {period}; keeping first")
            continue
        seen.add(key)

        oil_bpd, oil_note = to_daily_rate(oil_value, raw.get("oil_unit"), period, _OIL_UNITS)
        gas_rate, gas_note = to_daily_rate(gas_value, raw.get("gas_unit"), period, _GAS_UNITS)

        clean.append({
            "entity_type": etype,
            "entity_name": name,
            "licence": licence,
            "is_target_asset": licence in TARGET_ASSETS,
            "operator": raw.get("operator"),
            "period": period,
            "oil_bpd": oil_bpd,
            "gas_mmscf_d": gas_rate,
            "oil_value_as_printed": oil_value,
            "oil_unit_as_printed": raw.get("oil_unit"),
            "gas_value_as_printed": gas_value,
            "gas_unit_as_printed": raw.get("gas_unit"),
            "conversion_notes": "; ".join(n for n in (oil_note, gas_note) if n) or None,
            "source_url": source_url,
            "source_page": raw.get("page"),
            "evidence": (raw.get("evidence") or "")[:300],
            "notes": raw.get("notes"),
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
    raw = extract_with_claude(build_document_block(pdf_bytes))
    log.info(f"Claude returned {len(raw)} raw records")
    records = validate_records(raw, url, sha)
    if not records:
        log.warning(f"No usable records in {url}")
        return 0
    by_type = {}
    for r in records:
        by_type[r["entity_type"]] = by_type.get(r["entity_type"], 0) + 1
    targets = sorted({r["licence"] for r in records if r["is_target_asset"]})
    unconverted = sum(1 for r in records if r["conversion_notes"] and "unrecognised" in r["conversion_notes"])
    log.info(f"{len(records)} records by type: {by_type}")
    log.info(f"Target OMLs with explicit rows: {targets or 'none'}")
    if unconverted:
        log.warning(f"{unconverted} record(s) have units the pipeline does not recognise yet (kept as printed).")
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
