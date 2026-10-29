#!/usr/bin/env python3
"""
NUPRC regulatory ingestion pipeline.

Flow: download PDF -> send the actual document to Claude -> structured extraction
via forced tool use -> validate/normalise -> POST to Lovable unverified staging.
"""
import base64
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
MAX_OUTPUT_TOKENS = int(os.environ.get("MAX_OUTPUT_TOKENS", "").strip() or "16000")

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
EXTRACTION_TOOL = {
    "name": "record_production_figures",
    "description": "Record production figures found in the regulatory report for the target licences.",
    "input_schema": {
        "type": "object",
        "properties": {
            "records": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title_number": {
                            "type": "string",
                            "description": "Licence identifier exactly as 'OML <number>', e.g. 'OML 111'.",
                        },
                        "period": {
                            "type": "string",
                            "description": "Reporting month as YYYY-MM.",
                        },
                        "oil_bpd": {
                            "type": ["number", "null"],
                            "description": "Oil/condensate in barrels per day, only if a daily rate is stated in the document.",
                        },
                        "gas_mmscf_d": {
                            "type": ["number", "null"],
                            "description": "Gas in MMscf/d, only if stated in the document.",
                        },
                        "page": {
                            "type": ["integer", "null"],
                            "description": "1-based page number where the figure appears.",
                        },
                        "evidence": {
                            "type": "string",
                            "description": "Short verbatim snippet from the document supporting the figures.",
                        },
                        "notes": {
                            "type": ["string", "null"],
                            "description": "Units, caveats or ambiguities (e.g. figure is a monthly total, shared with another block).",
                        },
                    },
                    "required": ["title_number", "period", "oil_bpd", "gas_mmscf_d", "evidence"],
                },
            }
        },
        "required": ["records"],
    },
}

SYSTEM_PROMPT = (
    "You are a meticulous oil and gas regulatory data analyst. You extract figures only "
    "from the document provided. Never estimate, infer, or invent numbers. If a value is "
    "not explicitly stated, use null. If a target licence does not appear in the document, "
    "do not create a record for it. Returning an empty list is a valid answer."
)

USER_PROMPT = f"""Extract block-level production figures from the attached NUPRC report.

Target licences (ignore all others): {", ".join(TARGET_ASSETS)}.

Rules:
- One record per licence per reporting month. Reports covering several months
  (e.g. JAN-SEP) should produce a separate record for each month shown.
- oil_bpd and gas_mmscf_d must be daily rates as printed. Do NOT convert monthly or
  cumulative volumes; leave the field null and describe what was shown in "notes".
- If the report only gives terminal or stream totals (not per-OML figures), do not
  attribute them to an OML.
- "evidence" must be copied from the document, not paraphrased.

Call the record_production_figures tool with your results."""


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


def extract_with_claude(document_block: dict) -> list:
    log.info(f"Calling Claude model: {ANTHROPIC_MODEL}")
    with claude.messages.stream(
        model=ANTHROPIC_MODEL,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=SYSTEM_PROMPT,
        tools=[EXTRACTION_TOOL],
        tool_choice={"type": "tool", "name": EXTRACTION_TOOL["name"]},
        messages=[{
            "role": "user",
            "content": [document_block, {"type": "text", "text": USER_PROMPT}],
        }],
    ) as stream:
        response = stream.get_final_message()

    if response.stop_reason == "max_tokens":
        log.warning(f"Claude hit the {MAX_OUTPUT_TOKENS}-token output limit; extraction may be incomplete. "
                    "Raise MAX_OUTPUT_TOKENS if this repeats.")

    usage = response.usage
    log.info(f"Token usage: input={usage.input_tokens}, output={usage.output_tokens}")

    for block in response.content:
        if block.type == "tool_use" and block.name == EXTRACTION_TOOL["name"]:
            return block.input.get("records", []) or []
    raise RuntimeError("Claude did not return a tool_use block.")


# ---------------------------------------------------------------------------
# Step 4: validate and normalise
# ---------------------------------------------------------------------------
_TITLE_RE = re.compile(r"\bOML\s*-?\s*(\d+)\b", re.IGNORECASE)
_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _to_number(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value >= 0 else None
    try:
        n = float(str(value).replace(",", "").strip())
        return n if n >= 0 else None
    except ValueError:
        return None


def validate_records(raw_records: list, source_url: str, source_sha256: str) -> list:
    clean, seen = [], set()
    extracted_at = datetime.now(timezone.utc).isoformat()

    for raw in raw_records:
        m = _TITLE_RE.search(str(raw.get("title_number", "")))
        if not m:
            log.warning(f"Dropped record with unrecognised title: {raw.get('title_number')!r}")
            continue
        title = f"OML {int(m.group(1))}"
        if title not in TARGET_ASSETS:
            log.info(f"Skipping non-target licence {title}")
            continue

        period = str(raw.get("period", "")).strip()
        if not _PERIOD_RE.match(period):
            log.warning(f"Dropped {title}: invalid period {period!r}")
            continue

        oil = _to_number(raw.get("oil_bpd"))
        gas = _to_number(raw.get("gas_mmscf_d"))
        if oil is None and gas is None:
            log.info(f"Skipping {title} {period}: no oil or gas figure")
            continue

        key = (title, period)
        if key in seen:
            log.warning(f"Duplicate {title} {period} in extraction; keeping first")
            continue
        seen.add(key)

        clean.append({
            "title_number": title,
            "period": period,
            "oil_bpd": oil,
            "gas_mmscf_d": gas,
            "source_url": source_url,          # set by us, not by the model
            "source_page": raw.get("page"),
            "evidence": (raw.get("evidence") or "")[:500],
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
        {"target_table": "production", "proposed_data": r, "source_url": r["source_url"]}
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
        log.warning(f"No valid target-asset records in {url}")
        return 0
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
