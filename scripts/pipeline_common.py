"""
Shared engine for the NNPC Asset Intelligence collectors.

Every collector follows the same rule: it only PROPOSES records. Everything is posted
to the MD review queue (unverified_staging) and nothing goes live without approval.
"""
import base64
import hashlib
import io
import json
import logging
import os
import re
import time
from collections import deque
from urllib.parse import unquote, urldefrag, urljoin, urlparse

import anthropic
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("collectors")


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, "").strip() or default


APP_INGESTION_URL = env("APP_INGESTION_URL").strip("'\"").strip()
INGESTION_SECRET_KEY = env("INGESTION_SECRET_KEY")
ANTHROPIC_API_KEY = env("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = env("ANTHROPIC_MODEL", "claude-haiku-5-5")
MAX_OUTPUT_TOKENS = int(env("MAX_OUTPUT_TOKENS", "32000"))
DRY_RUN = env("DRY_RUN").lower() in ("1", "true", "yes")
USE_BROWSER = env("USE_BROWSER", "1").lower() in ("1", "true", "yes")

TARGET_ASSETS = [
    "OML 111", "OML 65", "OML 20", "OML 42", "OML 18", "OML 11",
    "OML 130", "OML 115", "OML 127", "OML 54", "OML 53",
]
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (NNPC-Asset-Intelligence ingestion bot)"}
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sources.json")

# Nigeria's rough bounding box, used to sanity-check coordinates.
NIGERIA_BBOX = {"lat": (2.5, 14.5), "lon": (2.0, 15.5)}

STATS = {"claude_input_tokens": 0, "claude_output_tokens": 0}


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def check_env(need_claude: bool = True) -> bool:
    required = {"APP_INGESTION_URL": APP_INGESTION_URL, "INGESTION_SECRET_KEY": INGESTION_SECRET_KEY}
    if need_claude:
        required["ANTHROPIC_API_KEY"] = ANTHROPIC_API_KEY
    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error(f"Missing environment variables: {', '.join(missing)}")
        return False
    parsed = urlparse(APP_INGESTION_URL)
    if parsed.scheme != "https" or not parsed.netloc or " " in APP_INGESTION_URL:
        log.error("APP_INGESTION_URL is not a valid https:// address. Re-save the secret as the bare URL.")
        return False
    return True


# ---------------------------------------------------------------------------
# Titles (OML / OPL / PML / PPL / PEL)
# ---------------------------------------------------------------------------
TITLE_RE = re.compile(r"\b(OML|OPL|PML|PPL|PEL)\s*[-‐–]?\s*(\d{1,4})\b", re.IGNORECASE)


def normalise_title(text) -> str | None:
    m = TITLE_RE.search(str(text or ""))
    return f"{m.group(1).upper()} {int(m.group(2))}" if m else None


def titles_in(text) -> list:
    seen = []
    for m in TITLE_RE.finditer(str(text or "")):
        t = f"{m.group(1).upper()} {int(m.group(2))}"
        if t not in seen:
            seen.append(t)
    return seen


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(HTTP_HEADERS)


def fetch(url: str, attempts: int = 3, timeout: int = 60) -> requests.Response:
    last = None
    for attempt in range(1, attempts + 1):
        try:
            r = SESSION.get(url, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status and 400 <= status < 500 and status != 429:
                break
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last}")


def fetch_pdf(url: str) -> bytes:
    content = fetch(url).content
    if not content.startswith(b"%PDF"):
        raise ValueError(f"{url} did not return a PDF")
    return content


# ---------------------------------------------------------------------------
# Text helpers and evidence checks
# ---------------------------------------------------------------------------
def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header", "form", "aside"]):
        tag.decompose()
    main = soup.find("article") or soup.find("main") or soup.body or soup
    text = main.get_text("\n")
    return re.sub(r"\n\s*\n+", "\n\n", re.sub(r"[ \t]+", " ", text)).strip()


_QUOTE_MAP = str.maketrans({"’": "'", "‘": "'", "“": '"', "”": '"', "–": "-", "—": "-", "\u00a0": " "})


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").translate(_QUOTE_MAP)).strip().lower()


def evidence_found(source_text: str, quote: str) -> bool:
    """True when the quote (whitespace/quote-style normalised) occurs in the source text.
    Used to drop anything the model did not actually read in the document."""
    q = _norm(quote).strip(" .\"'")
    return len(q) >= 12 and q in _norm(source_text)


def pdf_text(pdf_bytes: bytes, max_pages: int = 200) -> str:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join((p.extract_text() or "") for p in reader.pages[:max_pages])
    except Exception as e:
        log.warning(f"Could not extract PDF text: {e}")
        return ""


def pdf_chunks(pdf_bytes: bytes, size: int = 90) -> list:
    """Split a long PDF into parts of at most `size` pages. Returns [(first_page_number, bytes)]."""
    from pypdf import PdfWriter
    reader = PdfReader(io.BytesIO(pdf_bytes))
    total = len(reader.pages)
    if total <= size:
        return [(1, pdf_bytes)]
    parts = []
    for start in range(0, total, size):
        writer = PdfWriter()
        for i in range(start, min(start + size, total)):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        parts.append((start + 1, buf.getvalue()))
    log.info(f"Split a {total}-page PDF into {len(parts)} parts of up to {size} pages.")
    return parts


def pdf_document_block(pdf_bytes: bytes) -> dict:
    """Native PDF for Claude when it fits; otherwise extracted text with page markers."""
    try:
        pages = len(PdfReader(io.BytesIO(pdf_bytes)).pages)
    except Exception:
        pages = 0
    if len(pdf_bytes) <= 30 * 1024 * 1024 and pages <= 100:
        return {"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                               "data": base64.standard_b64encode(pdf_bytes).decode("ascii")}}
    log.warning(f"PDF has {pages} pages; sending extracted text instead.")
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = "\n".join(f"--- Page {i} ---\n{p.extract_text() or ''}" for i, p in enumerate(reader.pages, 1))
    return {"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": text[:400_000]}}


# ---------------------------------------------------------------------------
# Crawling (static HTML first, headless browser fallback for JavaScript pages)
# ---------------------------------------------------------------------------
_EMBEDDED_URL_RE = re.compile(r"""https?:(?://|\\/\\/)[^\s"'<>()]+?\.(?:pdf|csv|xlsx?|json)\b""", re.IGNORECASE)


def page_links(html: str, base_url: str) -> list:
    """[(absolute_url, anchor_text)] including file URLs embedded in scripts/JSON."""
    out, seen = [], set()
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        url = urldefrag(urljoin(base_url, a["href"].strip()))[0]
        if url.startswith("http") and url not in seen:
            seen.add(url)
            out.append((url, re.sub(r"\s+", " ", a.get_text(" ")).strip()))
    for raw in _EMBEDDED_URL_RE.findall(html):
        url = raw.replace("\\/", "/")
        if url not in seen:
            seen.add(url)
            out.append((url, ""))
    return out


def fetch_html_static(url: str):
    try:
        r = fetch(url, attempts=2, timeout=30)
    except Exception as e:
        log.warning(f"Could not load {url}: {e}")
        return None
    return r.text if "html" in r.headers.get("Content-Type", "").lower() else None


class BrowserFetcher:
    def __enter__(self):
        from playwright.sync_api import sync_playwright
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


NON_PAGE_EXT = {"jpg", "jpeg", "png", "gif", "svg", "webp", "ico", "pdf", "csv", "xlsx", "xls", "zip",
                "doc", "docx", "ppt", "pptx", "mp4", "mp3", "json", "xml", "css", "js"}


def crawl(start_urls: list, follow, fetch_html, max_pages: int = 40, depth: int = 1) -> dict:
    """Breadth-first crawl. follow(url) decides which pages to open.
    Returns {link_url: {"text": anchor_text, "found_on": page_url}} for every link seen."""
    queue = deque((u, 0) for u in start_urls)
    visited, links = set(), {}
    while queue and len(visited) < max_pages:
        url, d = queue.popleft()
        if url in visited:
            continue
        visited.add(url)
        html = fetch_html(url)
        if html is None:
            continue
        found = page_links(html, url)
        log.info(f"  scanned {url}: {len(found)} link(s)")
        for link, text in found:
            if link not in links or (text and not links[link]["text"]):
                links[link] = {"text": text, "found_on": url}
            if d < depth and link not in visited and file_ext(link) not in NON_PAGE_EXT and follow(link):
                queue.append((link, d + 1))
    return links


def render_page(url: str, wait_ms: int = 6000, max_bytes: int = 25 * 1024 * 1024):
    """Open a page in a headless browser. Returns (rendered_html, captured_data_responses).
    Each captured item: {"url", "content_type", "body"}. Falls back to plain HTML without a browser."""
    if not USE_BROWSER:
        return fetch_html_static(url), []
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return fetch_html_static(url), []
    captured = []

    def on_response(resp):
        ctype = (resp.headers.get("content-type") or "").lower()
        if "json" not in ctype and "csv" not in ctype:
            return
        try:
            body = resp.body()
        except Exception:
            return
        if 200 <= len(body) <= max_bytes:
            captured.append({"url": resp.url, "content_type": ctype, "body": body})

    html = None
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(user_agent=HTTP_HEADERS["User-Agent"])
        page.on("response", on_response)
        try:
            page.goto(url, wait_until="networkidle", timeout=90_000)
            page.wait_for_timeout(wait_ms)
            html = page.content()
        except Exception as e:
            log.warning(f"Browser load of {url} incomplete: {e}")
            try:
                html = page.content()
            except Exception:
                pass
        browser.close()
    captured.sort(key=lambda c: len(c["body"]), reverse=True)
    return html, captured


_MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


def guess_period(text: str) -> str:
    """Best-effort 'YYYY-MM' (or 'YYYY-00') from a title or file name, for ranking newest first."""
    t = unquote(str(text or "")).lower()
    years = re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", t)
    if not years:
        return "0000-00"
    nums = re.findall(r"((?:19|20)\d{2})[\s_\-./](0?[1-9]|1[0-2])(?!\d)", t)
    if nums:
        return max(f"{y}-{int(m):02d}" for y, m in nums)
    months = [i for i, m in enumerate(_MONTHS, 1) if re.search(rf"(?<![a-z]){m}[a-z]*", t)]
    return f"{max(years)}-{max(months):02d}" if months else f"{max(years)}-00"


def crawl_with_fallback(start_urls: list, follow, wanted, **kw) -> dict:
    """Static crawl; if nothing wanted is found, repeat in a headless browser."""
    links = crawl(start_urls, follow, fetch_html_static, **kw)
    if any(wanted(u, v) for u, v in links.items()) or not USE_BROWSER:
        return links
    log.info("Nothing found in static HTML; retrying with a headless browser.")
    try:
        with BrowserFetcher() as browser:
            return crawl(start_urls, follow, browser, **kw)
    except ImportError:
        log.warning("Playwright not installed; skipping browser crawl.")
    except Exception as e:
        log.warning(f"Browser crawl failed: {e}")
    return links


def same_site(start_urls: list):
    hosts = {urlparse(u).netloc.removeprefix("www.") for u in start_urls}
    return lambda url: urlparse(url).netloc.removeprefix("www.") in hosts


def file_ext(url: str) -> str:
    path = unquote(urlparse(url).path).lower()
    return path.rsplit(".", 1)[-1] if "." in path.rsplit("/", 1)[-1] else ""


# ---------------------------------------------------------------------------
# Claude (forced tool use, so output is always structured JSON)
# ---------------------------------------------------------------------------
_CLIENT = None
_LIMIT_RE = re.compile(r"max_tokens: \d+ > (\d+)")


def _client():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY or None, max_retries=3, timeout=600.0)
    return _CLIENT


def call_tool(system: str, content: list, tool: dict, max_tokens: int | None = None) -> dict:
    max_tokens = max_tokens or MAX_OUTPUT_TOKENS

    def _run(limit):
        with _client().messages.stream(
            model=ANTHROPIC_MODEL, max_tokens=limit, system=system, tools=[tool],
            tool_choice={"type": "tool", "name": tool["name"]},
            messages=[{"role": "user", "content": content}],
        ) as stream:
            return stream.get_final_message()

    try:
        resp = _run(max_tokens)
    except anthropic.BadRequestError as e:
        m = _LIMIT_RE.search(str(e))
        if not m:
            raise
        max_tokens = int(m.group(1))
        log.warning(f"Output limit above model maximum; retrying with {max_tokens}.")
        resp = _run(max_tokens)

    STATS["claude_input_tokens"] += resp.usage.input_tokens
    STATS["claude_output_tokens"] += resp.usage.output_tokens
    if resp.stop_reason == "max_tokens":
        raise RuntimeError(f"Claude output was cut off at {max_tokens} tokens; nothing from this item was staged.")
    for block in resp.content:
        if block.type == "tool_use" and block.name == tool["name"]:
            return block.input
    raise RuntimeError("Claude did not return structured output.")


# ---------------------------------------------------------------------------
# Staging (the MD review queue)
# ---------------------------------------------------------------------------
def make_key(*parts) -> str:
    """Stable fingerprint so the endpoint can ignore records it has already received."""
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()


def post_staging(target_table: str, items: list, collector: str, attempts: int = 3) -> int:
    """items: [{"proposed_data": {...}, "source_url": str, "dedupe_key": str}]"""
    if not items:
        return 0
    payload = [{
        "target_table": target_table,
        "proposed_data": it["proposed_data"],
        "source_url": it.get("source_url"),
        "dedupe_key": it["dedupe_key"],
        "submitted_by": f"pipeline:{collector}",
    } for it in items]

    if DRY_RUN:
        log.info(f"[dry run] would post {len(payload)} '{target_table}' record(s). First one:")
        log.info(json.dumps(payload[0], ensure_ascii=False, default=str)[:1500])
        return len(payload)

    headers = {"Content-Type": "application/json", "x-ingestion-key": INGESTION_SECRET_KEY}
    for start in range(0, len(payload), 500):
        chunk = payload[start:start + 500]
        for attempt in range(1, attempts + 1):
            try:
                r = SESSION.post(APP_INGESTION_URL, json=chunk, headers=headers, timeout=60)
            except requests.RequestException as e:
                log.warning(f"Network error posting to staging: {e}")
            else:
                if 200 <= r.status_code < 300:
                    log.info(f"Staging accepted {len(chunk)} '{target_table}' record(s): {r.text[:300]}")
                    break
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    raise RuntimeError(f"Staging rejected '{target_table}' (HTTP {r.status_code}): {r.text[:500]}")
                log.warning(f"Staging returned HTTP {r.status_code}: {r.text[:300]}")
            time.sleep(2 ** attempt)
        else:
            raise RuntimeError(f"Could not post '{target_table}' records after {attempts} attempts.")
    return len(payload)


def write_summary(markdown: str) -> None:
    """Shows on the GitHub Actions run page."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(markdown + "\n")
