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

DEFAULT_PDF_URLS = [
    "https://www.nuprc.gov.ng/wp-content/uploads/2026/04/Production_Report.pdf",
]
_url_override = os.environ.get("NUPRC_PDF_URLS", "").strip()
PDF_URLS = [u.strip() for u in _url_override.split(",") if u.strip()] or DEFAULT_PDF_URLS

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
- One record per licence per reporting month.
- oil_bpd and gas_mmscf_d must be daily rates as printed. Do NOT convert monthly or
  cumulative volumes; leave the field null and describe what was shown in "notes".
- If the report only gives terminal or stream totals (not per-OML figures), do not
  attribute them to an OML.
- "evidence" must be copied from the document, not paraphrased.

Call the record_production_figures tool with your results."""


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
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY or None, max_retries=3, timeout=180.0)


def extract_with_claude(document_block: dict) -> list:
    log.info(f"Calling Claude model: {ANTHROPIC_MODEL}")
    response = claude.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        tools=[EXTRACTION_TOOL],
        tool_choice={"type": "tool", "name": EXTRACTION_TOOL["name"]},
        messages=[{
            "role": "user",
            "content": [document_block, {"type": "text", "text": USER_PROMPT}],
        }],
    )

    if response.stop_reason == "max_tokens":
        log.warning("Claude hit max_tokens; extraction may be incomplete.")

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

    total, failures = 0, []
    for url in PDF_URLS:
        try:
            total += process_url(url)
        except anthropic.NotFoundError as e:
            log.error(f"Model '{ANTHROPIC_MODEL}' not found. Set the ANTHROPIC_MODEL repo variable. ({e})")
            return 1
        except Exception as e:
            log.exception(f"Failed processing {url}: {e}")
            failures.append(url)

    log.info(f"Done. {total} records staged from {len(PDF_URLS) - len(failures)}/{len(PDF_URLS)} documents.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
