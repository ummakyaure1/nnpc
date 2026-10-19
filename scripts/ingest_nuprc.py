#!/usr/bin/env python3
"""
scripts/ingest_nuprc.py

Automated ingestion script to scrape NUPRC monthly crude & gas production PDFs,
extract structured production data using Anthropic Claude, and push parsed
records into a Supabase staging table `unverified_staging` for review.

Environment variables required:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- ANTHROPIC_API_KEY

Dependencies (install via pip): requests, pypdf, anthropic, supabase

"""
import os
import re
import json
import logging
import tempfile
import traceback
from typing import List, Dict, Optional

import requests
from pypdf import PdfReader
from supabase import create_client
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Read environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not ANTHROPIC_API_KEY:
    logger.error("Missing required environment variables. Make sure SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY and ANTHROPIC_API_KEY are set.")
    raise SystemExit(1)

# Initialize clients
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
anthropic_client = anthropic.Client(api_key=ANTHROPIC_API_KEY)

BASE_NUPRC_URL = "https://www.nuprc.gov.ng"


def _absolute_url(href: str, base: str = BASE_NUPRC_URL) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # relative URL
    return base.rstrip("/") + "/" + href.lstrip("/")


def fetch_latest_nuprc_pdf_url(target_url: Optional[str] = None) -> Optional[str]:
    """Scrape the NUPRC website to find the latest monthly crude & gas production PDF.

    If target_url is provided it will be returned unchanged (after a HEAD check).
    Otherwise the function fetches the NUPRC homepage and searches for PDF links that
    look like production reports.
    """
    try:
        if target_url:
            logger.info("Using provided target URL: %s", target_url)
            # Basic HEAD check
            r = requests.head(target_url, allow_redirects=True, timeout=20)
            if r.status_code // 100 == 2:
                return target_url
            else:
                logger.warning("Provided target URL returned status %s", r.status_code)
                return target_url

        logger.info("Fetching NUPRC homepage to discover latest production PDF")
        r = requests.get(BASE_NUPRC_URL, timeout=20)
        r.raise_for_status()
        html = r.text

        # Find all pdf links
        hrefs = re.findall(r'href=["\']([^"\']+\.pdf)["\']', html, flags=re.IGNORECASE)
        logger.debug("Found %d PDF hrefs", len(hrefs))

        # Filter likely production reports by keywords
        keywords = ("production", "crude", "gas", "monthly", "report")
        candidates = [h for h in hrefs if any(k in h.lower() for k in keywords)]

        # If none matched by keywords, fall back to any PDF links
        if not candidates:
            candidates = hrefs

        if not candidates:
            logger.error("No PDF links found on %s", BASE_NUPRC_URL)
            return None

        # Prefer full URLs and those that have a year-like substring
        def score_href(h: str) -> int:
            score = 0
            if h.startswith("http"):
                score += 2
            if re.search(r"20\d{2}|19\d{2}", h):
                score += 1
            return score

        candidates.sort(key=score_href, reverse=True)
        chosen = _absolute_url(candidates[0])
        logger.info("Chose PDF: %s", chosen)
        return chosen

    except Exception:
        logger.error("Error while fetching NUPRC PDF URL:\n%s", traceback.format_exc())
        return None


def _download_pdf_text(url: str) -> Optional[str]:
    """Download a PDF and extract concatenated text from all pages."""
    try:
        logger.info("Downloading PDF from %s", url)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            tf.write(r.content)
            tmp_path = tf.name
        logger.info("Saved PDF to %s", tmp_path)

        reader = PdfReader(tmp_path)
        texts = []
        for i, page in enumerate(reader.pages):
            try:
                page_text = page.extract_text() or ""
                texts.append(page_text)
            except Exception:
                logger.warning("Failed to extract text from page %s", i)
        combined = "\n".join(texts)
        return combined
    except Exception:
        logger.error("Error downloading/extracting PDF text:\n%s", traceback.format_exc())
        return None


def extract_production_data_with_claude(pdf_text_or_url: str, source_url: Optional[str] = None) -> List[Dict]:
    """Call Anthropic Claude to parse the PDF text/tables and return structured JSON records.

    Returns a list of records with fields:
      - title_number (string, e.g., "OML 111")
      - period (string YYYY-MM)
      - oil_bpd (number)
      - gas_mmscf_d (number)
      - source_url (string)
    """
    try:
        if pdf_text_or_url.startswith("http://") or pdf_text_or_url.startswith("https://"):
            source_url = source_url or pdf_text_or_url
            text = _download_pdf_text(pdf_text_or_url)
        else:
            text = pdf_text_or_url

        if not text:
            logger.error("No text available to send to Claude")
            return []

        # Create a structured prompt asking for JSON array output only
        human_instructions = (
            "You are given the extracted text from a Nigerian Upstream Petroleum Regulatory Commission (NUPRC) monthly crude and gas production report PDF.\n"
            "Extract a JSON array (and ONLY the JSON array) of objects where each object contains the following fields:\n"
            "- title_number: string (example: 'OML 111', 'OML 65', 'OML 130')\n"
            "- period: string formatted as 'YYYY-MM' (the year and month the production relates to)\n"
            "- oil_bpd: numeric (barrels per day; use null if not found)\n"
            "- gas_mmscf_d: numeric (million standard cubic feet per day; use null if not found)\n"
            "- source_url: string (the PDF URL or source page)\n\n"
            "If a value is not available, set it to null. Ensure output is valid JSON parsable by standard JSON parsers.\n\n"
            "Here is the source_url to fill into each object: " + (source_url or "UNKNOWN") + "\n\n"
            "Now extract the records from the following text. Return ONLY the JSON array and nothing else.\n\n"
            "=== BEGIN SOURCE TEXT ===\n"
        )

        prompt_body = human_instructions + (text[:38000]) + "\n\n=== END SOURCE TEXT ===\n"

        # Build the full prompt using Anthropics HUMAN/A I delimiters if available
        try:
            prompt = f"{anthropic.HUMAN_PROMPT}\n{prompt_body}\n{anthropic.AI_PROMPT}"
        except Exception:
            # Fallback if constants are not present
            prompt = prompt_body

        logger.info("Sending text to Claude for structured extraction (truncated to 38k chars)")
        resp = anthropic_client.completions.create(
            model="claude-3-5-sonnet-20241022",
            prompt=prompt,
            max_tokens_to_sample=2000,
        )

        # Extract the completion text (supports multiple response shapes)
        completion_text = None
        if isinstance(resp, dict):
            completion_text = resp.get("completion") or resp.get("text") or resp.get("result")
        else:
            # Object with attribute
            completion_text = getattr(resp, "completion", None) or getattr(resp, "text", None)

        if not completion_text:
            logger.error("No completion text returned from Claude")
            return []

        logger.debug("Raw Claude output: %s", completion_text[:1000])

        # Try to find a JSON array in the completion
        m = re.search(r"(\[.*\])", completion_text, flags=re.DOTALL)
        json_text = None
        if m:
            json_text = m.group(1)
        else:
            # As a fallback, try to parse the entire completion
            json_text = completion_text.strip()

        # Attempt JSON parsing, with some cleanup
        try:
            records = json.loads(json_text)
            # Ensure records is a list
            if isinstance(records, dict):
                records = [records]
        except Exception:
            # Try to fix common issues: replace trailing commas
            try:
                cleaned = re.sub(r",\s*\]", "]", json_text)
                cleaned = re.sub(r",\s*\}", "}", cleaned)
                records = json.loads(cleaned)
                if isinstance(records, dict):
                    records = [records]
            except Exception:
                logger.error("Failed to parse JSON from Claude output:\n%s", traceback.format_exc())
                return []

        # Normalize records: ensure required keys and types
        normalized = []
        for r in records:
            nr = {
                "title_number": r.get("title_number") if isinstance(r.get("title_number"), str) else (r.get("title") or r.get("field") or None),
                "period": r.get("period") or None,
                "oil_bpd": r.get("oil_bpd") if isinstance(r.get("oil_bpd"), (int, float)) else _coerce_number(r.get("oil_bpd")),
                "gas_mmscf_d": r.get("gas_mmscf_d") if isinstance(r.get("gas_mmscf_d"), (int, float)) else _coerce_number(r.get("gas_mmscf_d")),
                "source_url": r.get("source_url") or source_url or None,
            }
            normalized.append(nr)

        logger.info("Extracted %d records from Claude output", len(normalized))
        return normalized

    except Exception:
        logger.error("Error during extraction with Claude:\n%s", traceback.format_exc())
        return []


def _coerce_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        # Remove commas and non-numeric characters except dot and minus
        s = re.sub(r"[^0-9.\-]", "", v)
        try:
            if s == "":
                return None
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            return None
    return None


def push_to_staging(records: List[Dict]) -> None:
    """Insert each record into Supabase table `unverified_staging` with status pending."""
    for rec in records:
        try:
            payload = {
                "target_table": "production",
                "proposed_data": rec,
                "source_url": rec.get("source_url"),
                "status": "pending",
            }
            logger.info("Inserting record for %s period=%s", rec.get("title_number"), rec.get("period"))
            resp = supabase.table("unverified_staging").insert(payload).execute()
            # supabase-py returns a tuple-like object in some versions, or a dict
            logger.debug("Supabase insert response: %s", getattr(resp, "data", resp))
        except Exception:
            logger.error("Failed to insert record into Supabase:\n%s", traceback.format_exc())


def run_once(target_pdf_url: Optional[str] = None) -> None:
    try:
        pdf_url = fetch_latest_nuprc_pdf_url(target_pdf_url)
        if not pdf_url:
            logger.error("No PDF URL available. Exiting.")
            return

        records = extract_production_data_with_claude(pdf_url, source_url=pdf_url)
        if not records:
            logger.warning("No records extracted from PDF. Exiting.")
            return

        push_to_staging(records)
        logger.info("Ingestion run completed. Inserted %d records.", len(records))

    except Exception:
        logger.error("Uncaught error in run_once:\n%s", traceback.format_exc())


if __name__ == "__main__":
    # Optional TARGET_PDF_URL env var to override scraping
    target = os.getenv("TARGET_PDF_URL")
    run_once(target_pdf_url=target)
