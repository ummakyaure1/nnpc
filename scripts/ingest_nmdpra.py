#!/usr/bin/env python3
"""
NMDPRA reports collector.

NMDPRA's website lists its documents through a public feed:
  https://www.nmdpra.gov.ng/api/General/Getdocuments?t=<category>
This collector reads that feed for each configured page. For pages marked "stage": true it
sends the newest documents to the MD's bulletin as 'news' items of type 'report', with a
short summary and headline figures (each figure kept only if its text is found in the PDF).
Pages marked "stage": false are only listed in the log.
"""
import io
import sys
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import pandas as pd

import pipeline_common as pc

log = pc.log
COLLECTOR = "nmdpra"
SITE = "https://www.nmdpra.gov.ng/"
DEFAULT_API = "https://www.nmdpra.gov.ng/api/General/Getdocuments"

SUMMARY_TOOL = {
    "name": "summarise_report",
    "description": "Summarise a regulator's report and list its headline figures.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "period": {"type": ["string", "null"], "description": "Period covered, as printed (e.g. 'August 2026')."},
            "summary": {"type": "string", "description": "At most 60 words, neutral."},
            "key_figures": {"type": "array", "items": {"type": "object", "properties": {
                "label": {"type": "string"}, "value": {"type": "number"}, "unit": {"type": ["string", "null"]},
                "evidence": {"type": "string", "description": "Text copied exactly from the document."}},
                "required": ["label", "value", "evidence"]}},
        },
        "required": ["title", "summary", "key_figures"],
    },
}


# ---------------------------------------------------------------------------
# Feed
# ---------------------------------------------------------------------------
def fetch_category(api_url: str, category: str) -> list:
    url = f"{api_url}?t={quote(category)}"
    try:
        data = pc.fetch(url, attempts=2, timeout=60).json()
    except Exception as e:
        log.warning(f"  feed '{category}' unavailable: {e}")
        return []
    if isinstance(data, dict):
        lists = [v for v in data.values() if isinstance(v, list)]
        data = max(lists, key=len) if lists else []
    return data if isinstance(data, list) else []


def discover_category(page_url: str):
    """Open the page in a browser and read which feed category it requests."""
    _, captured = pc.render_page(page_url, wait_ms=15000)
    for c in captured:
        if "getdocuments" in c["url"].lower():
            t = parse_qs(urlparse(c["url"]).query).get("t", [None])[0]
            if t:
                log.info(f"  page requests feed category '{t}'")
                return t
    return None


def doc_url(d: dict):
    for key in ("documentUrl", "documentUpload"):
        v = d.get(key)
        if isinstance(v, str) and v.strip() and len(v) < 2000 and not v.strip().startswith("data:"):
            return urljoin(SITE, v.strip())
    return None


def usable(d: dict) -> bool:
    return not any(d.get(k) for k in ("isHidden", "isDeleted", "isRequiredPasswordForDownload",
                                       "isRequiredPaymentForDownload")) and bool(doc_url(d))


def doc_date(d: dict):
    for key in ("displayDate", "addedDate"):
        ts = pd.to_datetime(d.get(key), errors="coerce", utc=True)
        if not pd.isna(ts):
            return ts
    return None


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------
def summarise_pdf(pdf: bytes) -> dict:
    first = pc.pdf_chunks(pdf)[0][1]
    out = pc.call_tool("You summarise regulatory reports using only the document provided. Never add outside facts.",
                       [pc.pdf_document_block(first),
                        {"type": "text", "text": "Summarise this report and list up to 8 headline figures."}],
                       SUMMARY_TOOL, max_tokens=3000)
    text = pc.pdf_text(pdf)
    figures = []
    for f in out.get("key_figures") or []:
        if text and not pc.evidence_found(text, f.get("evidence", "")):
            log.warning(f"  dropped figure '{f.get('label')}': its text was not found in the PDF")
            continue
        figures.append({**f, "evidence_verified": bool(text)})
    out["key_figures"] = figures
    return out


def build_item(page: dict, d: dict) -> dict:
    url = doc_url(d)
    ts = doc_date(d)
    data = {"item_type": "report", "source_name": f"NMDPRA - {page['name']}", "url": url,
            "headline": (d.get("name") or "").strip() or unquote(urlparse(url).path.rsplit("/", 1)[-1]),
            "published_at": ts.date().isoformat() if ts is not None else None,
            "summary": (d.get("detailsText") or "").strip() or None, "key_figures": [],
            "tags": {"assets": [], "companies": [], "regulators": ["NMDPRA"]},
            "mentions_target_asset": False, "nmdpra_doc_id": d.get("id"), "source_url": url}
    ext = pc.file_ext(url)
    if ext == "pdf" or (not ext and "pdf" in str(d.get("documentType", "")).lower()):
        pdf = pc.fetch_pdf(url)
        s = summarise_pdf(pdf)
        titles = pc.titles_in(pc.pdf_text(pdf))
        data.update({"summary": s.get("summary") or data["summary"], "period": s.get("period"),
                     "key_figures": s.get("key_figures"), "extraction_model": pc.ANTHROPIC_MODEL,
                     "mentions_target_asset": any(t in pc.TARGET_ASSETS for t in titles)})
        data["tags"]["assets"] = titles
    elif ext in ("xlsx", "xls"):
        sheets = pd.read_excel(io.BytesIO(pc.fetch(url).content), sheet_name=None, nrows=0)
        data["summary"] = data["summary"] or f"Spreadsheet with sheets: {', '.join(list(sheets)[:10])}."
    return {"proposed_data": data, "source_url": url,
            "dedupe_key": pc.make_key("news", "nmdpra", d.get("id") or url)}


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config().get("nmdpra") or {}
    if not cfg.get("api_url"):
        log.warning("sources.json has no NMDPRA feed address; using the default. Upload the latest sources.json.")
    api_url = cfg.get("api_url") or DEFAULT_API
    if not cfg.get("pages"):
        log.error("sources.json has no NMDPRA pages configured. Upload the latest sources.json.")
        return 1
    per_page = int(cfg.get("max_docs_per_page", 2))
    items, failures, lines = [], 0, []

    for page in cfg["pages"]:
        log.info(f"----- {page['name']}")
        docs, used = [], None
        for cat in page.get("categories", []):
            docs = fetch_category(api_url, cat)
            if docs:
                used = cat
                break
        if not docs and pc.USE_BROWSER:
            cat = discover_category(page["url"])
            if cat:
                docs, used = fetch_category(api_url, cat), cat
        docs = sorted((d for d in docs if isinstance(d, dict) and usable(d)),
                      key=lambda d: doc_date(d) or pd.Timestamp(0, tz="UTC"), reverse=True)
        log.info(f"  feed '{used}': {len(docs)} downloadable document(s)")
        for d in docs[:5]:
            ts = doc_date(d)
            log.info(f"    {ts.date() if ts is not None else '????-??-??'}  {str(d.get('name'))[:80]} | {doc_url(d)}")
        lines.append(f"- {page['name']}: {len(docs)} document(s){'' if page.get('stage') else ' (listed only)'}")

        if not page.get("stage") or pc.DRY_RUN:
            continue
        for d in docs[:per_page]:
            if pc.already_seen(dedupe_key=pc.make_key("news", "nmdpra", d.get("id") or doc_url(d))):
                log.info(f"  already sent: {str(d.get('name'))[:80]}")
                continue
            try:
                items.append(build_item(page, d))
                log.info(f"  prepared: {items[-1]['proposed_data']['headline'][:90]}")
            except Exception as e:
                failures += 1
                log.warning(f"  failed on {doc_url(d)}: {e}")

    pc.post_staging("news", items, COLLECTOR)
    pc.write_summary("### NMDPRA\n" + "\n".join(lines) + f"\n- Reports sent to the bulletin: {len(items)}"
                     + (f"\n- Failed: {failures}" if failures else ""))
    log.info(f"NMDPRA done: {len(items)} report(s) prepared, {failures} failure(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
