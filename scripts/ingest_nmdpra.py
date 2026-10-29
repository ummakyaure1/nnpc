#!/usr/bin/env python3
"""
NMDPRA reports collector.

Renders each configured NMDPRA report page in a headless browser (the site is built with
JavaScript), lists the documents and background data it finds, and for pages marked
"stage": true sends the newest documents to the MD's bulletin as 'news' items of type
'report', with a short summary and key figures. Key figures are kept only when their
supporting text is found in the PDF.
"""
import io
import json
import sys
from urllib.parse import unquote, urlparse

import pandas as pd

import pipeline_common as pc

log = pc.log
COLLECTOR = "nmdpra"
DOC_EXT = {"pdf", "xlsx", "xls", "csv", "doc", "docx"}

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


def doc_title(url: str, anchor: str) -> str:
    if anchor and len(anchor) >= 8 and anchor.lower() not in ("download", "view", "click here", "read more"):
        return anchor
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
    return name.replace("_", " ").replace("-", " ").strip() or url


def describe_json(body: bytes) -> str:
    try:
        obj = json.loads(body.decode("utf-8", "ignore"))
    except ValueError:
        return "not JSON"
    if isinstance(obj, dict):
        lists = [v for v in obj.values() if isinstance(v, list) and v and isinstance(v[0], dict)]
        obj = max(lists, key=len) if lists else obj
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        df = pd.json_normalize(obj)
        return f"{len(df)} row(s), columns {list(df.columns)[:15]}"
    return f"JSON {type(obj).__name__}"


def summarise_pdf(pdf: bytes) -> dict:
    first = pc.pdf_chunks(pdf)[0][1]
    out = pc.call_tool("You summarise regulatory reports using only the document provided. Never add outside facts.",
                       [pc.pdf_document_block(first), {"type": "text", "text": "Summarise this report and list up to 8 headline figures."}],
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


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config()["nmdpra"]
    per_page = int(cfg.get("max_docs_per_page", 2))
    items, failures = [], 0
    summary_lines = []

    for page in cfg["pages"]:
        log.info(f"----- {page['name']}: {page['url']}")
        html, captured = pc.render_page(page["url"])
        if not html:
            log.warning("  page did not load")
            summary_lines.append(f"- {page['name']}: page did not load")
            continue
        docs = {}
        for url, text in pc.page_links(html, page["url"]):
            if pc.file_ext(url) in DOC_EXT and url not in docs:
                docs[url] = doc_title(url, text)
        ranked = sorted(docs.items(), key=lambda d: (pc.guess_period(d[1] + " " + d[0]), d[0]), reverse=True)
        log.info(f"  {len(ranked)} document link(s); {len(captured)} background data response(s)")
        for url, title in ranked[:10]:
            log.info(f"    [{pc.guess_period(title + ' ' + url)}] {title[:80]} | {url}")
        for c in captured[:8]:
            log.info(f"    data: {len(c['body']) / 1024:.1f} KB {c['url'][:120]} -> {describe_json(c['body'])}")
        summary_lines.append(f"- {page['name']}: {len(ranked)} document(s), {len(captured)} data feed(s)"
                             f"{'' if page.get('stage') else ' (listed only)'}")

        if not page.get("stage") or pc.DRY_RUN:
            continue
        for url, title in ranked[:per_page]:
            try:
                data = {"item_type": "report", "published_at": None, "source_name": f"NMDPRA - {page['name']}",
                        "url": url, "headline": title, "summary": None, "key_figures": [],
                        "tags": {"assets": [], "companies": [], "regulators": ["NMDPRA"]},
                        "mentions_target_asset": False, "period_guess": pc.guess_period(title + " " + url),
                        "source_url": url}
                if pc.file_ext(url) == "pdf":
                    pdf = pc.fetch_pdf(url)
                    s = summarise_pdf(pdf)
                    titles = pc.titles_in(pc.pdf_text(pdf))
                    data.update({"headline": s.get("title") or title, "summary": s.get("summary"),
                                 "period": s.get("period"), "key_figures": s.get("key_figures"),
                                 "extraction_model": pc.ANTHROPIC_MODEL})
                    data["tags"]["assets"] = titles
                    data["mentions_target_asset"] = any(t in pc.TARGET_ASSETS for t in titles)
                elif pc.file_ext(url) in ("xlsx", "xls"):
                    sheets = pd.read_excel(io.BytesIO(pc.fetch(url).content), sheet_name=None, nrows=0)
                    data["summary"] = f"Spreadsheet with sheets: {', '.join(list(sheets)[:10])}."
                items.append({"proposed_data": data, "source_url": url, "dedupe_key": pc.make_key("news", url)})
                log.info(f"  prepared: {data['headline'][:90]}")
            except Exception as e:
                failures += 1
                log.warning(f"  failed on {url}: {e}")

    pc.post_staging("news", items, COLLECTOR)
    pc.write_summary("### NMDPRA\n" + "\n".join(summary_lines) + f"\n- Reports sent to the bulletin: {len(items)}"
                     + (f"\n- Failed: {failures}" if failures else ""))
    log.info(f"NMDPRA done: {len(items)} report(s) prepared, {failures} failure(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
