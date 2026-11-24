#!/usr/bin/env python3
"""
Rig activity collector.

Reads NUPRC's rig disposition report (monthly) and proposes 'rig_activity' records:
which rig is doing what, for which operator, on which well, field and licence.
Rows are kept only if their text is found in the PDF (when the PDF has a text layer).
"""
import hashlib
import sys

import pipeline_common as pc

log = pc.log
COLLECTOR = "rigs"

ACTIVITIES = ["drilling", "completion", "workover", "intervention", "testing", "moving", "standby",
              "idle", "stacked", "maintenance", "other"]

RIG_TOOL = {
    "name": "record_rigs",
    "description": "Record every rig listed in the rig disposition report.",
    "input_schema": {
        "type": "object",
        "properties": {
            "report_title": {"type": "string"},
            "as_of": {"type": ["string", "null"], "description": "Date the report describes, YYYY-MM-DD if printed."},
            "rigs": {"type": "array", "items": {"type": "object", "properties": {
                "rig_name": {"type": "string"},
                "rig_type": {"type": ["string", "null"], "description": "As printed: land, swamp barge, jack-up, drillship..."},
                "contractor": {"type": ["string", "null"]},
                "operator": {"type": ["string", "null"]},
                "well_name": {"type": ["string", "null"]},
                "field": {"type": ["string", "null"]},
                "title_number": {"type": ["string", "null"], "description": "OML/OPL/PML/PPL as printed, only if shown."},
                "activity": {"type": "string", "enum": ACTIVITIES},
                "activity_as_printed": {"type": ["string", "null"]},
                "terrain": {"type": ["string", "null"]},
                "page": {"type": ["integer", "null"]},
                "evidence": {"type": "string", "description": "The row's text copied exactly from the document."},
            }, "required": ["rig_name", "activity", "evidence"]}},
        },
        "required": ["report_title", "rigs"],
    },
}

SYSTEM = ("You transcribe regulator rig reports exactly as printed. Never add facts from your own knowledge; "
          "use null when a value is not printed.")


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config().get("rigs") or {}
    index = cfg.get("reports_index", "https://www.nuprc.gov.ng/reports")
    reports = pc.discover_reports(index, cfg.get("series_keywords", ["rig-disposition", "rig disposition"]),
                                  int(cfg.get("max_reports", 1)))
    if not reports:
        log.error("No rig disposition reports found. Run with dry_run and check the crawl log.")
        return 1
    if pc.DRY_RUN:
        log.info("DRY_RUN: discovery only.")
        return 0

    items, failures = [], 0
    for url, _series in reports:
        try:
            pdf = pc.fetch_pdf(url)
            sha = hashlib.sha256(pdf).hexdigest()
            if pc.already_seen(source_sha256=sha):
                log.info(f"Already received {url}; skipping.")
                continue
            text = pc.pdf_text(pdf)
            head, rigs = pc.extract_chunked(pdf, SYSTEM, "Transcribe every rig row. Copy each row's text as evidence.",
                                            RIG_TOOL, "rigs")
            log.info(f"{head.get('report_title')} | as of {head.get('as_of')} | {len(rigs)} rig row(s)")
            dropped = 0
            for r in rigs:
                ok = pc.verified(text, r.get("evidence"))
                if ok is False:
                    dropped += 1
                    continue
                title = pc.normalise_title(r.get("title_number"))
                data = {**{k: v for k, v in r.items() if v not in (None, "")},
                        "title_number": title, "is_target_asset": title in pc.TARGET_ASSETS if title else False,
                        "as_of": head.get("as_of"), "report_title": head.get("report_title"),
                        "evidence_verified": ok, "source_sha256": sha, "source_name": "NUPRC",
                        "source_url": url, "suggested_grade": "A", "extraction_model": pc.ANTHROPIC_MODEL}
                items.append({"proposed_data": data, "source_url": url,
                              "dedupe_key": pc.make_key("rig", sha, r.get("rig_name"), r.get("well_name"), title)})
            if dropped:
                log.warning(f"  dropped {dropped} row(s) whose text was not found in the PDF")
        except Exception as e:
            failures += 1
            log.warning(f"Failed on {url}: {e}")

    pc.post_staging("rig_activity", items, COLLECTOR)
    with_title = sum(1 for i in items if i["proposed_data"].get("title_number"))
    pc.write_summary(f"### Rig activity\n- Reports read: {len(reports) - failures}/{len(reports)}\n"
                     f"- Rig rows proposed: {len(items)} ({with_title} with a licence number)")
    log.info(f"Rigs done: {len(items)} row(s), {failures} failure(s).")
    return 1 if failures and not items else 0


if __name__ == "__main__":
    sys.exit(main())
