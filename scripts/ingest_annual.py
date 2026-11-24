#!/usr/bin/env python3
"""
Annual NUPRC reports collector.

Reads NUPRC's yearly reports (gas production status reports, annual financial & operational
performance reports) and proposes their tables as 'production_raw' rows at the level the
report uses: field, company, terminal, licence or national. Field rows link to blocks in
the app through the field-name dictionary.

Figures are copied as printed by the AI and converted to daily rates in code. Rows are kept
only when their text is found in the PDF (if the PDF has a text layer).
"""
import hashlib
import re
import sys

import pipeline_common as pc
import units

log = pc.log
COLLECTOR = "annual"

ENTITY_TYPES = ["licence", "field", "terminal", "crude_stream", "company", "region", "national", "other"]
MEASURES = ["crude_oil", "condensate", "oil_and_condensate", "gas_produced", "gas_utilised",
            "gas_flared", "gas_other"]
OIL_MEASURES = {"crude_oil", "condensate", "oil_and_condensate"}

TOOL = {
    "name": "record_tables",
    "description": "Record every production or gas table row in this part of the report.",
    "input_schema": {
        "type": "object",
        "properties": {
            "report_title": {"type": "string"},
            "rows": {"type": "array", "items": {"type": "object", "properties": {
                "entity_type": {"type": "string", "enum": ENTITY_TYPES},
                "entity_name": {"type": "string", "description": "Row label as printed (field, company, terminal...)."},
                "licence": {"type": ["string", "null"], "description": "OML/OPL/PML as printed, only if shown for the row."},
                "operator": {"type": ["string", "null"]},
                "measure": {"type": "string", "enum": MEASURES},
                "measure_label": {"type": "string", "description": "Column or row label as printed."},
                "unit": {"type": "string", "description": "Unit as printed, e.g. 'bbl', 'MMscf', 'bopd'."},
                "values": {"type": "object", "additionalProperties": {"type": "number"},
                           "description": "Keys: 'YYYY' for yearly figures or 'YYYY-MM' for monthly ones."},
                "page": {"type": ["integer", "null"]},
                "evidence": {"type": "string", "description": "The row's text copied exactly from the document."},
            }, "required": ["entity_type", "entity_name", "measure", "measure_label", "unit", "values", "evidence"]}},
        },
        "required": ["report_title", "rows"],
    },
}

SYSTEM = ("You transcribe regulator statistics tables exactly as printed. Never estimate, convert or add numbers. "
          "Skip percentages, rankings and narrative text.")
INSTRUCTION = ("Transcribe every table of oil, condensate or gas production, utilisation or flaring in this part, "
               "at the level the table uses (field, company, terminal, licence or national). One entry per table "
               "row, with all of its years or months in 'values'. Skip totals that only repeat other rows. "
               "Copy the row's text as evidence.")
PERIOD_RE = re.compile(r"^\d{4}(-(0[1-9]|1[0-2]))?$")


def expand(rows, url, sha, title):
    out, seen, dropped = [], set(), 0
    return_rows = []
    for row in rows:
        etype = row.get("entity_type") if row.get("entity_type") in ENTITY_TYPES else "other"
        measure = row.get("measure")
        name = str(row.get("entity_name") or "").strip()
        if not name or measure not in MEASURES:
            continue
        licence = pc.normalise_title(f"{row.get('licence') or ''} {name if etype == 'licence' else ''}")
        is_oil = measure in OIL_MEASURES
        for period, raw in (row.get("values") or {}).items():
            period = str(period).strip()
            if not PERIOD_RE.match(period):
                continue
            rate, note = units.to_daily(raw, row.get("unit"), units.OIL_UNITS if is_oil else units.GAS_UNITS,
                                        units.period_days(period))
            key = (etype, name.lower(), measure, str(row.get("measure_label")).lower(), period)
            if key in seen:
                continue
            seen.add(key)
            return_rows.append({
                "entity_type": etype, "entity_name": name, "licence": licence,
                "is_target_asset": licence in pc.TARGET_ASSETS if licence else False,
                "operator": row.get("operator"), "measure": measure, "measure_label": row.get("measure_label"),
                "period": period, "period_granularity": "year" if len(period) == 4 else "month",
                "oil_bpd": rate if is_oil else None, "gas_mmscf_d": None if is_oil else rate,
                "value_as_printed": units.to_number(raw), "unit_as_printed": row.get("unit"),
                "conversion_notes": note, "report_title": title, "source_url": url, "source_page": row.get("page"),
                "source_sha256": sha, "evidence": row.get("evidence"), "evidence_verified": row.get("_verified"),
                "source_name": "NUPRC", "extraction_model": pc.ANTHROPIC_MODEL,
            })
    for r in return_rows:
        out.append({"proposed_data": r, "source_url": url,
                    "dedupe_key": pc.make_key("annual", sha, r["entity_type"], r["entity_name"].lower(), r["measure"],
                                              str(r["measure_label"]).lower(), r["period"])})
    return out


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config().get("annual") or {}
    index = cfg.get("reports_index", "https://www.nuprc.gov.ng/reports")
    reports = pc.discover_reports(index, cfg.get("series_keywords", ["gas-production-status-report",
                                                                      "annual-financial-operational"]),
                                  int(cfg.get("max_reports_per_series", 2)), year_only=True)
    if not reports:
        log.error("No annual NUPRC reports found. Run with dry_run and check the crawl log.")
        return 1
    if pc.DRY_RUN:
        log.info("DRY_RUN: discovery only.")
        return 0

    items, failures = [], 0
    for url, series in reports:
        try:
            pdf = pc.fetch_pdf(url)
            sha = hashlib.sha256(pdf).hexdigest()
            if pc.already_seen(source_sha256=sha):
                log.info(f"Already received {url}; skipping.")
                continue
            text = pc.pdf_text(pdf)
            head, rows = pc.extract_chunked(pdf, SYSTEM, INSTRUCTION, TOOL, "rows")
            kept = []
            for r in rows:
                r["_verified"] = pc.verified(text, r.get("evidence"))
                if r["_verified"] is not False:
                    kept.append(r)
            if len(kept) < len(rows):
                log.warning(f"  dropped {len(rows) - len(kept)} row(s) whose text was not found in the PDF")
            found = expand(kept, url, sha, head.get("report_title"))
            by_type = {}
            for f in found:
                by_type[f["proposed_data"]["entity_type"]] = by_type.get(f["proposed_data"]["entity_type"], 0) + 1
            log.info(f"{head.get('report_title')}: {len(kept)} table row(s) -> {len(found)} yearly/monthly record(s) {by_type}")
            items += found
        except Exception as e:
            failures += 1
            log.warning(f"Failed on {url}: {e}")

    pc.post_staging("production_raw", items, COLLECTOR)
    fields = sum(1 for i in items if i["proposed_data"]["entity_type"] == "field")
    pc.write_summary(f"### Annual NUPRC reports\n- Reports read: {len(reports) - failures}/{len(reports)}\n"
                     f"- Records proposed: {len(items)} ({fields} at field level)")
    log.info(f"Annual done: {len(items)} record(s), {failures} failure(s).")
    return 1 if failures and not items else 0


if __name__ == "__main__":
    sys.exit(main())
