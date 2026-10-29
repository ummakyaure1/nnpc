#!/usr/bin/env python3
"""
Licence register collector (the OMLs).

Reads NUPRC acreage / licensing reports and proposes register details for each title:
  - 'assets'          status, type, operator, contract type, area, terrain, dates
  - 'asset_partners'  each partner and its equity share, where the report lists them
Scope is the 11 pilot titles by default ("scope": "all" in sources.json widens it).
"""
import hashlib
import re
import sys
from urllib.parse import unquote, urlparse

import pipeline_common as pc

log = pc.log
COLLECTOR = "assets"

STATUS_MAP = {"produc": "producing", "develop": "development", "explor": "exploration", "shut": "shut_in",
              "relinqu": "relinquished", "revok": "revoked", "conver": "under_conversion"}
TERRAIN_MAP = {"deep": "deep_offshore", "shallow": "shallow_offshore", "swamp": "swamp", "land": "onshore",
               "onshore": "onshore", "inland": "frontier_inland", "frontier": "frontier_inland", "offshore": "shallow_offshore"}
CONTRACT_MAP = {"psc": "PSC", "production sharing": "PSC", "jv": "JV", "joint venture": "JV",
                "service": "service_contract", "sole": "sole_risk", "marginal": "marginal_field"}

TITLES_TOOL = {
    "name": "record_titles",
    "description": "Record register details for each licence row in the report.",
    "input_schema": {
        "type": "object",
        "properties": {
            "report_title": {"type": "string"},
            "as_of": {"type": ["string", "null"], "description": "Date or period the report describes, as printed."},
            "rows": {"type": "array", "items": {"type": "object", "properties": {
                "title_number": {"type": "string", "description": "As printed, e.g. 'OML 18'."},
                "title_type": {"type": ["string", "null"]},
                "operator": {"type": ["string", "null"]},
                "partners": {"type": "array", "items": {"type": "object", "properties": {
                    "company": {"type": "string"}, "interest_pct": {"type": ["number", "null"]}},
                    "required": ["company", "interest_pct"]}},
                "contract_type": {"type": ["string", "null"], "description": "As printed (JV, PSC, ...)."},
                "status": {"type": ["string", "null"], "description": "As printed."},
                "area_km2": {"type": ["number", "null"], "description": "Only if printed in km2."},
                "terrain": {"type": ["string", "null"], "description": "As printed (onshore, swamp, deep offshore...)."},
                "basin": {"type": ["string", "null"]},
                "state": {"type": ["string", "null"]},
                "award_date": {"type": ["string", "null"], "description": "YYYY-MM-DD if printed."},
                "expiry_date": {"type": ["string", "null"], "description": "YYYY-MM-DD if printed."},
                "page": {"type": ["integer", "null"]},
                "evidence": {"type": "string", "description": "Text copied exactly from the row."},
            }, "required": ["title_number", "partners", "evidence"]}},
        },
        "required": ["report_title", "rows"],
    },
}


def _map(value, table):
    v = (value or "").lower()
    return next((out for key, out in table.items() if key in v), None)


def guess_period(url: str) -> str:
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1]).lower()
    years = re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", name)
    return max(years) if years else "0000"


def discover(cfg: dict) -> list:
    index = cfg["reports_index"]
    prefix = urlparse(index).path.rstrip("/")
    on_site = pc.same_site([index])
    kws = [k.lower() for k in cfg["report_keywords"]]

    def wanted(url, v):
        return pc.file_ext(url) == "pdf" and any(k in unquote(f"{url} {v['found_on']}").lower() for k in kws)

    links = pc.crawl_with_fallback([index], lambda u: on_site(u) and urlparse(u).path.startswith(prefix),
                                   wanted, max_pages=40, depth=2)
    series = {}
    for url, v in links.items():
        if wanted(url, v):
            slug = urlparse(v["found_on"]).path.rstrip("/").rsplit("/", 1)[-1] or "reports"
            series.setdefault(slug, []).append(url)
    selected = []
    for slug, urls in sorted(series.items()):
        urls.sort(key=guess_period, reverse=True)
        log.info(f"Series '{slug}': {len(urls)} report(s)")
        for i, u in enumerate(urls[:10], 1):
            log.info(f"  {i:>2}. {u}{'  <- selected' if i <= int(cfg.get('max_reports_per_series', 1)) else ''}")
        selected += urls[: int(cfg.get("max_reports_per_series", 1))]
    return selected


def process_report(url: str, scope: str) -> tuple:
    pdf = pc.fetch_pdf(url)
    text = pc.pdf_text(pdf)
    sha = hashlib.sha256(pdf).hexdigest()
    focus = ("Only record rows for these titles: " + ", ".join(pc.TARGET_ASSETS)) if scope == "pilots" \
        else "Record every licence row."
    out = pc.call_tool(
        "You transcribe licence registers exactly as printed. Never add facts from your own knowledge; "
        "use null when a value is not printed.",
        [pc.pdf_document_block(pdf), {"type": "text", "text": f"{focus}\nCopy each row's text as evidence."}],
        TITLES_TOOL)
    log.info(f"  report: {out.get('report_title')} | as of {out.get('as_of')} | {len(out.get('rows') or [])} row(s)")

    assets, partners = [], []
    for row in out.get("rows") or []:
        title = pc.normalise_title(row.get("title_number"))
        if not title or (scope == "pilots" and title not in pc.TARGET_ASSETS):
            continue
        verified = pc.evidence_found(text, row.get("evidence", "")) if text else None
        if verified is False:
            log.warning(f"  dropped {title}: its evidence text was not found in the PDF")
            continue
        fields = {
            "title_type": title.split()[0],
            "title_type_as_printed": row.get("title_type"),
            "operator": row.get("operator"),
            "contract_type": _map(row.get("contract_type"), CONTRACT_MAP),
            "contract_type_as_printed": row.get("contract_type"),
            "status": _map(row.get("status"), STATUS_MAP),
            "status_as_printed": row.get("status"),
            "area_km2": row.get("area_km2"),
            "terrain": _map(row.get("terrain"), TERRAIN_MAP),
            "terrain_as_printed": row.get("terrain"),
            "basin": row.get("basin"), "state": row.get("state"),
            "award_date": row.get("award_date"), "expiry_date": row.get("expiry_date"),
        }
        fields = {k: v for k, v in fields.items() if v not in (None, "", [])}
        common = {"title_number": title, "is_target_asset": title in pc.TARGET_ASSETS,
                  "report_title": out.get("report_title"), "as_of": out.get("as_of"),
                  "source_page": row.get("page"), "evidence": row.get("evidence"),
                  "evidence_verified": verified, "suggested_grade": "A",
                  "source_name": "NUPRC", "source_url": url}
        assets.append({"proposed_data": {**common, "fields": fields, "null_means": "not stated; keep current value"},
                       "source_url": url, "dedupe_key": pc.make_key("asset", sha, title, sorted(fields.items()))})
        for p in row.get("partners") or []:
            if not p.get("company"):
                continue
            partners.append({"proposed_data": {**common, "company": p["company"].strip(),
                                               "interest_pct": p.get("interest_pct"),
                                               "is_operator": bool(row.get("operator")) and
                                               p["company"].strip().lower() == str(row.get("operator")).strip().lower()},
                             "source_url": url,
                             "dedupe_key": pc.make_key("partner", sha, title, p["company"].strip().lower(),
                                                       p.get("interest_pct"))})
    return assets, partners


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config()["assets"]
    reports = discover(cfg)
    if not reports:
        log.error("No licence or acreage reports found. Run with dry_run and check the crawl log.")
        return 1
    if pc.DRY_RUN:
        log.info("DRY_RUN: discovery only.")
        return 0
    all_assets, all_partners, failures = [], [], 0
    for url in reports:
        try:
            a, p = process_report(url, cfg.get("scope", "pilots"))
            all_assets += a
            all_partners += p
        except Exception as e:
            failures += 1
            log.warning(f"Failed on {url}: {e}")
    pc.post_staging("assets", all_assets, COLLECTOR)
    pc.post_staging("asset_partners", all_partners, COLLECTOR)
    found = sorted({a["proposed_data"]["title_number"] for a in all_assets})
    missing = [t for t in pc.TARGET_ASSETS if t not in found]
    pc.write_summary(f"### Licence register\n- Reports read: {len(reports) - failures}/{len(reports)}\n"
                     f"- Title proposals: {len(all_assets)}; partner proposals: {len(all_partners)}\n"
                     f"- Pilot titles not found in these reports: {', '.join(missing) or 'none'}")
    log.info(f"Pilot titles found: {found or 'none'}; not found: {missing or 'none'}")
    return 1 if failures == len(reports) else 0


if __name__ == "__main__":
    sys.exit(main())
