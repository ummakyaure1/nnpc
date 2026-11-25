#!/usr/bin/env python3
"""
Company and audit disclosures collector.

Reads results reports, operational updates and annual reports published by operators,
and NEITI industry reports, and proposes for each licence they mention:
  - 'asset_partners'   the company's participating interest (and whether it operates)
  - 'asset_production' production for the block, as disclosed (gross, net or working interest)
  - 'asset_fact'       other stated updates (drilling, shut-ins, first oil, licence events)
Every item needs a quote that is found word for word in the document (when it has a text layer).
"""
import hashlib
import os
import re
import sys
from datetime import date

import pipeline_common as pc
import units

log = pc.log
COLLECTOR = "disclosures"

TOOL = {
    "name": "record_asset_disclosures",
    "description": "Record what the document states about specific licences (OML/OPL/PML/PPL).",
    "input_schema": {
        "type": "object",
        "properties": {
            "reporting_company": {"type": ["string", "null"]},
            "report_title": {"type": "string"},
            "report_period_end": {"type": ["string", "null"], "description": "YYYY-MM-DD if stated."},
            "assets": {"type": "array", "items": {"type": "object", "properties": {
                "title_number": {"type": "string", "description": "As printed, e.g. 'OML 53'."},
                "company": {"type": ["string", "null"], "description": "Company whose interest is stated."},
                "interest_pct": {"type": ["number", "null"]},
                "is_operator": {"type": ["boolean", "null"]},
                "interest_evidence": {"type": ["string", "null"], "description": "Exact sentence stating the interest."},
                "interest_is_current": {"type": ["boolean", "null"],
                                        "description": "True if this is the company's stake at the report date; false if it is "
                                                       "a past stake (e.g. before an acquisition or divestment)."},
                "production": {"type": "array", "items": {"type": "object", "properties": {
                    "period_label": {"type": "string", "description": "As printed, e.g. 'H1 2026', 'Q3 2026', '2025'."},
                    "period_start": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
                    "period_end": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
                    "basis": {"type": "string", "enum": ["gross", "net", "working_interest", "unknown"]},
                    "oil_value": {"type": ["number", "null"]}, "oil_unit": {"type": ["string", "null"]},
                    "gas_value": {"type": ["number", "null"]}, "gas_unit": {"type": ["string", "null"]},
                    "boe_value": {"type": ["number", "null"]}, "boe_unit": {"type": ["string", "null"]},
                    "evidence": {"type": "string", "description": "Exact text (sentence or table row) with the figures."},
                }, "required": ["period_label", "basis", "evidence"]}},
                "updates": {"type": "array", "items": {"type": "object", "properties": {
                    "detail": {"type": "string"},
                    "evidence": {"type": "string", "description": "Exact sentence."},
                }, "required": ["detail", "evidence"]}},
                "page": {"type": ["integer", "null"]},
            }, "required": ["title_number", "production", "updates"]}},
        },
        "required": ["report_title", "assets"],
    },
}

SYSTEM = ("You extract facts about oil and gas licences from company and audit reports, using only the document. "
          "Never add facts from your own knowledge. Every figure must be copied with its exact supporting text.")
INSTRUCTION = ("For every licence (OML, OPL, PML, PPL) named in this part, record the stated participating interest "
               "(only one entry per company per licence: its stake at the report date; mark past stakes as not current), "
               "production figures with their period and basis, and other stated updates. Copy figures and units "
               "exactly as printed; do not convert. Skip licences mentioned only in passing.")


DROP_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "documents")


def dropped_documents() -> list:
    """PDFs placed in data/documents/ (for sites that block automated downloads)."""
    if not os.path.isdir(DROP_FOLDER):
        return []
    files = sorted(f for f in os.listdir(DROP_FOLDER) if f.lower().endswith(".pdf"))
    log.info(f"Document drop folder: {len(files)} PDF(s)")
    return [os.path.join(DROP_FOLDER, f) for f in files]


def candidates(src: dict) -> list:
    kws = [k.lower() for k in src.get("keywords", [])]
    on_site = pc.same_site(src["start_urls"])
    excluded = [w.lower() for w in src.get("exclude_words", [])]
    min_year = date.today().year - int(src.get("max_age_years", 1))

    def wanted(url, v):
        text = f"{url} {v['text']}".lower()
        return (pc.file_ext(url) == "pdf" and any(k in text for k in kws) and not any(w in text for w in excluded))

    follow_terms = [t.lower() for t in src.get("follow_contains", [])]
    follow = (lambda u: on_site(u) and any(t in u.lower() for t in follow_terms)) if follow_terms else on_site
    links = pc.crawl_with_fallback(src["start_urls"], follow, wanted, max_pages=int(src.get("max_pages", 25)),
                                   depth=int(src.get("depth", 2)))
    found = []
    for u, v in links.items():
        if not wanted(u, v):
            continue
        period = pc.guess_period(f"{v['text']} {u}")
        if int(period[:4]) and int(period[:4]) < min_year:
            continue
        found.append((period, u, v["text"]))
    found.sort(reverse=True)
    log.info(f"{src['name']}: {len(found)} candidate document(s)")
    for p, u, t in found[:10]:
        log.info(f"  [{p}] {t[:70]} | {u}")
    return [(u, t) for _, u, t in found[: int(src.get("max_docs", 2))]]


_SUFFIXES = r"\b(plc|limited|ltd|llc|inc|nigeria|nig|company|co|holdings?|group)\b\.?"


def company_key(name: str) -> str:
    """'Oando PLC', 'OANDO Plc.' -> 'oando'. Used to keep one stake per company per block."""
    k = re.sub(_SUFFIXES, "", (name or "").lower())
    return re.sub(r"[^a-z0-9]", "", k)


def build_items(out: dict, assets: list, text: str, url: str, sha: str, src: dict) -> list:
    items, stakes = [], {}
    company_default = out.get("reporting_company") or src.get("company")
    common = {"report_title": out.get("report_title"), "source_name": src["name"], "source_url": url,
              "source_sha256": sha, "extraction_model": pc.ANTHROPIC_MODEL}
    for a in assets:
        title = pc.normalise_title(a.get("title_number"))
        if not title:
            continue
        base = {"title_number": title, "is_target_asset": title in pc.TARGET_ASSETS, "source_page": a.get("page"), **common}
        company = (a.get("company") or company_default or "").strip()
        if a.get("interest_pct") is not None and company and a.get("interest_is_current") is not False:
            ok = pc.row_verified(text, a.get("interest_evidence"), names=[title], numbers=[a["interest_pct"]])
            if ok is not False:
                stakes.setdefault(title, {})[company_key(company)] = {
                    **base, "company": company, "interest_pct": a["interest_pct"], "is_operator": a.get("is_operator"),
                    "as_of": out.get("report_period_end"), "evidence": a.get("interest_evidence"),
                    "evidence_verified": ok, "suggested_grade": "B"}
        for p in a.get("production") or []:
            ok = pc.row_verified(text, p.get("evidence"), names=[title],
                                 numbers=[p.get(k) for k in ("oil_value", "gas_value", "boe_value")])
            if ok is False:
                continue
            days = units.period_days(start=p.get("period_start"), end=p.get("period_end"))
            oil, oil_note = units.to_daily(p.get("oil_value"), p.get("oil_unit"), units.OIL_UNITS, days)
            gas, gas_note = units.to_daily(p.get("gas_value"), p.get("gas_unit"), units.GAS_UNITS, days)
            boe, boe_note = units.to_daily(p.get("boe_value"), p.get("boe_unit"), units.BOE_UNITS, days)
            if oil is None and gas is None and boe is None:
                continue
            items.append(("asset_production", {
                "proposed_data": {**base, "company": company or None, "period_label": p.get("period_label"),
                                  "period_start": p.get("period_start"), "period_end": p.get("period_end"),
                                  "basis": "operator_disclosed", "volume_basis": p.get("basis"),
                                  "oil_bpd": oil, "gas_mmscf_d": gas, "boepd": boe,
                                  "values_as_printed": {k: p.get(k) for k in ("oil_value", "oil_unit", "gas_value",
                                                                              "gas_unit", "boe_value", "boe_unit") if p.get(k) is not None},
                                  "conversion_notes": "; ".join(n for n in (oil_note, gas_note, boe_note) if n) or None,
                                  "evidence": p.get("evidence"), "evidence_verified": ok, "suggested_grade": "B"},
                "source_url": url,
                "dedupe_key": pc.make_key("blockprod", sha, title, p.get("period_label"), p.get("basis"), oil, gas, boe)}))
        for u in a.get("updates") or []:
            ok = pc.verified(text, u.get("evidence"))
            if ok is False:
                continue
            items.append(("asset_fact", {
                "proposed_data": {**base, "fact_type": "other", "company": company or None, "detail": u.get("detail"),
                                  "evidence": u.get("evidence"), "evidence_verified": ok,
                                  "published_at": out.get("report_period_end"), "suggested_grade": "B"},
                "source_url": url, "dedupe_key": pc.make_key("fact", sha, title, u.get("evidence"))}))
    for title, by_company in stakes.items():
        # A parent and its subsidiary stating the same stake ('Oando PLC' / 'Oando Energy Resources') are one holding.
        merged = {}
        for key, st in by_company.items():
            first = re.sub(r"[^a-z0-9]", "", (st["company"].split() or [""])[0].lower())
            twin = next((k for k, v in merged.items()
                         if re.sub(r"[^a-z0-9]", "", (v["company"].split() or [""])[0].lower()) == first
                         and abs(float(v["interest_pct"]) - float(st["interest_pct"])) < 0.01), None)
            if twin:
                merged[twin].setdefault("also_named_as", []).append(st["company"])
            else:
                merged[key] = st
        by_company = merged
        total = sum(float(s["interest_pct"]) for s in by_company.values())
        for key, s in by_company.items():
            if total > 100.5:
                s["shares_conflict"] = f"stakes stated for {title} in this document add up to {total:g}%; check before approving"
            items.append(("asset_partners", {"proposed_data": s, "source_url": url,
                                             "dedupe_key": pc.make_key("partner", sha, title, key, s["interest_pct"])}))
        if total > 100.5:
            log.warning(f"  {title}: stated stakes add up to {total:g}%; flagged for the MD")
    return items


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config().get("disclosures") or {}
    by_target, failures, read = {}, 0, 0
    sources = [s for s in cfg.get("sources", []) if s.get("enabled", True)]
    for s in sources:
        if s.get("allow_broken_certificate"):
            for u in s["start_urls"]:
                host = pc.urlparse(u).netloc.removeprefix("www.")
                pc.INSECURE_HOSTS.add(host)
                log.warning(f"{s['name']}: certificate checks are off for {host} (its certificate chain is incomplete)")
    local = [{"name": "Document drop folder", "company": None, "_files": dropped_documents()}]
    for src in sources + local:
        try:
            docs = [(f, os.path.basename(f)) for f in src["_files"]] if "_files" in src else candidates(src)
        except Exception as e:
            log.warning(f"{src['name']}: discovery failed: {e}")
            continue
        if pc.DRY_RUN:
            continue
        for url, _ in docs:
            try:
                if os.path.isfile(url):
                    with open(url, "rb") as fh:
                        pdf = fh.read()
                    url = f"repo:data/documents/{os.path.basename(url)}"
                else:
                    pdf = pc.fetch_pdf(url)
                sha = hashlib.sha256(pdf).hexdigest()
                if pc.already_seen(source_sha256=sha):
                    log.info(f"  already received {url}; skipping")
                    continue
                text = pc.pdf_text(pdf, max_pages=600)
                head, assets = pc.extract_chunked(pdf, SYSTEM, INSTRUCTION, TOOL, "assets")
                found = build_items(head, assets, text, url, sha, src)
                read += 1
                log.info(f"  {head.get('report_title')}: {len(assets)} licence mention(s) -> "
                         f"{sum(1 for t, _ in found if t == 'asset_partners')} interest(s), "
                         f"{sum(1 for t, _ in found if t == 'asset_production')} production figure(s), "
                         f"{sum(1 for t, _ in found if t == 'asset_fact')} update(s)")
                for target, item in found:
                    by_target.setdefault(target, []).append(item)
            except Exception as e:
                failures += 1
                log.warning(f"  failed on {url}: {e}")
    if pc.DRY_RUN:
        log.info("DRY_RUN: discovery only.")
        return 0
    for target, items in by_target.items():
        pc.post_staging(target, list({i["dedupe_key"]: i for i in items}.values()), COLLECTOR)
    pc.write_summary("### Company & audit disclosures\n" + f"- Documents read: {read}\n" +
                     "\n".join(f"- {t}: {len(v)} proposed" for t, v in by_target.items()) +
                     (f"\n- Documents failed: {failures}" if failures else ""))
    log.info(f"Disclosures done: {read} document(s), {failures} failure(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
