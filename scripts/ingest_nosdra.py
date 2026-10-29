#!/usr/bin/env python3
"""
NOSDRA oil spill collector.

Finds spill data on NOSDRA's sites (CSV/Excel downloads, HTML tables or PDF reports) and
proposes 'incident' records. For tables, Claude only identifies which column holds which
field; every row is then converted by code, so figures never pass through the AI.
"""
import io
import json
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

import pipeline_common as pc

log = pc.log
COLLECTOR = "nosdra"

FIELDS = ["incident_ref", "incident_date", "company", "facility_name", "latitude", "longitude",
          "lga", "state", "incident_type", "cause", "volume", "description"]

MAPPING_TOOL = {
    "name": "map_columns",
    "description": "Map spreadsheet columns to incident fields.",
    "input_schema": {
        "type": "object",
        "properties": {
            "is_spill_data": {"type": "boolean"},
            "mapping": {
                "type": "object",
                "description": "Field -> exact column name from the header, or null if absent.",
                "properties": {f: {"type": ["string", "null"]} for f in FIELDS},
                "required": FIELDS,
            },
            "volume_unit": {"type": ["string", "null"], "description": "Unit of the volume column: bbl, m3, litres."},
            "date_day_first": {"type": "boolean", "description": "True if dates are written day first (e.g. 31/01/2026)."},
        },
        "required": ["is_spill_data", "mapping", "volume_unit", "date_day_first"],
    },
}

PDF_TOOL = {
    "name": "record_incidents",
    "description": "Transcribe every spill incident listed in the document.",
    "input_schema": {
        "type": "object",
        "properties": {"incidents": {"type": "array", "items": {"type": "object", "properties": {
            "incident_ref": {"type": ["string", "null"]},
            "incident_date": {"type": ["string", "null"], "description": "YYYY-MM-DD"},
            "company": {"type": ["string", "null"]},
            "facility_name": {"type": ["string", "null"]},
            "latitude": {"type": ["number", "null"]},
            "longitude": {"type": ["number", "null"]},
            "lga": {"type": ["string", "null"]},
            "state": {"type": ["string", "null"]},
            "cause": {"type": ["string", "null"]},
            "volume": {"type": ["number", "null"]},
            "volume_unit": {"type": ["string", "null"]},
            "description": {"type": ["string", "null"]},
            "evidence": {"type": "string", "description": "Text copied from the row."},
        }, "required": ["evidence"]}}},
        "required": ["incidents"],
    },
}

TO_BBL = {"bbl": 1.0, "bbls": 1.0, "barrels": 1.0, "m3": 6.2898, "cubic metres": 6.2898,
          "litres": 0.0062898, "liters": 0.0062898, "l": 0.0062898}

TYPE_WORDS = {"sabotage": "sabotage", "vandal": "sabotage", "theft": "sabotage", "fire": "fire",
              "corrosion": "spill", "equipment": "spill", "operational": "spill", "spill": "spill"}


def classify(cause: str) -> str:
    c = (cause or "").lower()
    return next((v for k, v in TYPE_WORDS.items() if k in c), "spill")


def valid_coords(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None, None
    if pc.NIGERIA_BBOX["lat"][0] <= lat <= pc.NIGERIA_BBOX["lat"][1] and \
            pc.NIGERIA_BBOX["lon"][0] <= lon <= pc.NIGERIA_BBOX["lon"][1]:
        return round(lat, 6), round(lon, 6)
    if pc.NIGERIA_BBOX["lat"][0] <= lon <= pc.NIGERIA_BBOX["lat"][1] and \
            pc.NIGERIA_BBOX["lon"][0] <= lat <= pc.NIGERIA_BBOX["lon"][1]:
        return round(lon, 6), round(lat, 6)  # columns were swapped
    return None, None


def to_record(row: dict, unit: str | None, source_url: str, evidence: str) -> dict | None:
    date = row.get("incident_date")
    if not date:
        return None
    lat, lon = valid_coords(row.get("latitude"), row.get("longitude"))
    volume = row.get("volume")
    try:
        volume = float(str(volume).replace(",", "")) if volume not in (None, "", "nan") else None
    except ValueError:
        volume = None
    factor = TO_BBL.get((unit or "").strip().lower())
    ref = str(row.get("incident_ref") or "").strip() or None
    data = {
        "incident_ref": ref,
        "incident_date": date,
        "incident_type": classify(row.get("cause") or row.get("incident_type")),
        "cause_as_reported": row.get("cause") or row.get("incident_type"),
        "operator": row.get("company"),
        "facility_name": row.get("facility_name"),
        "latitude": lat, "longitude": lon,
        "lga": row.get("lga"), "state": row.get("state"),
        "volume_bbl": round(volume * factor, 2) if volume is not None and factor else None,
        "volume_as_reported": volume, "volume_unit_as_reported": unit,
        "description": row.get("description"),
        "evidence": evidence[:400],
        "suggested_grade": "A",
        "source_name": "NOSDRA",
        "source_url": source_url,
    }
    key = pc.make_key("incident", ref) if ref else pc.make_key("incident", date, row.get("company"), lat, lon,
                                                               row.get("facility_name"), volume)
    return {"proposed_data": data, "source_url": source_url, "dedupe_key": key}


def parse_date(value, day_first: bool):
    if value in (None, "") or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and value > 1e11:      # map services send epoch milliseconds
        ts = pd.to_datetime(value, unit="ms", errors="coerce")
    elif isinstance(value, (int, float)) and value > 1e8:     # epoch seconds
        ts = pd.to_datetime(value, unit="s", errors="coerce")
    else:
        ts = pd.to_datetime(value, dayfirst=day_first, errors="coerce")
    return None if pd.isna(ts) else ts.date().isoformat()


# ---------------------------------------------------------------------------
# Interactive map pages: capture the data the page loads in the background
# ---------------------------------------------------------------------------
DATA_TYPES = ("json", "csv", "geo+json", "spreadsheet", "excel")


def sniff_page_data(url: str, max_bytes: int = 25 * 1024 * 1024) -> list:
    """Open the page in a headless browser and keep every JSON/CSV response it loads.
    Returns [{"url", "content_type", "body"}], largest first."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("Playwright not installed; cannot capture background data.")
        return []
    captured = []

    def on_response(resp):
        ctype = (resp.headers.get("content-type") or "").lower()
        if not any(t in ctype for t in DATA_TYPES) and pc.file_ext(resp.url) not in ("json", "geojson", "csv"):
            return
        try:
            body = resp.body()
        except Exception:
            return
        if 200 <= len(body) <= max_bytes:
            captured.append({"url": resp.url, "content_type": ctype, "body": body})

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(user_agent=pc.HTTP_HEADERS["User-Agent"])
        page.on("response", on_response)
        try:
            page.goto(url, wait_until="networkidle", timeout=90_000)
            page.wait_for_timeout(8_000)  # map layers often load after the page settles
        except Exception as e:
            log.warning(f"Browser load of {url} incomplete: {e}")
        browser.close()
    captured.sort(key=lambda c: len(c["body"]), reverse=True)
    log.info(f"Captured {len(captured)} background data response(s) from {url}:")
    for c in captured[:15]:
        log.info(f"  {len(c['body']) / 1024:8.1f} KB  {c['content_type'][:30]:30}  {c['url'][:150]}")
    return captured


def json_to_frame(obj) -> pd.DataFrame:
    """Flatten GeoJSON, ArcGIS feature sets or plain lists of records into a table.
    Point coordinates become _lon / _lat columns (converted from Web Mercator if needed)."""
    if isinstance(obj, dict) and isinstance(obj.get("features"), list):
        rows = []
        for f in obj["features"]:
            props = dict(f.get("properties") or f.get("attributes") or {})
            g = f.get("geometry") or {}
            if isinstance(g.get("coordinates"), list) and len(g["coordinates"]) >= 2 \
                    and isinstance(g["coordinates"][0], (int, float)):
                props["_lon"], props["_lat"] = g["coordinates"][0], g["coordinates"][1]
            elif "x" in g and "y" in g:
                props["_lon"], props["_lat"] = g["x"], g["y"]
            rows.append(props)
        df = pd.DataFrame(rows)
        if "_lon" in df and pd.to_numeric(df["_lon"], errors="coerce").abs().max() > 180:
            from pyproj import Transformer
            tr = Transformer.from_crs(3857, 4326, always_xy=True)
            lon, lat = tr.transform(pd.to_numeric(df["_lon"]).values, pd.to_numeric(df["_lat"]).values)
            df["_lon"], df["_lat"] = lon, lat
        return df
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    if isinstance(obj, dict):  # e.g. {"data": [...]} : take the largest list of records
        lists = [v for v in obj.values() if isinstance(v, list) and v and isinstance(v[0], dict)]
        return pd.json_normalize(max(lists, key=len)) if lists else pd.DataFrame()
    return pd.DataFrame()


def frame_from_body(body: bytes, content_type: str, url: str) -> pd.DataFrame:
    if "csv" in content_type or pc.file_ext(url) == "csv":
        return pd.read_csv(io.BytesIO(body))
    text = body.decode("utf-8", "ignore").strip()
    if text.endswith(")") and "(" in text[:200]:  # JSONP wrapper
        text = text[text.index("(") + 1:-1]
    return json_to_frame(json.loads(text))


def process_table(df: pd.DataFrame, source_url: str, cutoff) -> list:
    df = df.dropna(how="all")
    if df.empty or len(df.columns) < 4:
        return []
    header = [str(c) for c in df.columns]
    sample = df.head(5).astype(str).to_dict(orient="records")
    m = pc.call_tool("You map spreadsheet columns. Use only the header and rows shown.",
                     [{"type": "text", "text": f"Header: {json.dumps(header)}\nSample rows: {json.dumps(sample)[:6000]}"}],
                     MAPPING_TOOL, max_tokens=1500)
    if not m.get("is_spill_data"):
        log.info("  table is not spill data; skipped")
        return []
    mapping = {f: c for f, c in (m.get("mapping") or {}).items() if c in df.columns}
    log.info(f"  column mapping: {mapping}")
    records = []
    for _, r in df.iterrows():
        row = {f: (None if pd.isna(r[c]) else r[c]) for f, c in mapping.items()}
        row["incident_date"] = parse_date(row.get("incident_date"), bool(m.get("date_day_first", True)))
        if not row["incident_date"] or row["incident_date"] < cutoff:
            continue
        evidence = " | ".join(f"{c}: {r[c]}" for c in mapping.values() if not pd.isna(r[c]))
        rec = to_record(row, m.get("volume_unit"), source_url, evidence)
        if rec:
            records.append(rec)
    return records


def process_pdf(pdf_bytes: bytes, source_url: str, cutoff) -> list:
    text = pc.pdf_text(pdf_bytes)
    out = pc.call_tool("You transcribe oil spill records exactly as printed. Never invent values.",
                       [pc.pdf_document_block(pdf_bytes), {"type": "text", "text": "Transcribe every incident."}],
                       PDF_TOOL)
    records = []
    for inc in out.get("incidents") or []:
        if text and not pc.evidence_found(text, inc.get("evidence", "")):
            log.warning("  dropped incident: its evidence text was not found in the PDF")
            continue
        if not inc.get("incident_date") or inc["incident_date"] < cutoff:
            continue
        rec = to_record(inc, inc.get("volume_unit"), source_url, inc.get("evidence", ""))
        if rec:
            rec["proposed_data"]["evidence_verified"] = bool(text)
            records.append(rec)
    return records


def main() -> int:
    if not pc.check_env():
        return 1
    cfg = pc.load_config()["nosdra"]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(cfg.get("lookback_days", 60)))).date().isoformat()
    exts = set(cfg["data_file_extensions"])
    kws = [k.lower() for k in cfg["keywords"]]
    on_site = pc.same_site(cfg["start_urls"])

    def wanted(url, v):
        return pc.file_ext(url) in exts and any(k in f"{url} {v['text']}".lower() for k in kws)

    links = pc.crawl_with_fallback(cfg["start_urls"], on_site, wanted, max_pages=25, depth=2)
    files = [u for u, v in links.items() if wanted(u, v)]
    pages = [v["found_on"] for v in links.values()]
    log.info(f"Found {len(files)} candidate spill data file(s):")
    for u in files[:20]:
        log.info(f"  {u}")

    records, failures = [], 0
    limit = int(cfg.get("max_files_per_run", 5))

    # Interactive map pages load their records in the background: capture those first.
    captured = []
    if pc.USE_BROWSER:
        for start in cfg["start_urls"]:
            captured += sniff_page_data(start)
    for c in captured[:limit]:
        if not any(k in c["url"].lower() for k in kws) and len(c["body"]) < 20_000:
            continue  # small config/style files, not data
        try:
            df = frame_from_body(c["body"], c["content_type"], c["url"])
            log.info(f"Background data {c['url'][:120]}: {len(df)} row(s), columns {list(df.columns)[:15]}")
            if not pc.DRY_RUN and not df.empty:
                records += process_table(df, cfg["start_urls"][0], cutoff)
        except Exception as e:
            log.warning(f"Could not read background data {c['url'][:120]}: {e}")

    for url in files[:limit]:
        try:
            ext = pc.file_ext(url)
            log.info(f"Processing {url}")
            if pc.DRY_RUN:
                continue
            if ext == "pdf":
                records += process_pdf(pc.fetch_pdf(url), url, cutoff)
            elif ext == "csv":
                records += process_table(pd.read_csv(io.BytesIO(pc.fetch(url).content)), url, cutoff)
            elif ext in ("xlsx", "xls"):
                for _, df in pd.read_excel(io.BytesIO(pc.fetch(url).content), sheet_name=None).items():
                    records += process_table(df, url, cutoff)
            elif ext == "json":
                records += process_table(pd.json_normalize(pc.fetch(url).json()), url, cutoff)
        except Exception as e:
            failures += 1
            log.warning(f"Failed on {url}: {e}")

    if not files and not records and not captured:  # last resort: HTML tables on the scanned pages
        for page in sorted(set(pages))[:10]:
            try:
                for df in pd.read_html(io.StringIO(pc.fetch(page).text)):
                    if not pc.DRY_RUN:
                        records += process_table(df, page, cutoff)
            except ValueError:
                continue
            except Exception as e:
                log.warning(f"Could not read tables on {page}: {e}")

    unique = list({r["dedupe_key"]: r for r in records}.values())
    pc.post_staging("incident", unique, COLLECTOR)
    pc.write_summary(f"### NOSDRA\n- Data files found: {len(files)}; background data responses: {len(captured)}\n- Incidents proposed (since {cutoff}): "
                     f"{len(unique)}\n- Files failed: {failures}")
    log.info(f"NOSDRA done: {len(unique)} incident(s) since {cutoff}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
