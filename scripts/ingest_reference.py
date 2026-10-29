#!/usr/bin/env python3
"""
Map reference data collector (boundaries, facilities, pipelines).

Watches data/reference/ in the repository:
  data/reference/boundaries/   GeoJSON or zipped shapefiles of licence polygons
  data/reference/facilities/   GeoJSON points or CSV (name, facility_type, latitude, longitude, ...)
  data/reference/pipelines/    GeoJSON lines
Each folder needs a source.json: {"publisher", "title", "url", "retrieved_date", "licence_terms"}.
Files are converted to WGS84 (EPSG:4326), checked, and proposed to the MD review queue.
No AI is involved.
"""
import csv
import io
import json
import os
import sys
import zipfile

import pipeline_common as pc

log = pc.log
COLLECTOR = "reference"
ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# Reading and reprojecting
# ---------------------------------------------------------------------------
def _transformer(crs_text: str | None):
    if not crs_text:
        return None
    from pyproj import CRS, Transformer
    src = CRS.from_user_input(crs_text)
    if src.equals(CRS.from_epsg(4326)) or "CRS84" in str(crs_text).upper():
        return None
    log.info(f"  reprojecting from {src.name} to WGS84")
    return Transformer.from_crs(src, CRS.from_epsg(4326), always_xy=True)


def _apply(coords, tr):
    if isinstance(coords[0], (int, float)):
        x, y = tr.transform(coords[0], coords[1]) if tr else (coords[0], coords[1])
        return [round(x, 6), round(y, 6)]
    return [_apply(c, tr) for c in coords]


def _points(coords):
    if isinstance(coords[0], (int, float)):
        yield coords
    else:
        for c in coords:
            yield from _points(c)


def inside_nigeria(geom) -> bool:
    return all(pc.NIGERIA_BBOX["lon"][0] <= x <= pc.NIGERIA_BBOX["lon"][1] and
               pc.NIGERIA_BBOX["lat"][0] <= y <= pc.NIGERIA_BBOX["lat"][1]
               for x, y, *_ in _points(geom["coordinates"]))


def read_features(path: str) -> list:
    """Returns GeoJSON features in WGS84."""
    if path.lower().endswith((".geojson", ".json")):
        with open(path, encoding="utf-8") as f:
            fc = json.load(f)
        crs = ((fc.get("crs") or {}).get("properties") or {}).get("name")
        tr = _transformer(crs)
        feats = fc.get("features") or ([fc] if fc.get("type") == "Feature" else [])
    elif path.lower().endswith(".zip"):
        import shapefile
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            pick = lambda ext: next((n for n in names if n.lower().endswith(ext)), None)
            shp, dbf, prj = pick(".shp"), pick(".dbf"), pick(".prj")
            if not shp or not dbf:
                raise ValueError("zip must contain .shp and .dbf files")
            reader = shapefile.Reader(shp=io.BytesIO(z.read(shp)), dbf=io.BytesIO(z.read(dbf)))
            tr = _transformer(z.read(prj).decode("utf-8", "ignore") if prj else None)
            feats = reader.__geo_interface__["features"]
    elif path.lower().endswith(".csv"):
        with open(path, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        feats = []
        for r in rows:
            lat = r.get("latitude") or r.get("lat")
            lon = r.get("longitude") or r.get("lon") or r.get("lng")
            if lat and lon:
                feats.append({"type": "Feature", "properties": r,
                              "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]}})
        tr = None
    else:
        return []
    out = []
    for f in feats:
        g = f.get("geometry")
        if g and g.get("coordinates"):
            out.append({"properties": dict(f.get("properties") or {}),
                        "geometry": {"type": g["type"], "coordinates": _apply(g["coordinates"], tr)}})
    return out


# ---------------------------------------------------------------------------
# Kinds
# ---------------------------------------------------------------------------
def load_source(folder: str):
    path = os.path.join(folder, "source.json")
    if not os.path.exists(path):
        log.error(f"{folder} has no source.json; its files were skipped (the MD needs a source for every record).")
        return None
    with open(path, encoding="utf-8") as f:
        src = json.load(f)
    if not src.get("publisher") or not (src.get("url") or src.get("title")):
        log.error(f"{path} needs at least 'publisher' and 'url' or 'title'.")
        return None
    return src


def boundary_items(feats, src, rel, cfg, scope) -> list:
    items = []
    for f in feats:
        g, props = f["geometry"], f["properties"]
        if g["type"] not in ("Polygon", "MultiPolygon"):
            continue
        title = next((pc.normalise_title(props[k]) for k in cfg["title_properties"]
                      if k in props and pc.normalise_title(props[k])), None)
        if not title:
            log.warning(f"  {rel}: a polygon has no recognisable title in {cfg['title_properties']}; skipped")
            continue
        if scope == "pilots" and title not in pc.TARGET_ASSETS:
            continue
        if not inside_nigeria(g):
            log.warning(f"  {rel}: {title} falls outside Nigeria after conversion; check the coordinate system")
            continue
        items.append({"proposed_data": {"title_number": title, "is_target_asset": title in pc.TARGET_ASSETS,
                                        "boundary": g, "file": rel, "source": src, "suggested_grade": "A"},
                      "source_url": src.get("url"),
                      "dedupe_key": pc.make_key("boundary", title, json.dumps(g, sort_keys=True))})
    return items


def facility_items(feats, src, rel) -> list:
    items = []
    for f in feats:
        g, p = f["geometry"], f["properties"]
        if g["type"] != "Point" or not p.get("name") or not inside_nigeria(g):
            continue
        data = {"name": p["name"], "facility_type": (p.get("facility_type") or "").lower() or None,
                "operator": p.get("operator"), "owner": p.get("owner"),
                "capacity_bpd": p.get("capacity_bpd"), "capacity_mmscfd": p.get("capacity_mmscfd"),
                "longitude": g["coordinates"][0], "latitude": g["coordinates"][1],
                "file": rel, "source": src, "suggested_grade": "A"}
        items.append({"proposed_data": data, "source_url": src.get("url"),
                      "dedupe_key": pc.make_key("facility", json.dumps(data, sort_keys=True, default=str))})
    return items


def pipeline_items(feats, src, rel) -> list:
    items = []
    for f in feats:
        g, p = f["geometry"], f["properties"]
        if g["type"] not in ("LineString", "MultiLineString") or not p.get("name") or not inside_nigeria(g):
            continue
        data = {"name": p["name"], "operator": p.get("operator"), "capacity_bpd": p.get("capacity_bpd"),
                "status": (p.get("status") or "unknown").lower(), "route": g,
                "file": rel, "source": src, "suggested_grade": "A"}
        items.append({"proposed_data": data, "source_url": src.get("url"),
                      "dedupe_key": pc.make_key("pipeline", json.dumps(data, sort_keys=True, default=str))})
    return items


def main() -> int:
    if not pc.check_env(need_claude=False):
        return 1
    config = pc.load_config()
    cfg, scope = config["reference"], config["assets"].get("scope", "pilots")
    base = os.path.join(ROOT, cfg["folder"])
    kinds = {"boundaries": ("asset_boundary", None), "facilities": ("facility", None), "pipelines": ("pipeline", None)}
    totals = {}
    for kind, (target, _) in kinds.items():
        folder = os.path.join(base, kind)
        if not os.path.isdir(folder):
            continue
        src = load_source(folder)
        if not src:
            continue
        items = []
        for name in sorted(os.listdir(folder)):
            if name == "source.json":
                continue
            path, rel = os.path.join(folder, name), f"{cfg['folder']}/{kind}/{name}"
            try:
                feats = read_features(path)
            except Exception as e:
                log.error(f"Could not read {rel}: {e}")
                continue
            log.info(f"{rel}: {len(feats)} feature(s)")
            if kind == "boundaries":
                items += boundary_items(feats, src, rel, cfg, scope)
            elif kind == "facilities":
                items += facility_items(feats, src, rel)
            else:
                items += pipeline_items(feats, src, rel)
        totals[target] = pc.post_staging(target, items, COLLECTOR)
    if not totals:
        log.info(f"No reference folders with files found under {cfg['folder']}/.")
    pc.write_summary("### Map reference data\n" + "\n".join(f"- {k}: {v} proposed" for k, v in totals.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
