#!/usr/bin/env python3
"""Runs the selected collectors in order and reports one combined result.

Usage: python scripts/run_collectors.py --collectors news,nosdra
       python scripts/run_collectors.py --collectors all
"""
import argparse
import importlib
import sys

import pipeline_common as pc

COLLECTORS = {
    "nuprc": "ingest_nuprc",        # monthly production reports (oil, gas)
    "nosdra": "ingest_nosdra",      # oil spill incidents
    "news": "ingest_news",          # bulletin + title facts
    "assets": "ingest_assets",      # licence register for the OMLs
    "nmdpra": "ingest_nmdpra",      # NMDPRA gas and industry reports
    "reference": "ingest_reference",  # map files dropped in data/reference/
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--collectors", default="all")
    names = [c.strip().lower() for c in ap.parse_args().collectors.split(",") if c.strip()]
    if not names or "all" in names:
        names = list(COLLECTORS)
    unknown = [n for n in names if n not in COLLECTORS]
    if unknown:
        pc.log.error(f"Unknown collector(s): {unknown}. Choose from {list(COLLECTORS)} or 'all'.")
        return 1

    results = {}
    for name in names:
        pc.log.info(f"===== {name} =====")
        try:
            results[name] = importlib.import_module(COLLECTORS[name]).main() or 0
        except Exception as e:
            pc.log.exception(f"{name} crashed: {e}")
            results[name] = 1

    pc.write_summary("### Run result\n" + "\n".join(
        f"- {n}: {'OK' if code == 0 else 'FAILED'}" for n, code in results.items())
        + f"\n- Claude tokens: {pc.STATS['claude_input_tokens']} in / {pc.STATS['claude_output_tokens']} out")
    pc.log.info(f"Results: {results}")
    return 1 if any(results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
