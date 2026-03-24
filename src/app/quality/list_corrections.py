#!/usr/bin/env python3
"""
list_corrections.py
-------------------
Displays all entries in the track corrections log — both manual trims
and metadata recalculations.

Usage:
    python scripts/list_corrections.py
"""

import sys
import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

CORRECTIONS_LOG = config.CORRECTIONS_LOG


def main():
    if not CORRECTIONS_LOG.exists():
        print("No corrections log found.")
        print(f"  Expected: {CORRECTIONS_LOG}")
        print("\n  The log is created automatically when you first use")
        print("  trim_track.py or recalculate_metadata.py.")
        return

    with open(CORRECTIONS_LOG, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("Corrections log exists but contains no entries.")
        return

    print("\n" + "=" * 90)
    print("TRACK CORRECTIONS LOG")
    print(f"File: {CORRECTIONS_LOG}")
    print("=" * 90)
    print(f"{'Date':<12} {'Activity ID':<35} {'Points':<16} {'Reason'}")
    print("-" * 90)

    for row in rows:
        date        = row.get("timestamp", "")[:10]
        aid         = row.get("activity_id", "")[:34]
        pts_before  = row.get("points_before", "")
        pts_after   = row.get("points_after",  "")
        pts         = f"{pts_before} -> {pts_after}" if pts_before != "-" else "-"
        reason      = row.get("reason", "")[:45]
        print(f"{date:<12} {aid:<35} {pts:<16} {reason}")

    print("-" * 90)
    print(f"Total entries: {len(rows)}")
    print()


if __name__ == "__main__":
    main()
