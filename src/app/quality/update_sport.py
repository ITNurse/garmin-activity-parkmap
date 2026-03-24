#!/usr/bin/env python3
"""
Update the sport field in track.json files based on park name and current sport.

Usage:
    python update_sport.py --park "Killarney Lake Park" --old-sport "other" --new-sport "hiking"

TRACKS_DIR and CORRECTIONS_LOG are read from config.py at the project root.
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

# Resolve project root and load config (same pattern as the rest of the project)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config


def log_correction(activity_id: str, old_sport: str, new_sport: str):
    log_path = config.CORRECTIONS_LOG
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = log_path.exists()
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "activity_id", "reason",
                             "points_before", "points_after", "user"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            activity_id,
            "Fix sport",
            old_sport,
            new_sport,
            "manual",
        ])


def find_and_update(tracks_dir: Path, park_name: str, old_sport: str, new_sport: str, dry_run: bool) -> None:
    matched = 0
    skipped = 0
    errors = 0

    all_files = sorted(tracks_dir.rglob("*.track.json"))
    print(f"  Found {len(all_files)} track.json file(s) total.\n")

    for track_file in all_files:
        try:
            with open(track_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  ERROR  {track_file}: {e}")
            errors += 1
            continue

        meta = data.get("meta", {})
        current_sport = meta.get("sport")
        parks = data.get("parks", [])
        park_names = [p.get("name", "") for p in parks]

        sport_match = current_sport == old_sport
        park_match = park_name in park_names

        if not sport_match or not park_match:
            skipped += 1
            continue

        # Match found
        matched += 1
        activity_id = track_file.stem  # e.g. 2021_05_31_192844-5744625786.track
        tag = "[DRY RUN] " if dry_run else ""
        print(f"  {tag}UPDATING  {track_file.name}")
        print(f"            sport: '{old_sport}' -> '{new_sport}'")

        if not dry_run:
            data["meta"]["sport"] = new_sport
            try:
                with open(track_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    f.write("\n")
                log_correction(activity_id, old_sport, new_sport)
            except OSError as e:
                print(f"  ERROR writing {track_file}: {e}")
                errors += 1

    print()
    print(f"Done. {matched} file(s) updated, {skipped} skipped, {errors} error(s).")
    if not dry_run and matched > 0:
        print(f"Corrections logged to: {config.CORRECTIONS_LOG}")
    if dry_run and matched > 0:
        print("(Dry run — no files were changed. Re-run without --dry-run to apply.)")


def main():
    parser = argparse.ArgumentParser(
        description="Update the sport field in track.json files matching a park name and sport."
    )
    parser.add_argument("--park",      required=True, help="Park name to match (e.g. 'Killarney Lake Park')")
    parser.add_argument("--old-sport", required=True, help="Current sport value to match (e.g. 'other')")
    parser.add_argument("--new-sport", required=True, help="New sport value to set (e.g. 'hiking')")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without writing any files")
    args = parser.parse_args()

    tracks_dir = Path(config.TRACKS_DIR)
    if not tracks_dir.is_dir():
        print(f"Error: TRACKS_DIR '{tracks_dir}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning: {tracks_dir}")
    print(f"  park      = '{args.park}'")
    print(f"  old sport = '{args.old_sport}'")
    print(f"  new sport = '{args.new_sport}'")
    if args.dry_run:
        print("  mode      = DRY RUN")
    print()

    find_and_update(tracks_dir, args.park, args.old_sport, args.new_sport, args.dry_run)


if __name__ == "__main__":
    main()
