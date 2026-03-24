#!/usr/bin/env python3
"""
restamp_tracks.py
----------------------
Restamps track timestamps into new start/end datetime ranges.

SINGLE MODE:
    python restamp_track_master.py <input_file> <start_datetime> <end_datetime> [output_file]

BATCH MODE (one input track, many rows in schedule):
    python restamp_track_master.py <input_file> --csv <schedule_csv> [--outdir <output_dir>]

MASTER MODE (many input tracks, each with its own schedule csv):
    python restamp_track_master.py --jobs <jobs_csv> [--outdir <output_dir>]

The schedule CSV must have columns: start_utc, end_utc
The jobs CSV must have columns: input_track_json, schedule

If a path in CSV is relative, it is resolved relative to the script folder.

Default output folder is: tracks
Each output file is named:
    {ts_str}-{park_name}.track.json
Where ts_str is derived from start_utc, and park_name is the input filename stem without the leading date.
"""

import sys
import csv
import json
import copy
import random
import argparse
from pathlib import Path
from datetime import datetime


SCRIPT_DIR = Path(__file__).resolve().parent

SPORTS = ["running", "walking", "hiking", "cycling"]


def parse_dt(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def restamp(data: dict, start_dt: datetime, end_dt: datetime) -> dict:
    data = copy.deepcopy(data)
    track = data.get("track", [])
    n = len(track)
    if n == 0:
        raise ValueError("No track points found.")
    if end_dt <= start_dt:
        raise ValueError("end_datetime must be after start_datetime.")

    total_seconds = (end_dt - start_dt).total_seconds()

    for i, point in enumerate(track):
        t = i / (n - 1) if n > 1 else 0.0
        new_dt = start_dt + (end_dt - start_dt) * t
        point["t"] = new_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    data["first_timestamp"] = track[0]["t"]
    data["first_lat"] = track[0].get("lat")
    data["first_lon"] = track[0].get("lon")
    data["point_count"] = n

    if "meta" in data and isinstance(data["meta"], dict):
        data["meta"]["sport"] = random.choice(SPORTS)
        data["meta"]["start_time"] = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        data["meta"]["end_time"] = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        data["meta"]["total_elapsed_time_s"] = round(total_seconds, 2)

    return data


def load_source(input_path: Path) -> dict:
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    with input_path.open(encoding="utf-8") as f:
        return json.load(f)


def write_track(data: dict, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def resolve_path(p: str | Path) -> Path:
    p = Path(str(p).strip())
    if p.is_absolute():
        return p
    return (SCRIPT_DIR / p).resolve()


def derive_park_name_from_input(input_path: Path) -> str:
    # expected: YYYYMMDD_park_name_words.track.json (or similar)
    full_stem = input_path.stem.replace(".track", "")
    if "_" in full_stem:
        return full_stem.split("_", 1)[1]
    return full_stem


def run_single(input_path, start_str, end_str, output_path=None):
    input_path = resolve_path(input_path)
    source = load_source(input_path)
    start_dt = parse_dt(start_str)
    end_dt = parse_dt(end_str)
    result = restamp(source, start_dt, end_dt)

    if output_path is None:
        stem = input_path.stem.replace(".track", "")
        output_path = input_path.parent / f"{stem}_restamped.track.json"
    else:
        output_path = resolve_path(output_path)

    write_track(result, output_path)
    total_seconds = (end_dt - start_dt).total_seconds()
    print("Done!")
    print(f"  Points:   {len(result['track'])}")
    print(f"  Start:    {result['track'][0]['t']}")
    print(f"  End:      {result['track'][-1]['t']}")
    print(f"  Duration: {total_seconds/60:.1f} min")
    print(f"  Output:   {output_path}")


def run_batch(input_path, csv_path, outdir):
    input_path = resolve_path(input_path)
    csv_file = resolve_path(csv_path)
    out_dir = resolve_path(outdir)

    source = load_source(input_path)

    if not csv_file.exists():
        raise FileNotFoundError(f"CSV not found: {csv_file}")

    with csv_file.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise ValueError("CSV is empty.")

    if "start_utc" not in rows[0] or "end_utc" not in rows[0]:
        raise ValueError("Schedule CSV must have 'start_utc' and 'end_utc' columns.")

    park_name = derive_park_name_from_input(input_path)

    print(f"Batch restamping {len(rows)} tracks from {input_path.name} using {csv_file.name} -> {out_dir}/")
    print()

    for i, row in enumerate(rows, 1):
        start_dt = parse_dt(row["start_utc"])
        end_dt = parse_dt(row["end_utc"])
        result = restamp(source, start_dt, end_dt)

        ts_str = start_dt.strftime("%Y%m%d_%H%M%S")
        out_file = out_dir / f"{ts_str}-{park_name}.track.json"

        write_track(result, out_file)

        duration_min = (end_dt - start_dt).total_seconds() / 60
        print(
            f"  [{i:3d}/{len(rows)}] {start_dt.strftime('%Y-%m-%d %H:%M')}Z  "
            f"({duration_min:.0f} min)  ->  {out_file.name}"
        )

    print()
    print(f"Done! {len(rows)} files written to {out_dir}/")


def run_master_jobs(jobs_csv, outdir):
    jobs_csv = resolve_path(jobs_csv)
    out_dir = resolve_path(outdir)

    if not jobs_csv.exists():
        raise FileNotFoundError(f"Jobs CSV not found: {jobs_csv}")

    with jobs_csv.open(newline="", encoding="utf-8") as f:
        jobs = list(csv.DictReader(f))

    if not jobs:
        raise ValueError("Jobs CSV is empty.")

    required = {"input_track_json", "schedule"}
    if not required.issubset(set(jobs[0].keys())):
        raise ValueError("Jobs CSV must have columns: input_track_json, schedule")

    print(f"Master batch: {len(jobs)} job(s) -> {out_dir}/")
    print()

    for j, job in enumerate(jobs, 1):
        input_track = job["input_track_json"].strip()
        schedule = job["schedule"].strip()

        print(f"[Job {j}/{len(jobs)}] {input_track}  +  {schedule}")
        run_batch(input_track, schedule, out_dir)
        print()

    print("All jobs complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", nargs="?", help="Input .track.json for single or schedule batch mode")
    parser.add_argument("start_datetime", nargs="?", help="Single mode start datetime (ISO)")
    parser.add_argument("end_datetime", nargs="?", help="Single mode end datetime (ISO)")
    parser.add_argument("output_file", nargs="?", help="Single mode output file (optional)")

    parser.add_argument("--csv", help="Schedule CSV for batch mode (requires input_file)")
    parser.add_argument("--jobs", help="Jobs CSV for master mode (many input tracks + schedules)")
    parser.add_argument("--outdir", default="tracks", help="Output directory (default: tracks)")

    args = parser.parse_args()

    try:
        if args.jobs:
            run_master_jobs(args.jobs, args.outdir)
        elif args.csv:
            if not args.input_file:
                raise ValueError("Batch mode requires input_file plus --csv.")
            run_batch(args.input_file, args.csv, args.outdir)
        else:
            if not (args.input_file and args.start_datetime and args.end_datetime):
                print(__doc__)
                sys.exit(1)
            run_single(args.input_file, args.start_datetime, args.end_datetime, args.output_file)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
