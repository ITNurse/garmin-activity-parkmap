import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import config

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fitparse import FitFile, FitParseError

# =============================================================================
# CONFIGURATION — paths pulled from config.py at project root
# =============================================================================
FIT_DIR      = config.RAW_DATA  / "new"
TRACKS_DIR   = config.TRACKS_DIR

OVERWRITE    = False  # Set to True to re-process files that already have output


SEMICIRCLES_TO_DEGREES = 180 / (2 ** 31)


def build_output_filename(start_time: str | None, fit_stem: str) -> str:
    """
    Construct: YYYY_MM_DD_HHMMSS-<fit_stem>.track.json
    Matches the naming scheme used by the Strava pipeline.
    Falls back to 'unknown_datetime-<stem>' if start_time is missing/unparseable.
    """
    if start_time:
        try:
            dt = datetime.fromisoformat(start_time)
            return dt.strftime("%Y_%m_%d_%H%M%S") + f"-{fit_stem}.track.json"
        except ValueError:
            pass
    return f"unknown_datetime-{fit_stem}.track.json"


# =============================================================================
# FIT PARSING
# =============================================================================

def parse_fit_file(fit_path: Path) -> dict:
    ff = FitFile(str(fit_path))
    messages = []
    for msg in ff.get_messages():
        fields = []
        for field in msg.fields:
            val = field.value
            if isinstance(val, datetime):
                val = val.isoformat()
            fields.append({"name": field.name, "value": val})
        messages.append({"name": msg.name, "fields": fields})
    return {"filename": fit_path.name, "messages": messages}


# =============================================================================
# TRACK EXTRACTION
# =============================================================================

def get_field_value(fields: list, name: str):
    for f in fields:
        if f["name"] == name:
            return f["value"]
    return None


def extract_track(data: dict) -> dict | None:
    messages = data.get("messages", [])
    filename = data.get("filename", "unknown.fit")

    meta = {
        "filename":              filename,
        "sport":                 None,
        "sub_sport":             None,
        "start_time":            None,
        "end_time":              None,
        "total_distance_m":      None,
        "total_elapsed_time_s":  None,
        "avg_heart_rate":        None,
        "max_heart_rate":        None,
        "total_ascent_m":        None,
        "total_descent_m":       None,
        "avg_speed_ms":          None,
        "max_speed_ms":          None,
    }

    for msg in messages:
        name   = msg.get("name")
        fields = msg.get("fields", [])

        if name == "session":
            if meta["sport"] is None:
                meta["sport"]     = get_field_value(fields, "sport")
                meta["sub_sport"] = get_field_value(fields, "sub_sport")
            if meta["start_time"] is None:
                meta["start_time"] = get_field_value(fields, "start_time")
            if meta["end_time"] is None:
                ts = get_field_value(fields, "timestamp")
                if ts:
                    meta["end_time"] = ts
            if meta["total_distance_m"] is None:
                meta["total_distance_m"] = get_field_value(fields, "total_distance")
            if meta["total_elapsed_time_s"] is None:
                meta["total_elapsed_time_s"] = get_field_value(fields, "total_elapsed_time")
            if meta["avg_heart_rate"] is None:
                meta["avg_heart_rate"] = get_field_value(fields, "avg_heart_rate")
            if meta["max_heart_rate"] is None:
                meta["max_heart_rate"] = get_field_value(fields, "max_heart_rate")
            if meta["total_ascent_m"] is None:
                meta["total_ascent_m"] = get_field_value(fields, "total_ascent")
            if meta["total_descent_m"] is None:
                meta["total_descent_m"] = get_field_value(fields, "total_descent")
            if meta["avg_speed_ms"] is None:
                meta["avg_speed_ms"] = (
                    get_field_value(fields, "avg_speed")
                    or get_field_value(fields, "enhanced_avg_speed")
                )
            if meta["max_speed_ms"] is None:
                meta["max_speed_ms"] = (
                    get_field_value(fields, "max_speed")
                    or get_field_value(fields, "enhanced_max_speed")
                )

        elif name == "lap" and meta["sport"] is None:
            meta["sport"]     = get_field_value(fields, "sport")
            meta["sub_sport"] = get_field_value(fields, "sub_sport")

        elif name == "sport":
            INVALID = {None, "generic"}
            if meta["sport"] in INVALID:
                sport = get_field_value(fields, "sport")
                if sport is not None:
                    meta["sport"] = sport
            if meta["sub_sport"] in INVALID:
                sub_sport = get_field_value(fields, "sub_sport")
                if sub_sport is not None:
                    meta["sub_sport"] = sub_sport

    track_points = []
    first_lat = first_lon = first_ts = None

    for msg in messages:
        if msg.get("name") != "record":
            continue

        fv = {f["name"]: f["value"] for f in msg.get("fields", [])}

        lat_sc = fv.get("position_lat")
        lon_sc = fv.get("position_long")
        if lat_sc is None or lon_sc is None:
            continue

        lat = round(lat_sc * SEMICIRCLES_TO_DEGREES, 7)
        lon = round(lon_sc * SEMICIRCLES_TO_DEGREES, 7)
        ts  = fv.get("timestamp")
        alt = fv.get("enhanced_altitude") or fv.get("altitude")
        hr  = fv.get("heart_rate")
        spd = fv.get("enhanced_speed") or fv.get("speed")

        point = {"lat": lat, "lon": lon}
        if ts      is not None: point["t"]   = ts
        if alt     is not None: point["alt"] = round(alt, 1)
        if hr      is not None: point["hr"]  = hr
        if spd     is not None: point["spd"] = round(spd, 3)

        track_points.append(point)

        if first_lat is None:
            first_lat, first_lon, first_ts = lat, lon, ts

    if not track_points:
        return None

    if meta["start_time"] is None:
        meta["start_time"] = first_ts

    return {
        "meta":            meta,
        "first_lat":       first_lat,
        "first_lon":       first_lon,
        "first_timestamp": first_ts,
        "point_count":     len(track_points),
        "track":           track_points,
    }


# =============================================================================
# FOLDER PROCESSING
# =============================================================================

def process_folder(input_dir: Path, output_dir: Path, overwrite: bool = False):
    output_dir.mkdir(parents=True, exist_ok=True)

    fit_files = sorted(input_dir.glob("*.fit"))
    if not fit_files:
        print(f"No .fit files found in {input_dir}")
        return

    processed = skipped = failed = 0

    for fit_path in fit_files:
        try:
            data   = parse_fit_file(fit_path)
            result = extract_track(data)

            if result is None:
                print(f"  [warn]  {fit_path.name}  →  no GPS data found")
                failed += 1
                continue

            out_path = output_dir / build_output_filename(
                result["meta"].get("start_time"), fit_path.stem
            )

            if out_path.exists() and not overwrite:
                print(f"  [skip]  {fit_path.name}  →  already exists")
                skipped += 1
                continue

            with open(out_path, "w") as f:
                json.dump(result, f, separators=(",", ":"))

            kb_in  = fit_path.stat().st_size / 1024
            kb_out = out_path.stat().st_size / 1024
            ratio  = kb_in / kb_out if kb_out > 0 else 0
            print(
                f"  [ok]    {fit_path.name}  →  {out_path.name}"
                f"  ({kb_in:.0f} KB → {kb_out:.0f} KB, {ratio:.1f}x smaller)"
            )
            processed += 1

        except FitParseError as e:
            print(f"  [error] {fit_path.name}  →  FIT parse error: {e}")
            failed += 1
        except Exception as e:
            print(f"  [error] {fit_path.name}  →  {e}")
            failed += 1

    print(f"\nDone: {processed} processed, {skipped} skipped, {failed} failed.")
    print(f"Track files written to: {output_dir.resolve()}")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    input_dir  = Path(FIT_DIR)
    output_dir = Path(TRACKS_DIR)

    if not input_dir.exists():
        print(f"Error: Input folder not found: {input_dir}")
        return

    print(f"Input:  {input_dir.resolve()}")
    print(f"Output: {output_dir.resolve()}")
    print()
    process_folder(input_dir, output_dir, overwrite=OVERWRITE)


if __name__ == "__main__":
    main()
