"""
preprocess_fit_files.py
--------------------------
Batch-converts Garmin .fit activity files into lightweight .track.json files
that are used by the rest of the pipeline for mapping and analysis.

For each .fit file found in the input folder, the script:
  1. Checks a persistent index (fit_index.csv) to see if this file has been
     seen before. If it has, it is skipped without being opened.
  2. For new files: parses the binary .fit format and checks for GPS data.
  3. Records the result (True/False) in the index so the file is never
     opened again on future runs.
  4. If GPS data was found, extracts the full track and writes a .track.json.

Output files are named:  YYYY_MM_DD_HHMMSS-<original_filename>.track.json
This matches the naming convention used by the Strava pipeline so that
tracks from both sources sort and compare consistently by date.

The index file (fit_index.csv) is saved to config.DATA_OUTPUTS and has
two columns:
    filename   — the .fit filename (e.g. "789456123.fit")
    has_gps    — True if the file contains GPS points, False otherwise

This index makes repeated runs very efficient: on a run with no new files,
every file is found in the index and skipped immediately without being opened.

Usage:
    python 01_preprocess_fit_files.py

Configuration:
    Input and output folders are defined in config.py at the project root.
    FIT_DIR      — folder containing new .fit files to process  (config.RAW_DATA/new)
    TRACKS_DIR   — folder where .track.json files are written   (config.TRACKS_DIR)
    INDEX_PATH   — path to the persistent index CSV             (config.DATA_OUTPUTS/fit_index.csv)
"""

import sys
import os

# ---------------------------------------------------------------------------
# Path setup — make sure Python can find config.py at the project root.
#
# __file__ is the path to this script. We go up two levels (/../..) to reach
# the project root, then insert that folder at the front of sys.path.
# sys.path is the list of folders Python searches when you write "import X".
# Inserting at index 0 means it's checked first, before any installed packages.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import config  # config.py lives at the project root and defines shared folder paths

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fitparse import FitFile, FitParseError  # fitparse reads binary .fit files from Garmin devices


# =============================================================================
# CONFIGURATION — paths pulled from config.py at project root
# =============================================================================

# The folder where new, unprocessed .fit files are waiting to be converted.
FIT_DIR    = config.RAW_DATA / "new"

# The folder where we'll write the output .track.json files.
TRACKS_DIR = config.TRACKS_DIR

# The persistent index CSV that records which .fit files have been seen before
# and whether they contained GPS data. Saved alongside other data outputs.
INDEX_PATH = config.DATA_OUTPUTS / "fit_index.csv"

# If False, files that already have a corresponding output will be skipped.
# Set to True if you want to force re-processing (e.g. after changing the output format).
OVERWRITE  = False

# Garmin devices store GPS coordinates in "semicircles" rather than decimal degrees.
# Multiplying by this constant converts semicircles → decimal degrees.
# The formula is: 180 degrees / 2^31 semicircles.
SEMICIRCLES_TO_DEGREES = 180 / (2 ** 31)


# =============================================================================
# INDEX MANAGEMENT — load and save the persistent record of seen files
# =============================================================================

def load_index(index_path: Path) -> dict[str, bool]:
    """
    Load the persistent GPS index from a CSV file and return it as a dict.

    The index records every .fit file that has ever been opened by this script,
    along with whether it contained GPS points. This lets future runs skip
    files instantly without opening them.

    The CSV has two columns:
        filename  — the .fit filename, e.g. "789456123.fit"
        has_gps   — the string "True" or "False"

    Returns a dict mapping filename → has_gps (as a boolean), e.g.:
        {
            "789456123.fit": True,
            "987654321.fit": False,
        }

    If the index file doesn't exist yet (first ever run), returns an empty dict.
    """
    if not index_path.exists():
        # First run — no index yet. Return empty dict so everything gets processed.
        return {}

    index = {}
    with open(index_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)  # Reads each row as a dict keyed by column header
        for row in reader:
            # Convert the "True"/"False" string back into an actual Python boolean
            index[row["filename"]] = row["has_gps"] == "True"

    return index


def save_index(index: dict[str, bool], index_path: Path) -> None:
    """
    Write the in-memory index dict back out to the CSV file.

    Called once at the end of each run (after all files have been processed)
    so the updated index is ready for next time.

    Arguments:
        index       — dict mapping filename → has_gps boolean
        index_path  — where to write the CSV file
    """
    # Make sure the output folder exists before trying to write the file
    index_path.parent.mkdir(parents=True, exist_ok=True)

    with open(index_path, "w", encoding="utf-8", newline="") as f:
        # DictWriter writes dicts as CSV rows using the given fieldnames as column headers
        writer = csv.DictWriter(f, fieldnames=["filename", "has_gps"])
        writer.writeheader()  # Write the "filename,has_gps" header row

        # Sort by filename so the CSV is human-readable and easy to inspect
        for filename, has_gps in sorted(index.items()):
            writer.writerow({"filename": filename, "has_gps": has_gps})


# =============================================================================
# FILENAME BUILDER
# =============================================================================

def build_output_filename(start_time: str | None, fit_stem: str) -> str:
    """
    Build the output filename for a processed track file.

    The format is:  YYYY_MM_DD_HHMMSS-<original_fit_filename>.track.json
    This naming scheme matches the convention used by the Strava pipeline so
    that both sources of tracks sort and compare consistently by date.

    Arguments:
        start_time  — ISO 8601 datetime string from the .fit file's session data,
                      e.g. "2024-07-28T10:32:00". May be None if not found.
        fit_stem    — The .fit filename without its extension, e.g. "my_activity"

    Returns a filename string. If start_time is missing or can't be parsed,
    falls back to "unknown_datetime-<fit_stem>.track.json" so the file is
    still written rather than silently dropped.
    """
    if start_time:
        try:
            # datetime.fromisoformat() parses an ISO 8601 string into a datetime object.
            # strftime() then formats it as a string in our desired layout.
            dt = datetime.fromisoformat(start_time)
            return dt.strftime("%Y_%m_%d_%H%M%S") + f"-{fit_stem}.track.json"
        except ValueError:
            # fromisoformat() raises ValueError if the string isn't a valid datetime.
            # We catch it and fall through to the fallback below.
            pass

    # Fallback: use a placeholder date so we still produce an output file.
    return f"unknown_datetime-{fit_stem}.track.json"


# =============================================================================
# FIT PARSING — convert the binary .fit file into a Python data structure
# =============================================================================

def parse_fit_file(fit_path: Path) -> dict:
    """
    Read a .fit binary file and convert its entire contents into a plain
    Python dictionary that's easy to work with.

    A .fit file is structured as a sequence of "messages", each of which
    has a name (e.g. "record", "session", "lap") and a list of fields
    (e.g. heart_rate=142, position_lat=123456789).

    This function doesn't filter or interpret anything — it just extracts
    everything into a standard Python structure. The actual GPS track
    extraction and filtering happens in extract_track() below.

    Returns a dict shaped like:
        {
            "filename": "my_activity.fit",
            "messages": [
                {
                    "name": "record",
                    "fields": [
                        {"name": "heart_rate", "value": 142},
                        {"name": "position_lat", "value": 123456789},
                        ...
                    ]
                },
                ...
            ]
        }
    """
    ff = FitFile(str(fit_path))  # Open and decode the binary .fit file
    messages = []

    for msg in ff.get_messages():
        fields = []
        for field in msg.fields:
            val = field.value

            # datetime objects can't be serialised to JSON directly.
            # Convert them to ISO 8601 strings (e.g. "2024-07-28T10:32:00")
            # so the data can be safely written to a .json file later.
            if isinstance(val, datetime):
                val = val.isoformat()

            fields.append({"name": field.name, "value": val})

        messages.append({"name": msg.name, "fields": fields})

    return {"filename": fit_path.name, "messages": messages}


# =============================================================================
# TRACK EXTRACTION — pull out the metadata and GPS points we care about
# =============================================================================

def get_field_value(fields: list, name: str):
    """
    Search a list of field dicts for one with a matching name, and return its value.

    This is a small helper used repeatedly in extract_track() to avoid
    writing the same search loop over and over.

    Arguments:
        fields  — a list of dicts like [{"name": "heart_rate", "value": 142}, ...]
        name    — the field name to look for, e.g. "heart_rate"

    Returns the value if found, or None if no field with that name exists.
    """
    for f in fields:
        if f["name"] == name:
            return f["value"]
    return None  # Explicit None return makes it clear "not found" is an expected outcome


def extract_track(data: dict) -> dict | None:
    """
    Parse the raw message data from parse_fit_file() and extract two things:

      1. Metadata ("meta"): summary statistics for the whole activity, pulled
         from "session", "lap", and "sport" messages. Things like sport type,
         start time, total distance, heart rate, etc.

      2. Track points: the timestamped GPS positions recorded throughout the
         activity, pulled from "record" messages. Each point includes lat/lon
         and optionally timestamp, altitude, heart rate, and speed.

    Returns a dict containing both, or None if no GPS points were found
    (which would make the file useless for mapping purposes).

    The returned dict is shaped like:
        {
            "meta":            { sport, start_time, total_distance_m, ... },
            "first_lat":       45.123,
            "first_lon":       -81.456,
            "first_timestamp": "2024-07-28T10:32:00",
            "point_count":     1234,
            "track":           [ {"lat": ..., "lon": ..., "t": ..., ...}, ... ]
        }
    """
    messages = data.get("messages", [])
    filename = data.get("filename", "unknown.fit")

    # Initialise the metadata dict with None for every field we want to capture.
    # We'll fill these in as we find the relevant messages below.
    meta = {
        "filename":              filename,
        "sport":                 None,  # e.g. "hiking", "running", "cycling"
        "sub_sport":             None,  # e.g. "trail", "road"
        "start_time":            None,  # ISO datetime string
        "end_time":              None,
        "total_distance_m":      None,  # Total distance in metres
        "total_elapsed_time_s":  None,  # Total time including pauses, in seconds
        "avg_heart_rate":        None,  # Beats per minute
        "max_heart_rate":        None,
        "total_ascent_m":        None,  # Cumulative elevation gain in metres
        "total_descent_m":       None,
        "avg_speed_ms":          None,  # Average speed in metres per second
        "max_speed_ms":          None,
    }

    # -------------------------------------------------------------------------
    # Pass 1: scan all messages for metadata.
    # We use "if meta[...] is None" guards so that the first value found wins
    # and later duplicate messages (some .fit files have multiple sessions) don't
    # overwrite it.
    # -------------------------------------------------------------------------
    for msg in messages:
        name   = msg.get("name")
        fields = msg.get("fields", [])

        if name == "session":
            # The "session" message is the primary source for activity summary data.
            if meta["sport"] is None:
                meta["sport"]     = get_field_value(fields, "sport")
                meta["sub_sport"] = get_field_value(fields, "sub_sport")
            if meta["start_time"] is None:
                meta["start_time"] = get_field_value(fields, "start_time")
            if meta["end_time"] is None:
                # "timestamp" on the session message marks when the session ended
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
                # Some devices use "enhanced_avg_speed" instead of "avg_speed".
                # The "or" here tries the first field and falls back to the second if it's None/falsy.
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
            # "lap" messages sometimes carry sport info when "session" doesn't.
            # Only use it as a fallback if we haven't found sport yet.
            meta["sport"]     = get_field_value(fields, "sport")
            meta["sub_sport"] = get_field_value(fields, "sub_sport")

        elif name == "sport":
            # The dedicated "sport" message can have more specific sport info.
            # We only override if the current value is missing or the unhelpful default "generic".
            INVALID = {None, "generic"}
            if meta["sport"] in INVALID:
                sport = get_field_value(fields, "sport")
                if sport is not None:
                    meta["sport"] = sport
            if meta["sub_sport"] in INVALID:
                sub_sport = get_field_value(fields, "sub_sport")
                if sub_sport is not None:
                    meta["sub_sport"] = sub_sport

    # -------------------------------------------------------------------------
    # Pass 2: extract GPS track points from "record" messages.
    # Each "record" message represents one point in time during the activity.
    # -------------------------------------------------------------------------
    track_points = []
    first_lat = first_lon = first_ts = None  # We'll capture the very first point separately

    for msg in messages:
        # Skip any message that isn't a GPS record
        if msg.get("name") != "record":
            continue

        # Convert the list of field dicts into a plain {name: value} dict
        # so we can look up values quickly with fv.get("field_name")
        fv = {f["name"]: f["value"] for f in msg.get("fields", [])}

        # lat and lon are required — skip this record if either is missing
        lat_sc = fv.get("position_lat")
        lon_sc = fv.get("position_long")  # Note: Garmin uses "position_long", not "position_lon"
        if lat_sc is None or lon_sc is None:
            continue

        # Convert from Garmin semicircles to decimal degrees and round to 7 decimal places
        # (7 decimal places = ~1 cm precision, more than enough for any GPS use case)
        lat = round(lat_sc * SEMICIRCLES_TO_DEGREES, 7)
        lon = round(lon_sc * SEMICIRCLES_TO_DEGREES, 7)

        ts  = fv.get("timestamp")

        # For altitude and speed, try the "enhanced" version first (higher precision),
        # then fall back to the standard version if enhanced isn't available.
        alt = fv.get("enhanced_altitude") or fv.get("altitude")
        hr  = fv.get("heart_rate")
        spd = fv.get("enhanced_speed") or fv.get("speed")

        # Start with the required fields, then only add optional fields if they exist.
        # This keeps the output compact — no "alt": null entries cluttering the JSON.
        point = {"lat": lat, "lon": lon}
        if ts  is not None: point["t"]   = ts
        if alt is not None: point["alt"] = round(alt, 1)
        if hr  is not None: point["hr"]  = hr
        if spd is not None: point["spd"] = round(spd, 3)

        track_points.append(point)

        # Capture the very first valid GPS point's coordinates and timestamp.
        # "is None" check means this only runs once (on the first point).
        if first_lat is None:
            first_lat, first_lon, first_ts = lat, lon, ts

    # If no GPS points were found at all, this file can't be used for mapping.
    # Return None to signal that the caller should skip/warn on this file.
    if not track_points:
        return None

    # If the session metadata didn't include a start_time, use the first GPS
    # record's timestamp as a reasonable approximation.
    if meta["start_time"] is None:
        meta["start_time"] = first_ts

    return {
        "meta":            meta,
        "first_lat":       first_lat,   # Useful for quick map previews without parsing all points
        "first_lon":       first_lon,
        "first_timestamp": first_ts,
        "point_count":     len(track_points),
        "track":           track_points,
    }


# =============================================================================
# FOLDER PROCESSING — find all .fit files and process them in bulk
# =============================================================================

def process_folder(input_dir: Path, output_dir: Path, index: dict[str, bool], overwrite: bool = False):
    """
    Scan input_dir for .fit files, convert each one to a .track.json file,
    and write the results to output_dir.

    Uses the index dict to skip files that have been seen on a previous run,
    so that only truly new files are ever opened and parsed.

    For each new file, the pipeline is:
        parse_fit_file()  →  extract_track()  →  update index  →  write .track.json

    The index is updated in-place as files are processed. The caller is
    responsible for saving it to disk afterwards (via save_index()).

    Prints a one-line status for each file ([ok], [skip], [no gps], or [error])
    and a summary at the end.

    Arguments:
        input_dir  — folder to scan for .fit files
        output_dir — folder to write .track.json files into
        index      — the in-memory index dict (modified in-place)
        overwrite  — if True, re-process files even if already in TRACKS_DIR
    """
    # Create the output folder if it doesn't exist yet.
    # parents=True also creates any missing parent folders.
    # exist_ok=True means no error if it already exists.
    output_dir.mkdir(parents=True, exist_ok=True)

    # glob("*.fit") returns all files in input_dir matching the pattern.
    # sorted() processes them in alphabetical order for predictable output.
    fit_files = sorted(input_dir.glob("*.fit"))

    if not fit_files:
        print(f"No .fit files found in {input_dir}")
        return

    # Counters for the summary line at the end
    processed = skipped_index = skipped_exists = no_gps = failed = 0

    for fit_path in fit_files:
        try:
            # ------------------------------------------------------------------
            # Check 1: Has this file been seen before?
            # If the filename is already in the index, we opened it on a previous
            # run and recorded the result. No need to open it again.
            # ------------------------------------------------------------------
            if not overwrite and fit_path.name in index:
                if index[fit_path.name]:
                    # has_gps=True: it was processed before. Check if the output
                    # file still exists (it could have been manually deleted).
                    already_done = any(
                        fit_path.stem in existing.name
                        for existing in output_dir.glob("*.track.json")
                    )
                    if already_done:
                        print(f"  [skip]  {fit_path.name}  →  already in index and output exists")
                        skipped_index += 1
                        continue
                    # Output file is missing even though we processed it before —
                    # fall through and re-process it rather than silently skipping.
                    print(f"  [reprocess]  {fit_path.name}  →  in index but output missing, re-processing")
                else:
                    # has_gps=False: we already know this file has no GPS data.
                    # Skip it immediately without opening it.
                    print(f"  [skip]  {fit_path.name}  →  no GPS (from index)")
                    skipped_index += 1
                    continue

            # ------------------------------------------------------------------
            # This is a new file (not in the index). Parse it and record the result.
            # ------------------------------------------------------------------

            # Step 1: Read the raw .fit binary into a Python dict
            data   = parse_fit_file(fit_path)

            # Step 2: Extract the metadata and GPS track points we want
            result = extract_track(data)

            if result is None:
                # No GPS points found — record this in the index so we never
                # open this file again, then move on.
                print(f"  [no gps] {fit_path.name}  →  no GPS data, recorded in index")
                index[fit_path.name] = False
                no_gps += 1
                continue

            # GPS data found — record this in the index
            index[fit_path.name] = True

            # Step 3: Determine the output filename based on the activity's start time
            out_path = output_dir / build_output_filename(
                result["meta"].get("start_time"), fit_path.stem
            )

            # Step 4: Skip writing the file if the output already exists
            # (can happen if the index was cleared but TRACKS_DIR was not)
            if out_path.exists() and not overwrite:
                print(f"  [skip]  {fit_path.name}  →  output file already exists")
                skipped_exists += 1
                continue

            # Step 5: Write the result to a compact JSON file.
            # separators=(",", ":") removes spaces after commas and colons,
            # producing the smallest possible valid JSON (no pretty-printing).
            with open(out_path, "w") as f:
                json.dump(result, f, separators=(",", ":"))

            # Print a status line showing the file sizes and compression ratio
            kb_in  = fit_path.stat().st_size / 1024
            kb_out = out_path.stat().st_size  / 1024
            ratio  = kb_in / kb_out if kb_out > 0 else 0
            print(
                f"  [ok]    {fit_path.name}  →  {out_path.name}"
                f"  ({kb_in:.0f} KB → {kb_out:.0f} KB, {ratio:.1f}x smaller)"
            )
            processed += 1

        except FitParseError as e:
            # FitParseError means the file is corrupt or not a valid .fit file
            print(f"  [error] {fit_path.name}  →  FIT parse error: {e}")
            failed += 1
        except Exception as e:
            # Catch-all for any other unexpected error so one bad file doesn't
            # stop the whole batch from processing
            print(f"  [error] {fit_path.name}  →  {e}")
            failed += 1

    # Print a summary once all files have been processed
    print(f"\nDone:")
    print(f"  {processed} processed and written")
    print(f"  {skipped_index} skipped (already in index)")
    print(f"  {skipped_exists} skipped (output file already exists)")
    print(f"  {no_gps} had no GPS data (recorded in index)")
    print(f"  {failed} errors")
    print(f"\nTrack files written to: {output_dir.resolve()}")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """
    Entry point for the script. Loads the index, runs the batch processing,
    then saves the updated index back to disk.

    The index is loaded at the start and saved at the end — even if no new
    files were processed, this is a fast no-op since the index hasn't changed.
    """
    input_dir  = Path(FIT_DIR)
    output_dir = Path(TRACKS_DIR)
    index_path = Path(INDEX_PATH)

    if not input_dir.exists():
        print(f"Error: Input folder not found: {input_dir}")
        return  # Exit gracefully rather than crashing with a traceback

    print(f"Input:  {input_dir.resolve()}")   # .resolve() prints the full absolute path
    print(f"Output: {output_dir.resolve()}")
    print(f"Index:  {index_path.resolve()}")
    print()

    # Load the index from disk (returns empty dict if file doesn't exist yet)
    index = load_index(index_path)
    print(f"Index loaded: {len(index)} files on record "
          f"({sum(index.values())} with GPS, {sum(not v for v in index.values())} without)")
    print()

    # Run the main processing loop, passing the index in so it can be updated
    process_folder(input_dir, output_dir, index, overwrite=OVERWRITE)

    # Save the updated index back to disk so next run benefits from it
    save_index(index, index_path)
    print(f"\nIndex saved: {len(index)} total files on record → {index_path.resolve()}")


# Only call main() if this script is being run directly (e.g. `python preprocess_fit_files.py`).
# If another script imports this file, main() won't run automatically.
if __name__ == "__main__":
    main()