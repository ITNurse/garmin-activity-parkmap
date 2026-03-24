"""
01_preprocess_strava_files.py
================================
Three-phase pipeline for Strava activity exports:

  Phase 1 — Decompress: any .gpx.gz / .tcx.gz in STRAVA_DIR are decompressed
             in-place to plain .gpx / .tcx files.
  Phase 2 — Clean up:   the original .gz files are deleted.
  Phase 3 — Convert:    all .gpx / .tcx files are converted to .track.json
             and written to the output folder.

Input defaults to config.STRAVA_DIR. Output defaults to config.TRACKS_DIR
(or ./tracks if config is unavailable).

Output schema mirrors the existing Garmin .track.json files:
    {
        "meta": {
            "filename":              str,
            "sport":                 str | null,
            "sub_sport":             null,
            "start_time":            ISO 8601 str,
            "end_time":              ISO 8601 str,
            "total_distance_m":      float | null,  # TCX: from lap summaries; GPX: computed via Haversine
            "total_elapsed_time_s":  float | null,
            "avg_heart_rate":        int | null,
            "max_heart_rate":        int | null,
            "total_ascent_m":        float | null,  # computed from elevation
            "total_descent_m":       float | null,  # computed from elevation
            "avg_speed_ms":          float | null,
            "max_speed_ms":          float | null,  # TCX only
        },
        "first_lat":       float,
        "first_lon":       float,
        "first_timestamp": ISO 8601 str,
        "point_count":     int,
        "track": [
            {"lat": float, "lon": float, "t": str, "alt": float,
             "hr": int,    "spd": float},   # hr and spd omitted when absent
            ...
        ]
    }

Usage:
    # Run the full pipeline using paths from config:
    python 01_preprocess_strava_files.py

    # Override input / output:
    python 01_preprocess_strava_files.py --input /path/to/activities --output /path/to/tracks

    # Test on the first N files only (skips decompression/deletion of others):
    python 01_preprocess_strava_files.py --limit 10
"""

import argparse
import gzip
import json
import logging
import re
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

# =============================================================================
# CONFIGURATION
# =============================================================================
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import config

try:
    from config import STRAVA_DIR, TRACKS_DIR
    DEFAULT_INPUT_DIR  = STRAVA_DIR
    DEFAULT_OUTPUT_DIR = TRACKS_DIR
except ImportError:
    DEFAULT_INPUT_DIR  = None
    DEFAULT_OUTPUT_DIR = Path("./tracks")

# XML namespaces
NS_GPX       = "http://www.topografix.com/GPX/1/1"
NS_GPXTPX    = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"
NS_TCX       = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
NS_TCX_EXT   = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"

# =============================================================================
# LOGGING — file only, no terminal output
# =============================================================================

LOG_FILENAME = "strava_preprocess.log"

def setup_logger(output_dir: Path) -> logging.Logger:
    """Configure a file-only logger that writes to output_dir/strava_preprocess.log."""
    log_path = output_dir / LOG_FILENAME
    logger = logging.getLogger("strava")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # don't pass to root logger (which might print to terminal)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(fh)
    return logger

log = logging.getLogger("strava")  # module-level reference; populated after setup_logger()

# =============================================================================
# PROGRESS BAR
# =============================================================================

def print_progress(current: int, total: int, prefix: str = "Converting") -> None:
    """Print an in-place terminal progress bar."""
    if total == 0:
        return
    bar_width = 40
    filled = int(bar_width * current / total)
    bar = "█" * filled + "░" * (bar_width - filled)
    pct = int(100 * current / total)
    # \r returns to start of line; end="" prevents newline; flush forces immediate display
    print(f"\r{prefix}: [{bar}] {pct:3d}%  {current}/{total}", end="", flush=True)
    if current == total:
        print()  # final newline when complete

# =============================================================================
# HELPERS
# =============================================================================

def read_file_bytes(path: Path) -> bytes:
    """Read a plain or gzip-compressed file, stripping any UTF-8 BOM."""
    if path.suffix.lower() == ".gz":
        with gzip.open(path, "rb") as f:
            data = f.read()
    else:
        with open(path, "rb") as f:
            data = f.read()
    # Strip UTF-8 BOM (0xEF 0xBB 0xBF) — present in some Strava TCX exports
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    # Strip leading whitespace — some Strava TCX exports have spaces before
    # the <?xml ...?> declaration, which is invalid and breaks ElementTree
    data = data.lstrip()
    return data

def parse_iso(ts: str) -> datetime | None:
    """Parse an ISO 8601 timestamp string into a UTC-aware datetime."""
    if not ts:
        return None
    ts = ts.strip()
    # Replace trailing Z with +00:00 for fromisoformat compatibility
    ts = re.sub(r"Z$", "+00:00", ts)
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None

def to_iso(dt: datetime | None) -> str | None:
    """Format a datetime as an ISO 8601 string without timezone suffix."""
    if dt is None:
        return None
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S")

def build_output_filename(start_time: datetime | None, source_path: Path) -> str:
    """
    Construct: YYYY_MM_DD_HHMMSS-<stem>.track.json
    where <stem> is everything before the first period in the source filename.
    e.g. 123456984.gpx.gz → 2021_11_07_163316-123456984.track.json
    Falls back to 'unknown_datetime' when start_time is unavailable.
    """
    if start_time:
        dt_str = start_time.astimezone(timezone.utc).strftime("%Y_%m_%d_%H%M%S")
    else:
        dt_str = "unknown_datetime"
    stem = source_path.name.split(".")[0]
    return f"{dt_str}-{stem}.track.json"

def compute_ascent_descent(track: list[dict]) -> tuple[float | None, float | None]:
    """
    Compute total ascent and descent in metres from a list of track points.
    Only counts changes >= 0.5 m to reduce GPS noise.
    Returns (ascent_m, descent_m) or (None, None) if no elevation data.
    """
    elevations = [p["alt"] for p in track if p.get("alt") is not None]
    if len(elevations) < 2:
        return None, None

    ascent = descent = 0.0
    threshold = 0.5  # metres
    for i in range(1, len(elevations)):
        diff = elevations[i] - elevations[i - 1]
        if diff > threshold:
            ascent += diff
        elif diff < -threshold:
            descent += abs(diff)
    return round(ascent, 1), round(descent, 1)

def compute_avg_max_hr(track: list[dict]) -> tuple[int | None, int | None]:
    """Return (avg_hr, max_hr) from track points, or (None, None)."""
    hrs = [p["hr"] for p in track if p.get("hr") is not None]
    if not hrs:
        return None, None
    return round(sum(hrs) / len(hrs)), max(hrs)

def compute_distance_m(track: list[dict]) -> float | None:
    """
    Compute total distance in metres from lat/lon track points using the
    Haversine formula. Returns None if fewer than 2 points have coordinates.
    """
    import math
    R = 6_371_000  # Earth radius in metres
    total = 0.0
    prev = None
    for p in track:
        if p.get("lat") is None or p.get("lon") is None:
            continue
        if prev is not None:
            lat1, lon1 = math.radians(prev["lat"]), math.radians(prev["lon"])
            lat2, lon2 = math.radians(p["lat"]),    math.radians(p["lon"])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
            total += R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        prev = p
    return round(total, 2) if total > 0 else None

def compute_avg_speed(track: list[dict], elapsed_s: float | None) -> float | None:
    """
    Estimate avg speed (m/s) from per-point speed values.
    """
    speeds = [p["spd"] for p in track if p.get("spd") is not None]
    if speeds:
        return round(sum(speeds) / len(speeds), 3)
    return None

# =============================================================================
# PHASE 1 & 2 — DECOMPRESS AND CLEAN UP .gz FILES
# =============================================================================

def decompress_gz_files(strava_dir: Path) -> tuple[int, int]:
    """
    Phase 1: Decompress all .gpx.gz and .tcx.gz files in strava_dir in-place.
    Phase 2: Delete the original .gz files after successful decompression.
    Returns (decompressed_count, error_count).
    """
    gz_files = sorted(
        p for p in strava_dir.iterdir()
        if p.suffix.lower() == ".gz"
    )

    if not gz_files:
        log.info("No .gz files found in %s — nothing to decompress.", strava_dir)
        return 0, 0

    log.info("Phase 1/2 — Decompressing %d .gz file(s) in %s", len(gz_files), strava_dir)
    decompressed = errors = 0

    for gz_path in gz_files:
        out_path = gz_path.with_suffix("")  # e.g. 123.tcx.gz -> 123.tcx

        # .fit.gz and other non-GPX/TCX formats: just delete, we can't convert them
        is_convertible = any(s in gz_path.name.lower() for s in (".gpx.", ".tcx."))
        if not is_convertible:
            try:
                gz_path.unlink()
                log.info("Deleted non-convertible compressed file: %s", gz_path.name)
                decompressed += 1
            except Exception as e:
                log.error("Failed to delete %s: %s", gz_path.name, e)
                errors += 1
            continue

        try:
            if not out_path.exists():
                data = gzip.decompress(gz_path.read_bytes())
                out_path.write_bytes(data)
                log.info("Decompressed: %s -> %s", gz_path.name, out_path.name)
            else:
                log.info("Already decompressed: %s (plain file exists)", gz_path.name)

            # Always attempt deletion now that we know the plain file exists
            gz_path.unlink()
            log.info("Deleted compressed file: %s", gz_path.name)
            decompressed += 1

        except Exception as e:
            log.error("Failed processing %s: %s", gz_path.name, e)
            errors += 1

    log.info(
        "Decompression complete — %d processed, %d errors.",
        decompressed, errors
    )
    return decompressed, errors


# =============================================================================
# GPX PARSER
# =============================================================================

def _find(element, tag: str, ns: str) -> ET.Element | None:
    return element.find(f"{{{ns}}}{tag}")

def _findtext(element, tag: str, ns: str) -> str | None:
    el = _find(element, tag, ns)
    return el.text.strip() if el is not None and el.text else None

def parse_gpx(path: Path) -> dict | None:
    """
    Parse a .gpx or .gpx.gz file into the .track.json structure.
    Handles the Strava GPX dialect including optional heart rate extensions.
    """
    try:
        root = ET.fromstring(read_file_bytes(path))
    except Exception as e:
        log.error("Failed to parse GPX %s: %s", path.name, e)
        return None

    ns = NS_GPX

    # ---- Activity name and type ----
    trk = _find(root, "trk", ns)
    if trk is None:
        log.warning("No <trk> element found in %s", path.name)
        return None

    activity_name = _findtext(trk, "n", ns) or _findtext(trk, "name", ns)
    activity_type = _findtext(trk, "type", ns)

    # ---- Track points ----
    track = []
    for seg in trk.findall(f"{{{ns}}}trkseg"):
        for trkpt in seg.findall(f"{{{ns}}}trkpt"):
            try:
                lat = float(trkpt.attrib["lat"])
                lon = float(trkpt.attrib["lon"])
            except (KeyError, ValueError):
                continue

            ele_el = _find(trkpt, "ele", ns)
            alt = round(float(ele_el.text), 1) if ele_el is not None and ele_el.text else None

            time_el = _find(trkpt, "time", ns)
            ts = parse_iso(time_el.text) if time_el is not None and time_el.text else None

            point = {
                "lat": round(lat, 7),
                "lon": round(lon, 7),
                "t":   to_iso(ts),
            }
            if alt is not None:
                point["alt"] = alt

            # Heart rate — Garmin TrackPointExtension or generic <hr>
            hr = None
            ext_el = _find(trkpt, "extensions", ns)
            if ext_el is not None:
                tpe = ext_el.find(f"{{{NS_GPXTPX}}}TrackPointExtension")
                if tpe is not None:
                    hr_el = tpe.find(f"{{{NS_GPXTPX}}}hr")
                    if hr_el is not None and hr_el.text:
                        try:
                            hr = int(hr_el.text)
                        except ValueError:
                            pass
                if hr is None:
                    hr_el = ext_el.find(f"{{{NS_GPXTPX}}}hr") or ext_el.find("hr")
                    if hr_el is not None and hr_el.text:
                        try:
                            hr = int(hr_el.text)
                        except ValueError:
                            pass
            if hr is not None:
                point["hr"] = hr

            track.append(point)

    if not track:
        log.warning("No track points found in %s", path.name)
        return None

    # ---- Timestamps ----
    timestamps = [parse_iso(p["t"]) for p in track if p.get("t")]
    start_time = min(timestamps) if timestamps else None
    end_time   = max(timestamps) if timestamps else None
    elapsed_s  = (end_time - start_time).total_seconds() if start_time and end_time else None

    # ---- Derived stats ----
    ascent_m, descent_m   = compute_ascent_descent(track)
    avg_hr, max_hr        = compute_avg_max_hr(track)
    avg_spd               = compute_avg_speed(track, elapsed_s)

    # ---- Build output ----
    first_point = track[0]
    result = {
        "meta": {
            "filename":             path.name,
            "sport":                activity_type,
            "sub_sport":            None,
            "start_time":           to_iso(start_time),
            "end_time":             to_iso(end_time),
            "total_distance_m":     compute_distance_m(track),  # computed from lat/lon
            "total_elapsed_time_s": round(elapsed_s, 2) if elapsed_s else None,
            "avg_heart_rate":       avg_hr,
            "max_heart_rate":       max_hr,
            "total_ascent_m":       ascent_m,
            "total_descent_m":      descent_m,
            "avg_speed_ms":         avg_spd,
            "max_speed_ms":         None,   # not in GPX
        },
        "first_lat":       first_point["lat"],
        "first_lon":       first_point["lon"],
        "first_timestamp": first_point["t"],
        "point_count":     len(track),
        "track":           track,
    }
    return result, start_time

# =============================================================================
# TCX PARSER
# =============================================================================

def parse_tcx(path: Path) -> dict | None:
    """
    Parse a .tcx or .tcx.gz file into the .track.json structure.
    TCX is richer than GPX: includes laps, distance, speed, cadence, and HR.
    """
    try:
        root = ET.fromstring(read_file_bytes(path))
    except Exception as e:
        log.error("Failed to parse TCX %s: %s", path.name, e)
        return None

    # Some TCX files omit the XML namespace entirely — detect and handle both
    ns = NS_TCX if root.tag.startswith("{") else ""

    def tcx(tag: str) -> str:
        return f"{{{ns}}}{tag}" if ns else tag

    # ---- Activity ----
    activities_el = root.find(tcx("Activities"))
    if activities_el is None:
        log.warning("No <Activities> element in %s", path.name)
        return None

    activity_el = activities_el.find(tcx("Activity"))
    if activity_el is None:
        log.warning("No <Activity> element in %s", path.name)
        return None

    activity_type = activity_el.attrib.get("Sport", None)

    # ---- Collect laps and track points ----
    track = []
    total_distance_m  = None
    max_speed_ms      = None

    for lap_el in activity_el.findall(tcx("Lap")):
        # Use direct children only to avoid picking up trackpoint-level <DistanceMeters>
        dist_el = None
        for child in lap_el:
            if child.tag == tcx("DistanceMeters"):
                dist_el = child
                break
        if dist_el is not None and dist_el.text:
            try:
                total_distance_m = (total_distance_m or 0) + float(dist_el.text)
            except ValueError:
                pass

        # Max speed per lap
        mspd_el = lap_el.find(tcx("MaximumSpeed"))
        if mspd_el is not None and mspd_el.text:
            try:
                spd = float(mspd_el.text)
                max_speed_ms = max(max_speed_ms or 0, spd)
            except ValueError:
                pass

        for trk_el in lap_el.findall(tcx("Track")):
            for tp_el in trk_el.findall(tcx("Trackpoint")):
                # Timestamp
                time_el = tp_el.find(tcx("Time"))
                ts = parse_iso(time_el.text) if time_el is not None and time_el.text else None

                # Position
                pos_el = tp_el.find(tcx("Position"))
                if pos_el is None:
                    continue
                lat_el = pos_el.find(tcx("LatitudeDegrees"))
                lon_el = pos_el.find(tcx("LongitudeDegrees"))
                if lat_el is None or lon_el is None:
                    continue
                try:
                    lat = round(float(lat_el.text), 7)
                    lon = round(float(lon_el.text), 7)
                except (TypeError, ValueError):
                    continue

                # Elevation
                alt = None
                alt_el = tp_el.find(tcx("AltitudeMeters"))
                if alt_el is not None and alt_el.text:
                    try:
                        alt = round(float(alt_el.text), 1)
                    except ValueError:
                        pass

                # Heart rate
                hr = None
                hr_bpm_el = tp_el.find(tcx("HeartRateBpm"))
                if hr_bpm_el is not None:
                    val_el = hr_bpm_el.find(tcx("Value"))
                    if val_el is not None and val_el.text:
                        try:
                            hr = int(val_el.text)
                        except ValueError:
                            pass

                # Speed (from ActivityExtension)
                spd = None
                ext_el = tp_el.find(tcx("Extensions"))
                if ext_el is not None:
                    tpx = ext_el.find(f"{{{NS_TCX_EXT}}}TPX")
                    if tpx is not None:
                        spd_el = tpx.find(f"{{{NS_TCX_EXT}}}Speed")
                        if spd_el is not None and spd_el.text:
                            try:
                                spd = round(float(spd_el.text), 3)
                            except ValueError:
                                pass

                point = {
                    "lat": lat,
                    "lon": lon,
                    "t":   to_iso(ts),
                }
                if alt is not None:
                    point["alt"] = alt
                if hr is not None:
                    point["hr"] = hr
                if spd is not None:
                    point["spd"] = spd

                track.append(point)

    if not track:
        log.warning("No track points found in %s", path.name)
        return None

    # ---- Timestamps ----
    timestamps = [parse_iso(p["t"]) for p in track if p.get("t")]
    start_time = min(timestamps) if timestamps else None
    end_time   = max(timestamps) if timestamps else None
    elapsed_s  = (end_time - start_time).total_seconds() if start_time and end_time else None

    # ---- Derived stats ----
    ascent_m, descent_m = compute_ascent_descent(track)
    avg_hr, max_hr      = compute_avg_max_hr(track)
    speeds = [p["spd"] for p in track if p.get("spd") is not None]
    avg_spd = round(sum(speeds) / len(speeds), 3) if speeds else None

    # ---- Build output ----
    first_point = track[0]
    result = {
        "meta": {
            "filename":             path.name,
            "sport":                activity_type.lower() if activity_type else None,
            "sub_sport":            None,
            "start_time":           to_iso(start_time),
            "end_time":             to_iso(end_time),
            "total_distance_m":     round(total_distance_m, 2) if total_distance_m is not None else None,
            "total_elapsed_time_s": round(elapsed_s, 2) if elapsed_s else None,
            "avg_heart_rate":       avg_hr,
            "max_heart_rate":       max_hr,
            "total_ascent_m":       ascent_m,
            "total_descent_m":      descent_m,
            "avg_speed_ms":         avg_spd,
            "max_speed_ms":         round(max_speed_ms, 3) if max_speed_ms else None,
        },
        "first_lat":       first_point["lat"],
        "first_lon":       first_point["lon"],
        "first_timestamp": first_point["t"],
        "point_count":     len(track),
        "track":           track,
    }
    return result, start_time

# =============================================================================
# DISPATCH
# =============================================================================

def convert_file(path: Path, output_dir: Path) -> tuple[bool, str | None]:
    """
    Detect format, parse, and write a .track.json file.
    Returns (success, output_filename_or_None).
    """
    suffixes = [s.lower() for s in path.suffixes]

    if ".gpx" in suffixes:
        parse_result = parse_gpx(path)
    elif ".tcx" in suffixes:
        parse_result = parse_tcx(path)
    else:
        log.warning("Unsupported format, skipping: %s", path.name)
        return False, None

    if parse_result is None:
        return False, None

    data, start_time = parse_result
    out_filename = build_output_filename(start_time, path)
    out_path = output_dir / out_filename

    if out_path.exists():
        log.info(
            "SKIP   | original: %-40s | output: %s (already exists)",
            path.name, out_filename
        )
        return True, out_filename

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        return True, out_filename
    except OSError as e:
        log.error("Failed to write %s: %s", out_path, e)
        return False, None

# =============================================================================
# MAIN
# =============================================================================

SUPPORTED_SUFFIXES = {".gpx", ".tcx", ".gz"}

def collect_files(input_path: Path) -> list[Path]:
    """Return all supported activity files from a file or directory."""
    if input_path.is_file():
        return [input_path]
    files = []
    for p in sorted(input_path.iterdir()):
        suffixes = {s.lower() for s in p.suffixes}
        if suffixes & {".gpx", ".tcx"}:
            files.append(p)
    return files

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Three-phase Strava pipeline: decompress .gz files, delete originals, "
            "then convert all .gpx/.tcx to .track.json."
        )
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Folder containing Strava activity files (default: config.STRAVA_DIR).",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output folder for .track.json files (default: config.TRACKS_DIR).",
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Only convert the first N files — decompression still runs in full.",
    )
    args = parser.parse_args()

    if args.input is None:
        print("ERROR: No input folder specified and config.STRAVA_DIR is not set.")
        print("Pass --input <folder> or define STRAVA_DIR in config.py.")
        sys.exit(1)

    if not args.input.exists():
        print(f"ERROR: Input path does not exist: {args.input}")
        sys.exit(1)

    args.output.mkdir(parents=True, exist_ok=True)

    # Set up file logger now that output dir exists
    setup_logger(args.output)
    log.info("=" * 60)
    log.info("Strava preprocessing started")
    log.info("  Input  : %s", args.input)
    log.info("  Output : %s", args.output)
    log.info("=" * 60)

    # ---- Phase 1 & 2: Decompress .gz files and delete them ----
    print("Phase 1/2: Decompressing .gz files...")
    decompress_gz_files(args.input)

    # ---- Phase 3: Convert all .gpx / .tcx files ----
    files = collect_files(args.input)
    if not files:
        print(f"ERROR: No supported activity files found in: {args.input}")
        log.error("No supported activity files found in: %s", args.input)
        sys.exit(1)

    if args.limit is not None:
        files = files[:args.limit]
        log.info("Limiting conversion to first %d file(s)", args.limit)

    total   = len(files)
    success = errors = skipped = 0

    print(f"Phase 3:   Converting {total} file(s) to .track.json")
    print_progress(0, total)

    for i, path in enumerate(files, start=1):
        suffixes = [s.lower() for s in path.suffixes]
        if ".gpx" not in suffixes and ".tcx" not in suffixes:
            skipped += 1
            print_progress(i, total)
            continue

        # Was the original a compressed file? Check if a .gz version existed.
        # By this point decompression has already run, so we check by convention.
        was_compressed = any(
            s in path.name.lower() for s in (".gpx", ".tcx")
        ) and path.suffix.lower() in {".gpx", ".tcx"}
        # We can't know at this point if the source *was* a .gz — the .gz is
        # already gone if decompression succeeded. We note it in the log by
        # checking whether a .gz sibling existed before we started (not possible
        # here). Instead, we record the source extension for transparency.
        source_note = "was .gz (decompressed this run)" if not path.suffix.lower().endswith(".gz") else "plain file"

        ok, out_filename = convert_file(path, args.output)

        if ok and out_filename:
            log.info(
                "OK     | original: %-40s | output: %s",
                path.name, out_filename,
            )
            success += 1
        elif ok:
            skipped += 1
        else:
            log.error("FAIL   | original: %-40s | output: (failed)", path.name)
            errors += 1

        print_progress(i, total)

    # ---- Summary ----
    print(f"\nDone. {success} converted, {errors} errors, {skipped} skipped.")
    print(f"Log written to: {args.output / LOG_FILENAME}")

    log.info("=" * 60)
    log.info("CONVERSION COMPLETE")
    log.info("  Converted successfully : %d", success)
    log.info("  Errors                 : %d", errors)
    log.info("  Skipped                : %d", skipped)
    log.info("  Track files written to : %s", args.output)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
