#!/usr/bin/env python3
"""
03_match_geojson_parks.py
--------------------------
Matches GPS .track.json files against ALL GeoJSON park boundary files found
in config.PARKS_DIR.

Each GeoJSON file is processed in turn. If a track matches parks from multiple
files, or multiple parks within the same file, each match gets its own entry
in the "parks" list:

    "parks": [
        {"name": "Algonquin Provincial Park", "source": "ontario_provincial_parks.geojson"},
        {"name": "Frontenac Provincial Park",  "source": "ontario_provincial_parks.geojson"},
        {"name": "Frontenac Arch Biosphere",   "source": "biosphere_reserves.geojson"}
    ]

Uses only the Python standard library — no shapely, no geopandas required.

Usage:
    # Run against all GeoJSON files in config.PARKS_DIR:
    python src/pipeline/03_match_geojson_parks.py

    # List property keys in a specific GeoJSON (helps find the right --name-field):
    python src/pipeline/03_match_geojson_parks.py --inspect <file.geojson>

    # Adjust the match threshold (default: 20% of GPS points inside boundary):
    python src/pipeline/03_match_geojson_parks.py --threshold 0.10

    # Match by a fixed minimum number of points instead of a percentage:
    python src/pipeline/03_match_geojson_parks.py --min-points 50

    # Re-match files that already have a parks field:
    python src/pipeline/03_match_geojson_parks.py --overwrite

    # Test against just the first N track files:
    python src/pipeline/03_match_geojson_parks.py --limit 10

    # Skip interactive selection and process all GeoJSON files automatically:
    python src/pipeline/03_match_geojson_parks.py --all
"""

import sys
import json
import argparse
from pathlib import Path
from collections import Counter

# =============================================================================
# PROJECT ROOT  (script lives at src/pipeline/ — go up 3 levels)
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

TRACKS_DIR = config.TRACKS_DIR


# =============================================================================
# PURE-PYTHON POINT-IN-POLYGON  (ray casting algorithm)
# =============================================================================

def point_in_polygon(lat: float, lon: float, ring: list) -> bool:
    x, y = lon, lat
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_multipolygon(lat: float, lon: float, rings: list) -> bool:
    return any(point_in_polygon(lat, lon, ring) for ring in rings)


def make_bbox(rings: list) -> tuple:
    all_pts = [pt for ring in rings for pt in ring]
    lats = [p[1] for p in all_pts]
    lons = [p[0] for p in all_pts]
    return min(lats), max(lats), min(lons), max(lons)


def in_bbox(lat: float, lon: float, bbox: tuple) -> bool:
    return bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3]


# =============================================================================
# NAME FIELD AUTO-DETECTION
# =============================================================================

NAME_FIELD_CANDIDATES = [
    "PROTECTED_AREA_NAME_ENG",
    "park_name",  "PARK_NAME",
    "name",       "NAME",
    "Name",
    "NAME_E",     "name_e",
    "LABEL",      "label",
    "TITLE",      "title",
    "SITE_NAME",  "site_name",
    "PK_NAME",    "pk_name",
    "FULLNAME",   "fullname",
    "DESCRIPTIO", "description",
]


def detect_name_field(features: list) -> str | None:
    sample = features[:10]
    for candidate in NAME_FIELD_CANDIDATES:
        for feat in sample:
            val = (feat.get("properties") or {}).get(candidate)
            if val and str(val).strip():
                return candidate
    return None


def inspect_geojson(path: Path):
    """Print all unique property keys found in a GeoJSON file, then exit."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    if not features:
        print("No features found in GeoJSON.")
        return

    all_keys: Counter = Counter()
    for feat in features:
        for k in (feat.get("properties") or {}).keys():
            all_keys[k] += 1

    sample_vals = {}
    for feat in features[:20]:
        for k, v in (feat.get("properties") or {}).items():
            if k not in sample_vals and v is not None and str(v).strip():
                sample_vals[k] = str(v)[:60]

    print(f"\nGeoJSON : {path.name}")
    print(f"Features: {len(features)}")
    print(f"\nProperty keys (with example values):\n")
    for key, count in sorted(all_keys.items(), key=lambda x: -x[1]):
        example = sample_vals.get(key, "")
        marker  = "  <-- likely name field" if key in NAME_FIELD_CANDIDATES else ""
        print(f"  {key:<38} ({count:>4} features)   e.g. {example!r}{marker}")
    print()


# =============================================================================
# INTERACTIVE PARK FILE SELECTION
# =============================================================================

def prompt_geojson_selection(geojson_files: list[Path]) -> list[Path]:
    """
    Display a numbered list of available GeoJSON park files and prompt the
    user to choose which ones to process.

    Accepts:
        - A single number       e.g.  2
        - A comma-separated list  e.g.  1,3,4
        - A range               e.g.  2-5
        - Any combination       e.g.  1,3-5,7
        - 'all' or blank Enter  to select everything
        - 'q' or Ctrl-C         to quit
    """
    print("\n" + "=" * 60)
    print("Available GeoJSON park files:")
    print("=" * 60)
    for i, path in enumerate(geojson_files, start=1):
        print(f"  [{i:>2}]  {path.name}")
    print("=" * 60)
    print("Enter numbers to select files (e.g. 1  |  1,3  |  2-4  |  1,3-5,7)")
    print("Press Enter or type 'all' to process all files  |  'q' to quit")
    print("=" * 60)

    while True:
        try:
            raw = input("Your selection: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)

        if raw.lower() == "q":
            print("Aborted.")
            sys.exit(0)

        if raw == "" or raw.lower() == "all":
            print(f"  -> All {len(geojson_files)} file(s) selected.\n")
            return geojson_files

        # Parse numbers and ranges
        selected_indices: set[int] = set()
        valid = True
        for part in raw.split(","):
            part = part.strip()
            if "-" in part:
                bounds = part.split("-", 1)
                try:
                    lo, hi = int(bounds[0].strip()), int(bounds[1].strip())
                    if lo < 1 or hi > len(geojson_files) or lo > hi:
                        raise ValueError
                    selected_indices.update(range(lo, hi + 1))
                except (ValueError, IndexError):
                    print(f"  Invalid range '{part}'. Use numbers between 1 and {len(geojson_files)}.")
                    valid = False
                    break
            else:
                try:
                    n = int(part)
                    if n < 1 or n > len(geojson_files):
                        raise ValueError
                    selected_indices.add(n)
                except ValueError:
                    print(f"  Invalid value '{part}'. Use numbers between 1 and {len(geojson_files)}.")
                    valid = False
                    break

        if not valid:
            continue

        chosen = [geojson_files[i - 1] for i in sorted(selected_indices)]
        labels = ", ".join(p.name for p in chosen)
        print(f"  -> Selected: {labels}\n")
        return chosen


# =============================================================================
# PARK LOADER
# =============================================================================

def load_geojson_parks(path: Path, name_field: str | None = None) -> list:
    """
    Load park polygons from a GeoJSON FeatureCollection.
    Returns a list of dicts: {name, source, rings, bbox}
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    if not features:
        print(f"  No features found in {path.name}")
        return []

    if name_field is None:
        name_field = detect_name_field(features)
        if name_field:
            print(f"  Auto-detected name field : '{name_field}'")
        else:
            print(f"  WARNING: Could not auto-detect a name field — using sequential index.")

    source  = path.name
    parks   = []
    skipped = 0

    for i, feat in enumerate(features):
        props  = feat.get("properties") or {}
        geom   = feat.get("geometry")   or {}
        gtype  = geom.get("type")
        coords = geom.get("coordinates", [])

        if name_field and name_field in props:
            name = str(props[name_field]).strip() or f"Park_{i}"
        else:
            name = f"Park_{i}"

        if gtype == "Polygon":
            if not coords:
                skipped += 1
                continue
            rings = [coords[0]]
        elif gtype == "MultiPolygon":
            rings = [poly[0] for poly in coords if poly]
        else:
            skipped += 1
            continue

        if not rings:
            skipped += 1
            continue

        try:
            bbox = make_bbox(rings)
        except (ValueError, IndexError):
            skipped += 1
            continue

        parks.append({"name": name, "source": source, "rings": rings, "bbox": bbox})

    if skipped:
        print(f"  Skipped {skipped} features (unsupported geometry or empty coords)")
    print(f"  Loaded {len(parks)} parks from {path.name}")
    return parks


# =============================================================================
# MATCHING
# =============================================================================

def match_track_to_parks(
    track_points: list,
    parks: list,
    threshold: float,
    min_points: int | None = None,
) -> list:
    """
    Return one {"name": ..., "source": ...} entry for EACH park where the
    track's GPS points meet the match criterion:

      - min_points (int): at least this many points fall inside the boundary.
      - threshold  (float): at least this fraction of total points fall inside
                            the boundary (default mode).

    If --min-points is supplied it takes precedence over --threshold.
    A track can match multiple parks.
    """
    total = len(track_points)
    if total == 0:
        return []

    matched = []
    for park in parks:
        bbox_hits = [p for p in track_points if in_bbox(p["lat"], p["lon"], park["bbox"])]
        if not bbox_hits:
            continue

        inside_count = sum(
            1 for p in bbox_hits
            if point_in_multipolygon(p["lat"], p["lon"], park["rings"])
        )

        if min_points is not None:
            qualifies = inside_count >= min_points
        else:
            qualifies = inside_count / total >= threshold

        if qualifies:
            matched.append({"name": park["name"], "source": park["source"]})

    return matched


# =============================================================================
# PROCESS ALL TRACKS AGAINST ONE PARKS FILE
# =============================================================================

def process_tracks_for_source(
    tracks_dir: Path,
    parks: list,
    threshold: float,
    overwrite: bool,
    limit: int | None,
    track_files: list,
    min_points: int | None = None,
) -> int:
    """
    Match all track files against `parks` (from one source file).
    Returns the number of tracks that matched at least one park.
    """
    source_name = parks[0]["source"] if parks else ""
    matched_any = 0
    skipped     = 0
    updated     = 0
    errors      = 0

    for path in track_files:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [error] {path.name} — {e}")
            errors += 1
            continue

        existing = data.get("parks", [])

        already_matched = any(
            (m["source"] if isinstance(m, dict) else "") == source_name
            for m in existing
        )
        if already_matched and not overwrite:
            skipped += 1
            continue

        matched = match_track_to_parks(data.get("track", []), parks, threshold, min_points)

        # Remove previous entries from this source, then append fresh results
        kept = [
            m for m in existing
            if (m["source"] if isinstance(m, dict) else "") != source_name
        ]
        data["parks"] = kept + matched

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, separators=(",", ":"))
        except OSError as e:
            print(f"  [error] Could not write {path.name}: {e}")
            errors += 1
            continue

        updated += 1
        if matched:
            matched_any += 1
            labels = ", ".join(m["name"] for m in matched)
            print(f"  [match]  {path.stem[:52]:<54}  ->  {labels}")

    if skipped:
        print(f"  Skipped {skipped} track(s) already matched against this source (--overwrite to redo)")
    if errors:
        print(f"  Errors  : {errors}")

    return matched_any


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Match GPS .track.json files against GeoJSON park boundary files in config.PARKS_DIR.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--inspect",
        type=Path,
        default=None,
        metavar="FILE",
        help="Print all property keys in a specific GeoJSON file and exit",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.20,
        metavar="FLOAT",
        help="Min fraction of GPS points inside a park boundary to count as a match (default: 0.20)",
    )
    parser.add_argument(
        "--min-points",
        type=int,
        default=None,
        metavar="N",
        help="Match by a fixed minimum number of GPS points inside the boundary instead of a percentage. Takes precedence over --threshold when set.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-match tracks that already have results for a given source file",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N track files (useful for testing)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Skip interactive selection and process all GeoJSON files automatically",
    )
    args = parser.parse_args()

    # ---- Inspect mode ----
    if args.inspect:
        path = args.inspect if args.inspect.is_absolute() else PROJECT_ROOT / args.inspect
        if not path.exists():
            print(f"Error: File not found: {path}")
            sys.exit(1)
        inspect_geojson(path)
        sys.exit(0)

    # ---- Validate folders ----
    parks_dir  = Path(config.PARKS_DIR)
    tracks_dir = Path(TRACKS_DIR)

    if not parks_dir.exists():
        sys.exit(f"Error: Parks folder not found: {parks_dir}\nSet PARKS_DIR in config.py.")
    if not tracks_dir.exists():
        sys.exit(f"Error: Tracks folder not found: {tracks_dir}\nRun 01_preprocess_fit_files.py first.")

    # ---- Collect GeoJSON files ----
    geojson_files = sorted(parks_dir.glob("*.geojson"))
    if not geojson_files:
        print(f"No .geojson files found in {parks_dir}")
        sys.exit(0)

    # ---- Collect track files once (shared across all park sources) ----
    track_files = sorted(tracks_dir.rglob("*.track.json"))  # rglob: search all subfolders
    if args.limit:
        track_files = track_files[:args.limit]

    if not track_files:
        print(f"No .track.json files found in {tracks_dir}")
        sys.exit(0)

    # ---- Interactive park file selection (skipped if --all is passed) ----
    if args.all:
        selected_files = geojson_files
        print(f"--all flag set: processing all {len(geojson_files)} GeoJSON file(s).")
    else:
        selected_files = prompt_geojson_selection(geojson_files)

    print(f"Track files : {len(track_files)}")
    print(f"Parks files : {len(selected_files)}")
    if args.min_points is not None:
        print(f"Match mode  : at least {args.min_points} GPS points inside boundary")
    else:
        print(f"Match mode  : {int(args.threshold * 100)}% of GPS points inside boundary")

    # ---- Process each selected GeoJSON park file in turn ----
    total_matched = 0
    for parks_path in selected_files:
        print(f"\n{'='*60}")
        print(f"Parks file: {parks_path.name}")
        parks = load_geojson_parks(parks_path)
        if not parks:
            print("  No parks loaded — skipping.")
            continue
        total_matched += process_tracks_for_source(
            tracks_dir, parks, args.threshold, args.overwrite, args.limit, track_files,
            min_points=args.min_points,
        )

    # ---- Final cumulative summary ----
    print(f"\n{'='*60}")
    print(f"Finished. {total_matched} track(s) matched at least one park.\n")

    counts: Counter = Counter()
    for p in tracks_dir.rglob("*.track.json"):  # rglob: search all subfolders
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            for m in d.get("parks", []):
                name = m["name"] if isinstance(m, dict) else str(m)
                counts[name] += 1
        except Exception:
            pass

    if counts:
        print("Cumulative matches per park (all sources):")
        for park_name, count in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"  {park_name:<48} {count:>4} activit{'y' if count == 1 else 'ies'}")
    print()


if __name__ == "__main__":
    main()
