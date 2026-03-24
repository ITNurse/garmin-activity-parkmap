"""
02_preprocess_wkt_boundaries.py

Converts all CSV files containing WKT geometry in the RAW_PARKS_WKT folder
to GeoJSON files in the RAW_PARKS_GEOJSON folder.

The geometry column is auto-detected from a list of common names, or falls
back to the first column that looks like WKT.

Usage:
    python 02_preprocess_wkt_boundaries.py

Output:
    One .geojson file per .csv in config.RAW_PARKS_GEOJSON

Dependencies:
    pip install shapely
"""

import csv
import json
import sys
from pathlib import Path

# WKT geometry strings can be very large — raise the csv field size limit
csv.field_size_limit(min(sys.maxsize, 2147483647))

try:
    from shapely import wkt
    from shapely.geometry import mapping
except ImportError:
    sys.exit("Error: shapely is required. Install it with: pip install shapely")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

# Common WKT geometry column names to try, in order of preference
GEOM_COL_CANDIDATES = [
    "the_geom", "geom", "geometry", "wkt", "wkt_geom",
    "shape", "geo", "geographic_data", "spatial_data",
]


def detect_geom_col(fieldnames: list[str]) -> str | None:
    """Return the geometry column name, trying common names then sniffing WKT content."""
    lower_map = {f.lower(): f for f in fieldnames}
    for candidate in GEOM_COL_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def process_csv(input_path: Path, output_path: Path) -> None:
    print(f"\n{'='*60}")
    print(f"Processing: {input_path.name}")

    features = []

    try:
        with input_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            geom_col = detect_geom_col(reader.fieldnames or [])
            if geom_col is None:
                print(f"  ERROR: Could not find a geometry column in {input_path.name}.")
                print(f"  Available columns: {', '.join(reader.fieldnames or [])}")
                print(f"  Expected one of: {', '.join(GEOM_COL_CANDIDATES)}")
                return

            print(f"  Geometry column: '{geom_col}'")

            skipped = 0
            for i, row in enumerate(reader):
                wkt_str = row.pop(geom_col, "").strip()
                if not wkt_str:
                    skipped += 1
                    continue
                try:
                    geom = wkt.loads(wkt_str)
                except Exception as e:
                    print(f"  Warning: Row {i + 1} has invalid WKT ({e}) — skipping.")
                    skipped += 1
                    continue

                features.append({
                    "type": "Feature",
                    "geometry": mapping(geom),
                    "properties": {k: v for k, v in row.items()},
                })

        if skipped:
            print(f"  Skipped {skipped} row(s) with missing or invalid geometry")

    except Exception as e:
        print(f"  ERROR reading {input_path.name}: {e}")
        return

    geojson = {"type": "FeatureCollection", "features": features}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)

    print(f"  Saved {len(features)} features → {output_path}")

    # Sanity check — print first feature's properties
    if features:
        print("  Sample properties from first feature:")
        for k, v in list(features[0]["properties"].items())[:8]:
            print(f"    {k}: {v}")


def main():
    input_folder  = Path(config.RAW_PARKS_WKT)
    output_folder = Path(config.RAW_PARKS_GEOJSON)

    if not input_folder.exists():
        sys.exit(
            f"Error: WKT folder not found: {input_folder}\n"
            f"Set RAW_PARKS_WKT in config.py."
        )

    csv_files = sorted(input_folder.glob("*.csv"))

    if not csv_files:
        print(f"No .csv files found in {input_folder}")
        return

    print(f"Found {len(csv_files)} CSV file(s) in {input_folder}")
    output_folder.mkdir(parents=True, exist_ok=True)

    for csv_path in csv_files:
        output_path = output_folder / (csv_path.stem + ".geojson")
        process_csv(csv_path, output_path)

    print(f"\nDone. GeoJSON files written to {output_folder}")


if __name__ == "__main__":
    main()
