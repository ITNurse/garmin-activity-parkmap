"""
02_preprocess_shapefile_boundaries.py

Converts all zipped shapefiles (.zip containing .shp, .dbf, .shx) in the
RAW_PARKS_SHAPEFILES folder to GeoJSON files in the RAW_PARKS_GEOJSON folder.
Reprojects to WGS-84 (EPSG:4326) if needed.
All attribute columns are preserved as GeoJSON feature properties.

Usage:
    python 02_preprocess_shapefile_boundaries.py

Output:
    One .geojson file per .zip in config.RAW_PARKS_GEOJSON

Dependencies:
    pip install geopandas
"""

import json
import sys
from pathlib import Path

try:
    import geopandas as gpd
except ImportError:
    sys.exit("Error: geopandas is required. Install with:  pip install geopandas")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config


def process_zip(input_path: Path, output_path: Path) -> None:
    print(f"\n{'='*60}")
    print(f"Processing: {input_path.name}")

    try:
        gdf = gpd.read_file(f"zip://{input_path}")
    except Exception as e:
        print(f"  ERROR reading {input_path.name}: {e}")
        return

    print(f"  Loaded {len(gdf)} features | CRS: {gdf.crs}")

    # Reproject to WGS-84 (EPSG:4326) — required by GeoJSON spec
    if gdf.crs is None:
        print("  WARNING: No CRS found; assuming EPSG:4326")
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        print(f"  Reprojecting from {gdf.crs} → EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)

    # Drop rows with null geometry
    before = len(gdf)
    gdf = gdf[gdf.geometry.notna()].reset_index(drop=True)
    if len(gdf) < before:
        print(f"  Dropped {before - len(gdf)} features with null geometry")

    # Write GeoJSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"  Saved {len(gdf)} features → {output_path}")

    # Sanity check — print first feature's properties
    with open(output_path, encoding="utf-8") as f:
        data = json.load(f)

    if data["features"]:
        props = data["features"][0].get("properties", {})
        print("  Sample properties from first feature:")
        for k, v in list(props.items())[:8]:
            print(f"    {k}: {v}")


def main():
    input_folder  = Path(config.RAW_PARKS_SHAPEFILES)
    output_folder = Path(config.RAW_PARKS_GEOJSON)

    if not input_folder.exists():
        sys.exit(
            f"Error: Shapefile folder not found: {input_folder}\n"
            f"Set RAW_PARKS_SHAPEFILES in config.py."
        )

    zip_files = sorted(input_folder.glob("*.zip"))

    if not zip_files:
        print(f"No .zip files found in {input_folder}")
        return

    print(f"Found {len(zip_files)} zip file(s) in {input_folder}")
    output_folder.mkdir(parents=True, exist_ok=True)

    for zip_path in zip_files:
        output_path = output_folder / (zip_path.stem + ".geojson")
        process_zip(zip_path, output_path)

    print(f"\nDone. GeoJSON files written to {output_folder}")


if __name__ == "__main__":
    main()
