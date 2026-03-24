from __future__ import annotations
"""
02_profile_shapefile_boundaries.py
-----------------------------------
Scans the raw shapefiles folder (config.RAW_PARKS_SHAPEFILES) for every .zip
file, profiles each one, and writes a per-file Excel profile to data/outputs/.

Each zip is expected to contain a shapefile (.shp + companions).  The script
reprojects to EPSG:4326, exports a GeoJSON alongside the zip, and writes a
four-sheet Excel profile:
    • columns        – raw attribute table
    • column_profile – null/distinct/type stats per column
    • data           – attribute table with geometry_wkt appended
    • geometry       – row count, CRS, geom-type counts, bounding box

Usage:
    python src/profiling/02_profile_shapefile_boundaries.py

Outputs (one Excel file per zip found):
    data/outputs/profile_shp_<stem>.xlsx
"""

from pathlib import Path
from typing import Any
import zipfile
import tempfile

import geopandas as gpd
import pandas as pd

import sys

# =============================================================================
# CONFIGURATION — paths pulled from config.py at project root
# =============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

SHP_DIR     = config.RAW_PARKS_SHAPEFILES   # folder containing .zip shapefiles
GEOJSON_DIR = config.PARKS_DIR              # where reprojected GeoJSONs are written
OUTPUT_DIR  = config.DATA_OUTPUTS           # where Excel profiles are written


# =============================================================================
# PROFILING HELPERS
# =============================================================================

def format_example(value: Any, max_len: int = 160) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ").strip()
    return s[:max_len - 3] + "..." if len(s) > max_len else s


def infer_series_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):               return "bool"
    if pd.api.types.is_integer_dtype(s):            return "int"
    if pd.api.types.is_float_dtype(s):              return "float"
    if pd.api.types.is_datetime64_any_dtype(s):     return "datetime"
    if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
        return "str"
    return str(s.dtype)


def profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    n_rows = len(df)
    rows: list[dict[str, Any]] = []
    for col in df.columns:
        s = df[col]
        non_null = int(s.notna().sum())
        nulls    = int(n_rows - non_null)
        null_pct = round((nulls / n_rows * 100.0) if n_rows else 0.0, 2)
        try:
            distinct = int(s.dropna().nunique())
        except Exception:
            distinct = None
        try:
            example = format_example(s.dropna().iloc[0] if non_null else None)
        except Exception:
            example = ""
        dtype_label = infer_series_type(s)
        num_min = num_max = None
        if pd.api.types.is_numeric_dtype(s):
            try:
                num_min = float(s.min()) if non_null else None
                num_max = float(s.max()) if non_null else None
            except Exception:
                pass
        min_len = max_len = None
        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            try:
                lens = s.dropna().astype(str).map(len)
                if not lens.empty:
                    min_len, max_len = int(lens.min()), int(lens.max())
            except Exception:
                pass
        rows.append({
            "column name":         col,
            "inferred data type":  dtype_label,
            "rows":                n_rows,
            "non-null":            non_null,
            "null":                nulls,
            "null %":              null_pct,
            "distinct (non-null)": distinct,
            "example value":       example,
            "min (numeric)":       num_min,
            "max (numeric)":       num_max,
            "min length (text)":   min_len,
            "max length (text)":   max_len,
        })
    return pd.DataFrame(rows)


def profile_geometry(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = [
        {"metric": "rows", "value": len(gdf)},
        {"metric": "crs",  "value": str(gdf.crs)},
    ]
    if "geometry" not in gdf.columns:
        rows.append({"metric": "geometry", "value": "no geometry column"})
        return pd.DataFrame(rows)
    for gt, cnt in gdf.geometry.geom_type.value_counts(dropna=False).items():
        rows.append({"metric": f"geom type count: {gt}", "value": int(cnt)})
    b = gdf.total_bounds
    rows.append({"metric": "bounds (minx, miny, maxx, maxy)", "value": str(tuple(b))})
    return pd.DataFrame(rows)


def safe_sheet_name(name: str, used: set[str]) -> str:
    bad = {":", "\\", "/", "?", "*", "[", "]"}
    cleaned = ("".join("_" if c in bad else c for c in name).strip() or "Sheet")[:31]
    candidate, i = cleaned, 2
    while candidate in used:
        suffix    = f"_{i}"
        candidate = cleaned[:31 - len(suffix)] + suffix
        i += 1
    used.add(candidate)
    return candidate


# =============================================================================
# PER-ZIP PROCESSING
# =============================================================================

def process_zip(zip_path: Path) -> None:
    """Read a zipped shapefile, reproject, export GeoJSON, write Excel profile."""
    print(f"\n{'='*60}")
    print(f"Processing: {zip_path.name}")

    # Extract zip to a temp directory so geopandas can find the .shp
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp)

        shp_files = list(Path(tmp).rglob("*.shp"))
        if not shp_files:
            print(f"  WARNING: no .shp file found inside {zip_path.name} — skipping.")
            return
        if len(shp_files) > 1:
            print(f"  WARNING: multiple .shp files found; using first: {shp_files[0].name}")

        shp_file = shp_files[0]
        print(f"  Reading : {shp_file.name}")
        gdf = gpd.read_file(str(shp_file))

    # Government of Canada shapefiles sometimes lack an embedded CRS.
    # The standard for federal Canadian geospatial data is NAD83 (EPSG:4269).
    if gdf.crs is None:
        print("  WARNING: no CRS found — assuming EPSG:4269 (NAD83, standard for GC data)")
        gdf = gdf.set_crs(epsg=4269)

    parks = gdf.to_crs(epsg=4326)

    # Export reprojected GeoJSON
    GEOJSON_DIR.mkdir(parents=True, exist_ok=True)
    geojson_out = GEOJSON_DIR / f"{zip_path.stem}.geojson"
    parks.to_file(str(geojson_out), driver="GeoJSON")
    print(f"  GeoJSON : {geojson_out}")

    # Build profile tables
    df_attrs = pd.DataFrame(parks.drop(columns=["geometry"], errors="ignore"))
    df_cols  = profile_columns(df_attrs)
    df_data  = df_attrs.copy()
    if "geometry" in parks.columns:
        df_data["geometry_wkt"] = parks.geometry.to_wkt()
    df_geom = profile_geometry(parks)

    # Write Excel profile
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    xlsx_out = OUTPUT_DIR / f"profile_shp_{zip_path.stem}.xlsx"
    used: set[str] = set()
    with pd.ExcelWriter(xlsx_out, engine="openpyxl") as writer:
        parks.drop(columns=["geometry"], errors="ignore").to_excel(
            writer, sheet_name=safe_sheet_name("columns", used), index=False)
        df_cols.to_excel(writer, sheet_name=safe_sheet_name("column_profile", used), index=False)
        df_data.to_excel(writer, sheet_name=safe_sheet_name("data", used), index=False)
        df_geom.to_excel(writer, sheet_name=safe_sheet_name("geometry", used), index=False)

    print(f"  Profile : {xlsx_out}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    shp_dir = Path(SHP_DIR)

    if not shp_dir.exists():
        raise FileNotFoundError(
            f"Shapefiles folder not found: {shp_dir}\n"
            f"Set RAW_PARKS_SHAPEFILES in config.py or create the directory."
        )

    zip_files = sorted(shp_dir.glob("*.zip"))
    if not zip_files:
        print(f"No .zip files found in {shp_dir}")
        return

    print(f"Found {len(zip_files)} zip file(s) in {shp_dir}")
    for zip_path in zip_files:
        try:
            process_zip(zip_path)
        except Exception as exc:
            print(f"  ERROR processing {zip_path.name}: {exc}")

    print(f"\nDone. Profiles written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
