from __future__ import annotations
"""
03_profile_geojson_boundaries.py
---------------------------------
Profiles all GeoJSON files in the RAW_PARKS_GEOJSON folder and writes a
column/geometry profile per file to data/outputs/.

Usage:
    python src/01_profiling/03_profile_geojson_boundaries.py

Output:
    data/outputs/profile_geojson_<filename>.xlsx  (one per file)
"""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

GEOJSON_EXTENSIONS = {".geojson", ".json", ".txt"}


# =============================================================================
# PROFILING HELPERS
# =============================================================================

def format_example(value: Any, max_len: int = 160) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ").strip()
    return s[:max_len - 3] + "..." if len(s) > max_len else s


def infer_series_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):              return "bool"
    if pd.api.types.is_integer_dtype(s):           return "int"
    if pd.api.types.is_float_dtype(s):             return "float"
    if pd.api.types.is_datetime64_any_dtype(s):    return "datetime"
    if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s): return "str"
    return str(s.dtype)


def profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    n_rows = len(df)
    rows: list[dict[str, Any]] = []
    for col in df.columns:
        s        = df[col]
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

    null_geom = int(gdf.geometry.isna().sum())
    rows.append({"metric": "null geometries", "value": null_geom})

    b = gdf.total_bounds
    rows.append({"metric": "bounds (minx, miny, maxx, maxy)", "value": str(tuple(b))})

    try:
        valid = gdf.geometry.dropna().is_valid
        rows.append({"metric": "valid geometries",   "value": int(valid.sum())})
        rows.append({"metric": "invalid geometries", "value": int((~valid).sum())})
    except Exception:
        pass

    return pd.DataFrame(rows)


def safe_sheet_name(name: str, used: set[str]) -> str:
    bad     = {":", "\\", "/", "?", "*", "[", "]"}
    cleaned = ("".join("_" if c in bad else c for c in name).strip() or "Sheet")[:31]
    candidate, i = cleaned, 2
    while candidate in used:
        suffix    = f"_{i}"
        candidate = cleaned[:31 - len(suffix)] + suffix
        i += 1
    used.add(candidate)
    return candidate


# =============================================================================
# PER-FILE PROCESSING
# =============================================================================

def process_file(geojson_path: Path) -> None:
    print(f"\n{'='*60}")
    print(f"Processing: {geojson_path.name}")

    try:
        gdf = gpd.read_file(str(geojson_path))
    except Exception as e:
        print(f"  ERROR reading {geojson_path.name}: {e}")
        return

    # GeoJSON spec mandates WGS84 (EPSG:4326), but flag if something else is embedded.
    if gdf.crs is None:
        print("  Warning: no CRS found — assuming EPSG:4326 (WGS84, GeoJSON standard)")
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        print(f"  Warning: CRS is {gdf.crs} (not EPSG:4326). Reprojecting to WGS84 for consistency.")
        gdf = gdf.to_crs(epsg=4326)

    # Attribute table (no geometry column)
    df_attrs = pd.DataFrame(gdf.drop(columns=["geometry"], errors="ignore"))

    # Excel doesn't support timezone-aware datetimes — strip tz info
    for col in df_attrs.columns:
        if pd.api.types.is_datetime64_any_dtype(df_attrs[col]):
            if hasattr(df_attrs[col].dt, "tz") and df_attrs[col].dt.tz is not None:
                df_attrs[col] = df_attrs[col].dt.tz_localize(None)

    df_cols = profile_columns(df_attrs)

    # Full data with WKT geometry for inspection
    df_data = df_attrs.copy()
    if "geometry" in gdf.columns:
        df_data["geometry_wkt"] = gdf.geometry.to_wkt()

    df_geom = profile_geometry(gdf)

    output_xlsx = config.DATA_OUTPUTS / f"profile_geojson_{geojson_path.stem}.xlsx"
    used: set[str] = set()
    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df_attrs.to_excel(writer, sheet_name=safe_sheet_name("columns",        used), index=False)
        df_cols.to_excel( writer, sheet_name=safe_sheet_name("column_profile", used), index=False)
        df_data.to_excel( writer, sheet_name=safe_sheet_name("data",           used), index=False)
        df_geom.to_excel( writer, sheet_name=safe_sheet_name("geometry",       used), index=False)

    print(f"  Profile saved: {output_xlsx}")
    print(f"  Rows    : {len(gdf):,}")
    print(f"  Columns : {len(df_attrs.columns)}")
    print(f"  CRS     : {gdf.crs}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    folder = Path(config.RAW_PARKS_GEOJSON)

    if not folder.exists():
        raise FileNotFoundError(
            f"GeoJSON folder not found: {folder}\n"
            f"Set RAW_PARKS_GEOJSON in config.py to the folder containing your GeoJSON files."
        )

    files = [f for f in sorted(folder.iterdir()) if f.suffix.lower() in GEOJSON_EXTENSIONS]

    if not files:
        print(f"No GeoJSON files found in {folder} (looked for {GEOJSON_EXTENSIONS})")
        return

    print(f"Found {len(files)} file(s) in {folder}")
    config.DATA_OUTPUTS.mkdir(parents=True, exist_ok=True)

    for f in files:
        process_file(f)


if __name__ == "__main__":
    main()
