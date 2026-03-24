from __future__ import annotations
"""
04_profile_custom_parks.py
--------------------------
Profiles the custom_parks.geojson file and writes a column/geometry
profile to data/outputs/.

custom_parks.geojson is the manually maintained boundary file for parks
that are not covered by the federal or provincial datasets (e.g. city
trails, municipal green spaces, private parks). It lives in parks/ and
is committed to git.

Usage:
    python src/profiling/04_profile_custom_parks.py

Output:
    data/outputs/profile_custom_parks.xlsx
"""

from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

GEOJSON_PATH = config.PARKS_CUSTOM
OUTPUT_XLSX  = config.DATA_OUTPUTS / "profile_custom_parks.xlsx"


# =============================================================================
# PROFILING HELPERS  (shared pattern with other profiling scripts)
# =============================================================================

def format_example(value: Any, max_len: int = 160) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ").strip()
    return s[:max_len - 3] + "..." if len(s) > max_len else s


def infer_series_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):            return "bool"
    if pd.api.types.is_integer_dtype(s):         return "int"
    if pd.api.types.is_float_dtype(s):           return "float"
    if pd.api.types.is_datetime64_any_dtype(s):  return "datetime"
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
    b = gdf.total_bounds
    rows.append({"metric": "bounds (minx, miny, maxx, maxy)", "value": str(tuple(b))})
    # Area in square kilometres (reproject to a metric CRS for accuracy)
    try:
        gdf_proj = gdf.to_crs(epsg=32620)   # UTM zone 20N — appropriate for NB
        areas_km2 = gdf_proj.geometry.area / 1_000_000
        rows.append({"metric": "area min (km²)",  "value": round(float(areas_km2.min()), 4)})
        rows.append({"metric": "area max (km²)",  "value": round(float(areas_km2.max()), 4)})
        rows.append({"metric": "area mean (km²)", "value": round(float(areas_km2.mean()), 4)})
        rows.append({"metric": "area total (km²)","value": round(float(areas_km2.sum()), 4)})
    except Exception:
        pass
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
# MAIN
# =============================================================================

def main() -> None:
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(
            f"Custom parks GeoJSON not found: {GEOJSON_PATH}\n"
            "Expected location: parks/custom_parks.geojson"
        )

    print(f"Reading: {GEOJSON_PATH}")
    gdf = gpd.read_file(str(GEOJSON_PATH))

    # Ensure WGS84
    if gdf.crs is None:
        print("Warning: no CRS defined — assuming EPSG:4326 (WGS84)")
        gdf = gdf.set_crs(epsg=4326)
    else:
        gdf = gdf.to_crs(epsg=4326)

    print(f"  Features : {len(gdf)}")
    print(f"  CRS      : {gdf.crs}")

    # Attribute profile (without geometry column)
    df_attrs = pd.DataFrame(gdf.drop(columns=["geometry"], errors="ignore"))
    df_cols  = profile_columns(df_attrs)

    # Data sheet — attributes + WKT geometry for inspection
    df_data = df_attrs.copy()
    if "geometry" in gdf.columns:
        df_data["geometry_wkt"] = gdf.geometry.to_wkt()

    # Geometry summary
    df_geom = profile_geometry(gdf)

    config.DATA_OUTPUTS.mkdir(parents=True, exist_ok=True)
    used: set[str] = set()
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_cols.to_excel(writer, sheet_name=safe_sheet_name("column_profile", used), index=False)
        df_data.to_excel(writer, sheet_name=safe_sheet_name("data", used),           index=False)
        df_geom.to_excel(writer, sheet_name=safe_sheet_name("geometry", used),       index=False)

    print(f"Profile saved: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
