from __future__ import annotations
"""
03_profile_wkt_boundaries.py
----------------------------
Scans the raw WKT folder (config.RAW_PARKS_WKT) for every .csv and .txt file,
profiles each one, and writes a per-file Excel profile to data/outputs/.

Each file is expected to contain a WKT geometry column (auto-detected, but
commonly named 'the_geom', 'wkt', 'geometry', or 'WKT').  No GIS library is
required — geometry is profiled directly from the WKT text.

Usage:
    python src/profiling/03_profile_wkt_boundaries.py

Outputs (one Excel file per source file found):
    data/outputs/profile_wkt_<stem>.xlsx
"""

import re
from pathlib import Path
from typing import Any

import pandas as pd

import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

WKT_DIR    = config.RAW_PARKS_WKT   # folder containing WKT source files
OUTPUT_DIR = config.DATA_OUTPUTS    # where Excel profiles are written

# File extensions to scan
WKT_EXTENSIONS = {".csv", ".txt"}

# Candidate column names that are likely to hold WKT geometry (checked in order)
WKT_COL_CANDIDATES = ["the_geom", "wkt", "geometry", "WKT", "GEOMETRY", "geom", "shape"]


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


def detect_wkt_column(df: pd.DataFrame) -> str | None:
    """Return the first column that looks like it holds WKT geometry, or None."""
    for candidate in WKT_COL_CANDIDATES:
        if candidate in df.columns:
            return candidate
    # Fallback: scan all string columns for WKT-like content
    wkt_pattern = re.compile(
        r"^\s*(MULTI)?(POLYGON|LINESTRING|POINT|GEOMETRYCOLLECTION)", re.IGNORECASE
    )
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            sample = df[col].dropna().head(10).astype(str)
            if sample.str.match(wkt_pattern).any():
                return col
    return None


def try_parse_dates_and_numbers(df: pd.DataFrame, skip_col: str | None) -> pd.DataFrame:
    """Light-touch type coercion — skips the geometry column."""
    out = df.copy()
    for col in out.columns:
        if col == skip_col:
            continue
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            numeric = pd.to_numeric(out[col], errors="coerce")
            n_valid = out[col].notna().sum()
            if n_valid > 0 and numeric.notna().sum() >= max(10, int(0.6 * n_valid)):
                out[col] = numeric
                continue
            dt = pd.to_datetime(out[col], errors="coerce", utc=False)
            if n_valid > 0 and dt.notna().sum() >= max(10, int(0.6 * n_valid)):
                out[col] = dt
    return out


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


def profile_geom_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Lightweight geometry profiling from WKT text — no GIS library required."""
    s      = df[col].astype("string")
    n_rows = len(s)
    non_null = int(s.notna().sum())

    def guess_geom_type(v: str) -> str:
        v2 = v.strip().upper()
        for t in ["MULTIPOLYGON", "POLYGON", "MULTILINESTRING",
                  "LINESTRING", "MULTIPOINT", "POINT", "GEOMETRYCOLLECTION"]:
            if v2.startswith(t):
                return t
        return "unknown"

    pair_re = re.compile(r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)")
    geom_type_counts: dict[str, int] = {}
    pair_counts: list[int] = []
    example = ""

    for v in s.dropna().head(500):
        v_str = str(v)
        gt    = guess_geom_type(v_str)
        geom_type_counts[gt] = geom_type_counts.get(gt, 0) + 1
        if not example:
            example = format_example(v_str, max_len=220)
        pair_counts.append(len(pair_re.findall(v_str)))

    rows = [
        {"metric": "wkt column",        "value": col},
        {"metric": "rows",              "value": n_rows},
        {"metric": "non-null",          "value": non_null},
        {"metric": "null",              "value": n_rows - non_null},
        {"metric": "example geometry",  "value": example},
        {"metric": "approx coord-pairs (min of first 500)",
         "value": int(min(pair_counts)) if pair_counts else None},
        {"metric": "approx coord-pairs (max of first 500)",
         "value": int(max(pair_counts)) if pair_counts else None},
    ]
    for gt, cnt in sorted(geom_type_counts.items(), key=lambda x: (-x[1], x[0])):
        rows.append({"metric": f"geom type count: {gt}", "value": cnt})
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

def process_file(file_path: Path) -> None:
    """Read a WKT source file, profile it, and write an Excel report."""
    print(f"\n{'='*60}")
    print(f"Processing: {file_path.name}")

    df_raw = pd.read_csv(file_path, dtype="string", low_memory=False)
    print(f"  Rows: {len(df_raw):,}  |  Columns: {len(df_raw.columns)}")

    wkt_col = detect_wkt_column(df_raw)
    if wkt_col:
        print(f"  WKT column detected: '{wkt_col}'")
    else:
        print("  WARNING: no WKT geometry column detected — geometry profile will be skipped.")

    df = try_parse_dates_and_numbers(df_raw, skip_col=wkt_col)

    df_cols = profile_columns(df)
    df_geom = profile_geom_column(df, wkt_col) if wkt_col else pd.DataFrame()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    xlsx_out = OUTPUT_DIR / f"profile_wkt_{file_path.stem}.xlsx"
    used: set[str] = set()
    with pd.ExcelWriter(xlsx_out, engine="openpyxl") as writer:
        df_cols.to_excel(writer, sheet_name=safe_sheet_name("column_profile", used), index=False)
        df.to_excel(writer,      sheet_name=safe_sheet_name("data", used),           index=False)
        if not df_geom.empty:
            df_geom.to_excel(writer, sheet_name=safe_sheet_name("geom_profile", used), index=False)

    print(f"  Profile : {xlsx_out}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    wkt_dir = Path(WKT_DIR)

    if not wkt_dir.exists():
        raise FileNotFoundError(
            f"WKT folder not found: {wkt_dir}\n"
            f"Set RAW_PARKS_WKT in config.py or create the directory."
        )

    source_files = sorted(
        f for f in wkt_dir.iterdir()
        if f.is_file() and f.suffix.lower() in WKT_EXTENSIONS
    )

    if not source_files:
        print(f"No {WKT_EXTENSIONS} files found in {wkt_dir}")
        return

    print(f"Found {len(source_files)} file(s) in {wkt_dir}")
    for file_path in source_files:
        try:
            process_file(file_path)
        except Exception as exc:
            print(f"  ERROR processing {file_path.name}: {exc}")

    print(f"\nDone. Profiles written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
