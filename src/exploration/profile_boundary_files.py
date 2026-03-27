from __future__ import annotations
"""
profile_boundaries.py
---------------------
A single script that profiles all three park boundary data formats used in
this project and writes a per-file Excel report for each one.

Supported formats and source folders (all configured in config.py):
    • GeoJSON  (.geojson, .json)       — config.RAW_PARKS_GEOJSON
    • Shapefile (.zip containing .shp) — config.RAW_PARKS_SHAPEFILES
    • WKT/CSV  (.csv, .txt)            — config.RAW_PARKS_WKT

All three folders are scanned automatically in a single run.  Files that
cannot be read are logged and skipped without stopping the rest of the run.

Output (one Excel file per source file found):
    data/outputs/profile_geojson_<name>.xlsx
    data/outputs/profile_shp_<name>.xlsx
    data/outputs/profile_wkt_<name>.xlsx

Usage:
    Scan all three configured source folders (default):
        python src/profiling/profile_boundaries.py

    Scan a single folder (format inferred from file extensions):
        python src/profiling/profile_boundaries.py --folder path/to/folder

    Profile a single file:
        python src/profiling/profile_boundaries.py --file path/to/file.geojson
"""

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
import re                   # regular expressions (used for WKT column detection)
import sys                  # used to modify Python's module search path
import argparse             # parses command-line arguments (e.g. --file, --folder)
import tempfile             # creates temporary directories for zip extraction
import zipfile              # reads .zip archives (shapefiles are distributed as zips)
from pathlib import Path    # modern, object-oriented file path handling
from typing import Any      # used in type hints to indicate "any type"

# ---------------------------------------------------------------------------
# Third-party library imports
# ---------------------------------------------------------------------------
import geopandas as gpd     # extends pandas for geospatial data (GeoDataFrames)
import pandas as pd         # core data manipulation library

# ---------------------------------------------------------------------------
# Project configuration
# ---------------------------------------------------------------------------
# Climb up three directory levels from this file's location to find the project
# root, then add it to Python's search path so we can import config.py.
# __file__ is the path to this script; .resolve() converts it to an absolute
# path; .parent.parent.parent walks up three folder levels.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

# Convenience aliases for the three source folders and the output folder.
# These are all defined in config.py — change them there, not here.
GEOJSON_DIR = config.RAW_PARKS_GEOJSON     # folder containing GeoJSON files
SHP_DIR     = config.RAW_PARKS_SHAPEFILES  # folder containing zipped shapefiles
WKT_DIR     = config.RAW_PARKS_WKT        # folder containing WKT/CSV files
OUTPUT_DIR  = config.DATA_OUTPUTS         # where all Excel profiles are written
PARKS_DIR   = config.PARKS_DIR            # where reprojected GeoJSONs are saved
                                           # (shapefiles only — produced as a side effect)

# Which file extensions belong to each format.
# Sets (curly braces) are used here because membership checks (e.g. "is this
# extension in the set?") are faster on sets than on lists.
GEOJSON_EXTENSIONS = {".geojson", ".json"}
WKT_EXTENSIONS     = {".csv", ".txt"}
# Shapefiles are always delivered as .zip archives in this project.

# Candidate column names that are likely to hold WKT geometry, checked in order.
# If none of these match by name, the script falls back to inspecting column content.
WKT_COL_CANDIDATES = ["the_geom", "wkt", "geometry", "WKT", "GEOMETRY", "geom", "shape"]


# =============================================================================
# SHARED HELPER FUNCTIONS
# These functions are used when profiling all three file formats.
# =============================================================================

def format_example(value: Any, max_len: int = 160) -> str:
    """
    Convert a single cell value to a clean, readable string for display.

    Newlines and carriage returns are replaced with spaces so the value fits
    neatly in a spreadsheet cell.  If the resulting string is longer than
    max_len characters it is truncated and "..." is appended so the reader
    knows something was cut off.

    Parameters
    ----------
    value   : the raw cell value (could be any type — int, float, str, None…)
    max_len : maximum number of characters to return (default 160)

    Returns
    -------
    A plain string, or an empty string if the value is None/NaN.
    """
    # Treat None and floating-point NaN as "no value" — return empty string.
    # pd.isna() catches numpy NaN values that isinstance alone would miss.
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    # Convert to string and tidy up whitespace / line endings
    s = str(value).replace("\r", " ").replace("\n", " ").strip()

    # Truncate long strings and append "..." to signal the value was cut off
    return s[:max_len - 3] + "..." if len(s) > max_len else s


def infer_series_type(s: pd.Series) -> str:
    """
    Return a human-friendly type label for a pandas Series.

    pandas stores dtype information internally, but the raw dtype strings
    (e.g. "int64", "object") aren't very readable in a profile report.
    This function maps those dtypes to simple labels like "int", "float",
    "str", etc.

    Parameters
    ----------
    s : a pandas Series (one column from a DataFrame)

    Returns
    -------
    A short string label describing the column's data type.
    """
    # Check types in order from most to least specific.
    # bool MUST come before int because in pandas, boolean columns are technically
    # a subtype of integer — is_integer_dtype() returns True for bool columns too.
    if pd.api.types.is_bool_dtype(s):               return "bool"
    if pd.api.types.is_integer_dtype(s):            return "int"
    if pd.api.types.is_float_dtype(s):              return "float"
    if pd.api.types.is_datetime64_any_dtype(s):     return "datetime"
    if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
        return "str"

    # Fall back to the raw pandas dtype string for anything unusual (e.g. "category")
    return str(s.dtype)


def extract_special_characters(series: pd.Series, max_chars: int = 20) -> str:
    """
    Scan a text column for non-alphanumeric characters and return them as a
    sorted string.

    This is useful for spotting unexpected punctuation, currency symbols, or
    encoding artifacts in a column — things that might cause problems if you
    later try to parse the column as a number.

    Parameters
    ----------
    series    : a pandas Series (one column from a DataFrame)
    max_chars : maximum number of distinct special characters to return

    Returns
    -------
    A sorted string of unique special characters found, e.g. "!$%,./" or "".
    """
    chars: set[str] = set()

    try:
        for val in series.dropna().astype(str):
            # re.findall returns a list of all characters that match the pattern.
            # [^a-zA-Z0-9\s] means "not a letter, digit, or whitespace character".
            found = re.findall(r"[^a-zA-Z0-9\s]", val)
            chars.update(found)

            # Stop early once we've collected enough distinct characters
            if len(chars) >= max_chars:
                break

        # Sort for consistency across runs, then cap the result length
        return "".join(sorted(chars))[:max_chars] if chars else ""

    except Exception:
        # If anything goes wrong (unusual dtype, encoding issue, etc.), return empty
        return ""


def profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a summary statistics table describing every column in a DataFrame.

    For each column this produces one row containing:
        - column name and inferred data type
        - total row count, non-null count, null count, and null percentage
        - number of distinct (unique) non-null values
        - a sample value from the first non-null row
        - min/max for numeric columns
        - min/max string length for text columns
        - data quality diagnostics (dirty numerics, mixed types, etc.)
        - special characters found in text columns

    This kind of summary is often called a "data profile" and is the fastest
    way to get a clear picture of a dataset before doing any analysis.

    Parameters
    ----------
    df : a plain pandas DataFrame (any geometry column should already be removed)

    Returns
    -------
    A new DataFrame with one row per column of the input.
    """
    n_rows = len(df)
    rows: list[dict[str, Any]] = []  # we'll build this list up one dict per column

    for col in df.columns:
        s        = df[col]
        non_null = int(s.notna().sum())    # count of rows that are NOT null
        nulls    = int(n_rows - non_null)  # count of rows that ARE null
        # Null percentage: what fraction of rows have no value?
        null_pct = round((nulls / n_rows * 100.0) if n_rows else 0.0, 2)

        # Count how many distinct values appear (ignoring nulls)
        try:
            distinct = int(s.dropna().nunique())
        except Exception:
            distinct = None

        # Grab one example value from the first non-null row
        try:
            example = format_example(s.dropna().iloc[0] if non_null else None)
        except Exception:
            example = ""

        dtype_label   = infer_series_type(s)
        special_chars = extract_special_characters(s)

        # ----------------------------
        # NUMERIC STATS
        # Compute min/max only for columns pandas already recognises as numeric.
        # Text columns that look like numbers are handled in the diagnostics section.
        # ----------------------------
        num_min = num_max = None
        if pd.api.types.is_numeric_dtype(s):
            try:
                num_min = float(s.min()) if non_null else None
                num_max = float(s.max()) if non_null else None
            except Exception:
                pass

        # ----------------------------
        # TEXT LENGTH STATS
        # For string columns, knowing the shortest and longest value helps catch
        # fixed-width codes, free-text fields, truncated values, etc.
        # ----------------------------
        min_len = max_len = None
        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            try:
                lens = s.dropna().astype(str).map(len)  # length of each value
                if not lens.empty:
                    min_len, max_len = int(lens.min()), int(lens.max())
            except Exception:
                pass

        # ----------------------------
        # DATA QUALITY DIAGNOSTICS
        # These metrics help identify columns that look like one type but are
        # stored as another — for example, a price column stored as text that
        # contains commas and dollar signs.
        # ----------------------------
        numeric_like_pct     = None   # what % of values can be parsed as a number?
        numeric_parse_errors = None   # how many values FAILED to parse as a number?
        bad_examples         = None   # example of a value that failed to parse
        has_commas_pct       = None   # what % of values contain a comma?
        has_currency_pct     = None   # what % of values contain $, €, or £?
        has_percent_pct      = None   # what % of values contain %?
        likely_numeric_dirty = False  # True if the column is probably a dirty number
        quality_flag         = "ok"   # overall quality label for quick filtering

        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            try:
                # Work only with non-null values, trimmed of surrounding whitespace
                s_non_null = s.dropna().astype(str).str.strip()

                if not s_non_null.empty:
                    # --- Pattern detection ---
                    # .mean() on a boolean Series gives the proportion that are True
                    has_commas_pct   = round(s_non_null.str.contains(",",      regex=False).mean() * 100, 2)
                    has_currency_pct = round(s_non_null.str.contains(r"[$€£]", regex=True ).mean() * 100, 2)
                    has_percent_pct  = round(s_non_null.str.contains("%",      regex=False).mean() * 100, 2)

                    # --- Numeric parseability ---
                    # Strip common numeric formatting characters before trying to parse,
                    # so "1,234.56" and "$99.99" are still recognised as numbers.
                    cleaned = (
                        s_non_null
                        .str.replace(",",      "", regex=False)
                        .str.replace(r"[$€£]", "", regex=True)
                        .str.replace("%",      "", regex=False)
                    )

                    # errors="coerce" turns unparseable values into NaN instead of raising
                    coerced      = pd.to_numeric(cleaned, errors="coerce")
                    success_rate = coerced.notna().mean()   # proportion that parsed OK
                    failure_mask = coerced.isna()           # True where parsing failed

                    if success_rate > 0:
                        numeric_like_pct     = round(success_rate * 100, 2)
                        numeric_parse_errors = int(failure_mask.sum())

                        # Capture one example of a value that failed to parse
                        if failure_mask.any():
                            bad_examples = format_example(s_non_null[failure_mask].iloc[0])

                        # Flag columns that are mostly parseable as numeric — these are
                        # good candidates to convert with pd.to_numeric() later
                        if success_rate > 0.8:
                            likely_numeric_dirty = True

            except Exception:
                pass  # if diagnostics fail for any reason, leave all values as None

        # ----------------------------
        # QUALITY FLAG
        # A single label summarising the most important quality concern for this column.
        # The order of these checks matters — "high_nulls" takes priority over others.
        # ----------------------------
        if null_pct > 50:
            quality_flag = "high_nulls"       # more than half the values are missing
        elif likely_numeric_dirty:
            quality_flag = "dirty_numeric"    # looks like a number but stored as text
        elif numeric_like_pct is not None and numeric_like_pct < 50:
            quality_flag = "mixed_types"      # column contains a mixture of types

        # Append one dictionary per column — these will become rows in the output DataFrame
        rows.append({
            "column name":              col,
            "inferred data type":       dtype_label,
            "rows":                     n_rows,
            "non-null":                 non_null,
            "null":                     nulls,
            "null %":                   null_pct,
            "distinct (non-null)":      distinct,
            "example value":            example,
            "min (numeric)":            num_min,
            "max (numeric)":            num_max,
            "min length (text)":        min_len,
            "max length (text)":        max_len,
            "numeric-like %":           numeric_like_pct,
            "numeric parse errors":     numeric_parse_errors,
            "example bad value":        bad_examples,
            "likely numeric (dirty)":   likely_numeric_dirty,
            "comma presence %":         has_commas_pct,
            "currency symbol %":        has_currency_pct,
            "percent symbol %":         has_percent_pct,
            "special characters":       special_chars,
            "quality flag":             quality_flag,
        })

    # Convert the list of dicts into a DataFrame — one row per input column
    return pd.DataFrame(rows)


def profile_geometry(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Build a high-level spatial summary of a GeoDataFrame.

    Produces a two-column ("metric" / "value") table covering:
        - total row count
        - coordinate reference system (CRS)
        - counts of each geometry type (Polygon, MultiPolygon, etc.)
        - number of null geometries
        - bounding box (the rectangular extent of all features combined)
        - counts of valid vs. invalid geometries

    This is used for GeoJSON and Shapefile formats, which are read directly
    into GeoDataFrames by geopandas.  WKT files use profile_geom_column()
    instead, which works without geopandas.

    Parameters
    ----------
    gdf : a GeoDataFrame that has already been read and (if necessary) reprojected

    Returns
    -------
    A two-column DataFrame with one row per metric.
    """
    # Start with the two most fundamental facts about any spatial dataset
    rows: list[dict[str, Any]] = [
        {"metric": "rows", "value": len(gdf)},
        {"metric": "crs",  "value": str(gdf.crs)},
    ]

    # If there's no geometry column at all, report that and return early
    if "geometry" not in gdf.columns:
        rows.append({"metric": "geometry", "value": "no geometry column"})
        return pd.DataFrame(rows)

    # Count how many features of each geometry type exist
    # value_counts() returns a Series sorted by frequency (most common first)
    for gt, cnt in gdf.geometry.geom_type.value_counts(dropna=False).items():
        rows.append({"metric": f"geom type count: {gt}", "value": int(cnt)})

    # Count features with no geometry at all (these often cause problems downstream)
    null_geom = int(gdf.geometry.isna().sum())
    rows.append({"metric": "null geometries", "value": null_geom})

    # Bounding box: the minimum rectangle that contains all features.
    # total_bounds returns [minx, miny, maxx, maxy] — i.e. [west, south, east, north]
    b = gdf.total_bounds
    rows.append({"metric": "bounds (minx, miny, maxx, maxy)", "value": str(tuple(b))})

    # Geometry validity: invalid geometries (self-intersecting polygons, etc.)
    # can cause failures in spatial operations like point-in-polygon tests
    try:
        valid = gdf.geometry.dropna().is_valid
        rows.append({"metric": "valid geometries",   "value": int(valid.sum())})
        rows.append({"metric": "invalid geometries", "value": int((~valid).sum())})
    except Exception:
        pass  # is_valid can fail on some geometry types — skip gracefully if so

    return pd.DataFrame(rows)


def safe_sheet_name(name: str, used: set[str]) -> str:
    """
    Produce a valid, unique Excel sheet name from an arbitrary string.

    Excel enforces two rules this function handles:
        1. Sheet names cannot contain these characters: : \\ / ? * [ ]
        2. Sheet names cannot exceed 31 characters.

    Additionally, if the same name would be used twice (because two files share
    a stem, for example), a numeric suffix (_2, _3, …) is appended.

    Parameters
    ----------
    name : the desired sheet name (may be invalid or too long)
    used : a set of names already in use — updated in-place by this function

    Returns
    -------
    A valid, unique sheet name of 31 characters or fewer.
    """
    # Characters that Excel forbids in sheet names
    bad = {":", "\\", "/", "?", "*", "[", "]"}

    # Replace each forbidden character with "_", strip leading/trailing spaces,
    # fall back to "Sheet" if the entire name is empty, then cap at 31 characters
    cleaned = ("".join("_" if c in bad else c for c in name).strip() or "Sheet")[:31]

    # Resolve name conflicts: if this name is already taken, append _2, _3, etc.
    candidate, i = cleaned, 2
    while candidate in used:
        suffix    = f"_{i}"
        # Trim the base so the suffix still fits within the 31-character limit
        candidate = cleaned[:31 - len(suffix)] + suffix
        i += 1

    used.add(candidate)  # register this name so future calls won't reuse it
    return candidate


# =============================================================================
# WKT-SPECIFIC HELPER FUNCTIONS
# These are only needed when reading WKT/CSV files, which are handled as plain
# DataFrames rather than GeoDataFrames.
# =============================================================================

def detect_wkt_column(df: pd.DataFrame) -> str | None:
    """
    Find the column in a DataFrame that contains WKT geometry strings.

    First checks a list of common column names (WKT_COL_CANDIDATES).  If none
    match, falls back to inspecting the actual content of each text column,
    looking for values that start with a WKT geometry type keyword like
    "POLYGON" or "MULTILINESTRING".

    Parameters
    ----------
    df : the raw DataFrame read from the CSV/TXT file

    Returns
    -------
    The name of the WKT column, or None if no geometry column is found.
    """
    # First pass: check whether any of the known candidate names exist as columns
    for candidate in WKT_COL_CANDIDATES:
        if candidate in df.columns:
            return candidate

    # Second pass: scan string columns for WKT-like values.
    # This catches geometry columns with unusual names (e.g. "boundary", "shape_geom").
    wkt_pattern = re.compile(
        r"^\s*(MULTI)?(POLYGON|LINESTRING|POINT|GEOMETRYCOLLECTION)", re.IGNORECASE
    )
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            # Sample the first 10 non-null values — enough to detect WKT without reading the whole column
            sample = df[col].dropna().head(10).astype(str)
            if sample.str.match(wkt_pattern).any():
                return col

    return None  # no geometry column found


def try_parse_dates_and_numbers(df: pd.DataFrame, skip_col: str | None) -> pd.DataFrame:
    """
    Lightly coerce string columns to numeric or datetime types where appropriate.

    CSV files are often read entirely as strings (especially when dtype="string"
    is passed to read_csv).  This function attempts to convert each text column
    to a more specific type, which makes the column profile more meaningful.

    A column is only converted if at least 60% of its non-null values can be
    parsed successfully — this avoids accidentally converting a mixed-content
    column.

    The WKT geometry column is always skipped, since geometry strings would
    produce meaningless results if coerced.

    Parameters
    ----------
    df       : the raw DataFrame (all columns may be strings)
    skip_col : name of the geometry column to leave untouched (may be None)

    Returns
    -------
    A copy of the DataFrame with appropriate columns converted.
    """
    out = df.copy()

    for col in out.columns:
        # Never touch the geometry column — its content is WKT, not data
        if col == skip_col:
            continue

        # Only attempt coercion on text columns
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):

            # --- Try numeric conversion first ---
            numeric = pd.to_numeric(out[col], errors="coerce")
            n_valid = out[col].notna().sum()  # number of non-null values before conversion
            # Convert only if at least 60% of values (and at least 10) parsed successfully
            if n_valid > 0 and numeric.notna().sum() >= max(10, int(0.6 * n_valid)):
                out[col] = numeric
                continue  # skip the datetime check — this column is now numeric

            # --- Try datetime conversion if numeric didn't work ---
            dt = pd.to_datetime(out[col], errors="coerce", format="mixed")
            if n_valid > 0 and dt.notna().sum() >= max(10, int(0.6 * n_valid)):
                out[col] = dt

    return out


def profile_geom_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Profile a WKT geometry column without using any GIS library.

    Because WKT files don't go through geopandas, we can't use profile_geometry()
    (which expects a GeoDataFrame).  Instead this function extracts geometry
    statistics directly from the raw WKT strings using text processing.

    Statistics produced:
        - row count and null count
        - an example WKT string
        - min/max coordinate pair count (a rough proxy for geometry complexity)
        - counts of each geometry type (inferred from the WKT prefix)

    Parameters
    ----------
    df  : the DataFrame containing the WKT column
    col : the name of the WKT column

    Returns
    -------
    A two-column ("metric" / "value") DataFrame, matching the format of
    profile_geometry() so the Excel output is consistent across all formats.
    """
    # Work with the WKT column as a string Series
    s        = df[col].astype("string")
    n_rows   = len(s)
    non_null = int(s.notna().sum())

    def guess_geom_type(v: str) -> str:
        """Read the WKT prefix to determine the geometry type."""
        v2 = v.strip().upper()
        # Check from most specific to least specific so "MULTIPOLYGON" is
        # matched before "POLYGON", etc.
        for t in ["MULTIPOLYGON", "POLYGON", "MULTILINESTRING",
                  "LINESTRING", "MULTIPOINT", "POINT", "GEOMETRYCOLLECTION"]:
            if v2.startswith(t):
                return t
        return "unknown"

    # Regex to find coordinate pairs: two numbers separated by whitespace
    # e.g. "-63.58 44.64" — counts as a rough measure of geometry complexity
    pair_re = re.compile(r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)")

    geom_type_counts: dict[str, int] = {}
    pair_counts:      list[int]      = []
    example = ""

    # Only inspect the first 500 rows to keep runtime reasonable for large files
    for v in s.dropna().head(500):
        v_str = str(v)
        gt    = guess_geom_type(v_str)
        geom_type_counts[gt] = geom_type_counts.get(gt, 0) + 1

        # Save the first value as our example geometry
        if not example:
            example = format_example(v_str, max_len=220)

        # Count coordinate pairs in this WKT string
        pair_counts.append(len(pair_re.findall(v_str)))

    # Build the metric/value table
    rows = [
        {"metric": "wkt column",       "value": col},
        {"metric": "rows",             "value": n_rows},
        {"metric": "non-null",         "value": non_null},
        {"metric": "null",             "value": n_rows - non_null},
        {"metric": "example geometry", "value": example},
        {"metric": "approx coord-pairs (min of first 500)",
         "value": int(min(pair_counts)) if pair_counts else None},
        {"metric": "approx coord-pairs (max of first 500)",
         "value": int(max(pair_counts)) if pair_counts else None},
    ]
    # Add one row per geometry type, sorted by frequency (most common first)
    for gt, cnt in sorted(geom_type_counts.items(), key=lambda x: (-x[1], x[0])):
        rows.append({"metric": f"geom type count: {gt}", "value": cnt})

    return pd.DataFrame(rows)


# =============================================================================
# FORMAT-SPECIFIC FILE READERS
# Each function below handles one file format and returns its data in a
# normalised form that the shared profiling functions can process.
# =============================================================================

def read_geojson(file_path: Path) -> gpd.GeoDataFrame:
    """
    Read a GeoJSON file into a GeoDataFrame and ensure it is in WGS 84.

    GeoJSON files are supposed to always use EPSG:4326 (WGS 84 lat/lon) by
    specification, but some files omit the CRS metadata or use a different
    projection.  This function normalises the CRS so the profile is consistent.

    Parameters
    ----------
    file_path : path to a .geojson, .json, or .txt file

    Returns
    -------
    A GeoDataFrame in EPSG:4326.
    """
    gdf = gpd.read_file(str(file_path))

    if gdf.crs is None:
        # GeoJSON spec implies WGS 84, so assume it if not specified
        print("  Warning: no CRS found — assuming EPSG:4326 (WGS84, GeoJSON standard)")
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        # Reproject to WGS 84 if the file uses a different coordinate system
        print(f"  Warning: CRS is {gdf.crs} — reprojecting to EPSG:4326 (WGS84)")
        gdf = gdf.to_crs(epsg=4326)

    return gdf


def read_shapefile(zip_path: Path) -> gpd.GeoDataFrame:
    """
    Extract a zipped shapefile to a temporary folder and read it into a GeoDataFrame.

    Shapefiles consist of several companion files (.shp, .dbf, .prj, .shx, etc.)
    which are distributed together as a .zip archive.  This function extracts
    everything to a temporary directory, locates the .shp file, reads it, and
    then lets the temporary directory be automatically deleted.

    Also reprojects to EPSG:4326 and exports a GeoJSON as a side effect.

    Parameters
    ----------
    zip_path : path to a .zip file containing a shapefile

    Returns
    -------
    A GeoDataFrame in EPSG:4326.
    """
    # tempfile.TemporaryDirectory() creates a temp folder and automatically
    # deletes it (and everything inside) when the `with` block exits
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp)   # unpack all files into the temp folder

        # rglob("*.shp") searches recursively — handles zips with subdirectories
        shp_files = list(Path(tmp).rglob("*.shp"))

        if not shp_files:
            raise ValueError(f"No .shp file found inside {zip_path.name}")

        if len(shp_files) > 1:
            print(f"  Warning: multiple .shp files found; using first: {shp_files[0].name}")

        print(f"  Reading : {shp_files[0].name}")
        # Read the shapefile — geopandas reads .shp plus its companion files automatically
        gdf = gpd.read_file(str(shp_files[0]))

    # The temporary directory is deleted here as the `with` block exits.
    # gdf is safely loaded into memory, so this is fine.

    # Some Government of Canada shapefiles omit the .prj file (which holds CRS info).
    # NAD83 (EPSG:4269) is the standard for federal Canadian geospatial data.
    if gdf.crs is None:
        print("  Warning: no CRS found — assuming EPSG:4269 (NAD83, GC standard)")
        gdf = gdf.set_crs(epsg=4269)

    # Reproject to WGS 84 so all outputs use the same coordinate system
    gdf = gdf.to_crs(epsg=4326)

    # Side effect: export the reprojected data as a GeoJSON file.
    # This is useful for visual inspection and for use in downstream scripts.
    PARKS_DIR.mkdir(parents=True, exist_ok=True)
    geojson_out = PARKS_DIR / f"{zip_path.stem}.geojson"
    gdf.to_file(str(geojson_out), driver="GeoJSON")
    print(f"  GeoJSON : {geojson_out}")

    return gdf


def read_wkt(file_path: Path) -> tuple[pd.DataFrame, str | None]:
    """
    Read a WKT/CSV file into a plain DataFrame, detect its geometry column,
    and lightly coerce column types.

    Unlike GeoJSON and shapefiles, WKT files are read as plain CSV files rather
    than GeoDataFrames because geopandas can't always parse WKT directly from CSV.
    The geometry column is identified separately and profiled as raw text.

    Parameters
    ----------
    file_path : path to a .csv or .txt file with a WKT geometry column

    Returns
    -------
    A tuple of:
        - df      : the DataFrame with columns type-coerced where appropriate
        - wkt_col : the name of the WKT geometry column, or None if not found
    """
    # Read everything as strings first — this prevents pandas from silently
    # misinterpreting values (e.g. treating "001" as the integer 1)
    df_raw = pd.read_csv(file_path, dtype="string", low_memory=False)
    print(f"  Rows: {len(df_raw):,}  |  Columns: {len(df_raw.columns)}")

    # Find the column that contains WKT geometry strings
    wkt_col = detect_wkt_column(df_raw)
    if wkt_col:
        print(f"  WKT column detected: '{wkt_col}'")
    else:
        print("  Warning: no WKT geometry column detected — geometry profile will be skipped")

    # Coerce non-geometry columns to numeric or datetime where appropriate
    df = try_parse_dates_and_numbers(df_raw, skip_col=wkt_col)

    return df, wkt_col


# =============================================================================
# PER-FILE PROCESSING
# A single dispatcher function that reads any supported format and then runs
# the appropriate profiling functions.
# =============================================================================

def process_file(file_path: Path) -> None:
    """
    Profile a single boundary file and write an Excel report.

    This function acts as a dispatcher: it inspects the file extension to
    determine which reader to call, then runs the shared profiling logic on
    the result.  The three supported formats produce slightly different Excel
    outputs to match what is most useful for each.

        column profile: summary statistics table describing every non- geometry column
        geometry profile: summary statistics table describing geometry columns 
        data: all data in the source file transformed into tabular format

    Parameters
    ----------
    file_path : path to the file to be processed (any supported format)
    """
    print(f"\n{'='*60}")
    print(f"Processing: {file_path.name}")

    ext = file_path.suffix.lower()

    # -------------------------------------------------------------------------
    # BRANCH 1: GeoJSON
    # -------------------------------------------------------------------------
    if ext in GEOJSON_EXTENSIONS:
        gdf = read_geojson(file_path)

        # Build the attribute table (drop the geometry column — it's handled separately)
        df_attrs = pd.DataFrame(gdf.drop(columns=["geometry"], errors="ignore"))

        # Excel doesn't support timezone-aware datetimes — strip timezone info
        # from any datetime columns to avoid write errors
        for col in df_attrs.columns:
            if pd.api.types.is_datetime64_any_dtype(df_attrs[col]):
                if hasattr(df_attrs[col].dt, "tz") and df_attrs[col].dt.tz is not None:
                    df_attrs[col] = df_attrs[col].dt.tz_localize(None)

        df_col_profile = profile_columns(df_attrs)

        # Add geometry as WKT text so it's human-readable in the spreadsheet.
        # to_wkt() converts each geometry object to a string like "POLYGON (…)"
        df_data = df_attrs.copy()
        if "geometry" in gdf.columns:
            df_data["geometry_wkt"] = gdf.geometry.to_wkt()

        df_geom = profile_geometry(gdf)

        # Write threefour sheets to the Excel file
        output_xlsx = OUTPUT_DIR / f"profile_geojson_{file_path.stem}.xlsx"
        used: set[str] = set()
        with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
            df_col_profile.to_excel(writer, sheet_name=safe_sheet_name("column_profile",  used), index=False)
            df_geom.to_excel(       writer, sheet_name=safe_sheet_name("geometry_profile",        used), index=False)
            df_data.to_excel(       writer, sheet_name=safe_sheet_name("data",            used), index=False)


        print(f"  Profile : {output_xlsx}")
        print(f"  Rows    : {len(gdf):,}  |  Columns: {len(df_attrs.columns)}  |  CRS: {gdf.crs}")

    # -------------------------------------------------------------------------
    # BRANCH 2: Shapefile (zipped)
    # -------------------------------------------------------------------------
    elif ext == ".zip":
        gdf = read_shapefile(file_path)

        # Same structure as GeoJSON: attribute table, column profile, data+WKT, geometry summary
        df_attrs = pd.DataFrame(gdf.drop(columns=["geometry"], errors="ignore"))
        df_col_profile = profile_columns(df_attrs)

        df_data = df_attrs.copy()
        if "geometry" in gdf.columns:
            df_data["geometry_wkt"] = gdf.geometry.to_wkt()

        df_geom = profile_geometry(gdf)

        # Write threefour sheets to the Excel file
        output_xlsx = OUTPUT_DIR / f"profile_shp_{file_path.stem}.xlsx"
        used = set()
        with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
            df_col_profile.to_excel(writer, sheet_name=safe_sheet_name("column_profile",  used), index=False)
            df_geom.to_excel(       writer, sheet_name=safe_sheet_name("geometry_profile",        used), index=False)
            df_data.to_excel(       writer, sheet_name=safe_sheet_name("data",            used), index=False)

        print(f"  Profile : {output_xlsx}")
        print(f"  Rows    : {len(gdf):,}  |  Columns: {len(df_attrs.columns)}  |  CRS: {gdf.crs}")

    # -------------------------------------------------------------------------
    # BRANCH 3: WKT / CSV
    # -------------------------------------------------------------------------
    elif ext in WKT_EXTENSIONS:
        df, wkt_col = read_wkt(file_path)

        df_col_profile = profile_columns(df)
        # Only produce a geometry profile if a WKT column was found
        df_geom = profile_geom_column(df, wkt_col) if wkt_col else pd.DataFrame()

        # Write the four sheets to the Excel file
        output_xlsx = OUTPUT_DIR / f"profile_wkt_{file_path.stem}.xlsx"
        used = set()
        with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
            df_col_profile.to_excel(writer, sheet_name=safe_sheet_name("column_profile", used), index=False)
            if not df_geom.empty:
                df_geom.to_excel(   writer, sheet_name=safe_sheet_name("geom_profile",  used), index=False)
            df.to_excel(       writer, sheet_name=safe_sheet_name("data",            used), index=False)

        print(f"  Profile : {output_xlsx}")

    # -------------------------------------------------------------------------
    # Unrecognised extension — shouldn't happen if main() filters correctly
    # -------------------------------------------------------------------------
    else:
        print(f"  Skipping: unrecognised file extension '{ext}'")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    """
    Profile boundary files in one of three modes:

        Default (no arguments):
            Scans all three configured source folders automatically.

        --folder <path>:
            Scans a single folder you specify. The format (GeoJSON, Shapefile,
            or WKT) is inferred from the file extensions found inside.

        --file <path>:
            Profiles a single file you specify.

    Usage examples:
        python profile_boundaries.py
        python profile_boundaries.py --folder path/to/my/folder
        python profile_boundaries.py --file path/to/my/file.geojson
    """
    parser = argparse.ArgumentParser(description="Profile park boundary files.")
    parser.add_argument("--folder", type=str, help="Path to a single folder to scan")
    parser.add_argument("--file",   type=str, help="Path to a single file to profile")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_attempted = 0
    total_succeeded = 0

    def process_folder(folder: Path, extensions: set[str], label: str) -> None:
        """Scan a folder for matching files and profile each one."""
        nonlocal total_attempted, total_succeeded

        if not folder.exists():
            print(f"\n[{label}] Folder not found — skipping: {folder}")
            return

        files = sorted(f for f in folder.iterdir()
                       if f.is_file() and f.suffix.lower() in extensions)

        if not files:
            print(f"\n[{label}] No files found in {folder}")
            return

        print(f"\n[{label}] Found {len(files)} file(s) in {folder}")

        for file_path in files:
            total_attempted += 1
            try:
                process_file(file_path)
                total_succeeded += 1
            except Exception as exc:
                print(f"  ERROR processing {file_path.name}: {exc}")

    # All supported extensions combined — used when scanning an unknown folder
    all_extensions = GEOJSON_EXTENSIONS | WKT_EXTENSIONS | {".zip"}

    # -----------------------------------------------------------------------
    # MODE 1: Single file
    # -----------------------------------------------------------------------
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        total_attempted += 1
        try:
            process_file(file_path)
            total_succeeded += 1
        except Exception as exc:
            print(f"  ERROR processing {file_path.name}: {exc}")

    # -----------------------------------------------------------------------
    # MODE 2: Single folder
    # -----------------------------------------------------------------------
    elif args.folder:
        folder = Path(args.folder)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder}")
        # Scan for all supported extensions — format is inferred per file inside process_file()
        process_folder(folder, all_extensions, label=folder.name)

    # -----------------------------------------------------------------------
    # MODE 3: Default — scan all three configured folders
    # -----------------------------------------------------------------------
    else:
        process_folder(Path(GEOJSON_DIR), GEOJSON_EXTENSIONS, "GeoJSON")
        process_folder(Path(SHP_DIR),     {".zip"},           "Shapefile")
        process_folder(Path(WKT_DIR),     WKT_EXTENSIONS,     "WKT/CSV")

    # Final summary
    print(f"\n{'='*60}")
    print(f"Done. {total_succeeded}/{total_attempted} file(s) profiled successfully.")
    print(f"Profiles written to: {OUTPUT_DIR}")


# This block only runs when the script is executed directly:
#     python profile_boundaries.py
# It does NOT run when this file is imported as a module by another script.
# This is a Python best practice that makes scripts safe to import.
if __name__ == "__main__":
    main()