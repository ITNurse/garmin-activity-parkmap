"""
02_preprocess_park_boundaries.py

Processes all park boundary files in config.RAW_PARKS_DIR and writes
normalised GeoJSON files to config.PARKS_DIR.

Three input types are handled automatically by file extension:

    .zip        Zipped shapefile — reprojected to WGS-84 (EPSG:4326) and
                written as GeoJSON.  An optional per-file column filter can
                be defined in FILE_FILTERS below (e.g. to keep only
                "Parc national" rows from Quebec's mixed-designation dataset).

    .csv / .txt CSV or TXT file containing a WKT geometry column — the
                geometry column is auto-detected from a list of common names.
                All other columns are preserved as GeoJSON feature properties.

    .geojson    Already-valid GeoJSON — copied to config.PARKS_DIR as-is.
                The copy is logged explicitly so it appears in the run record
                alongside converted files.

Any other file extensions are skipped with a notice.

Usage:
    python 02_preprocess_park_boundaries.py

Output:
    One .geojson file per supported input file in config.PARKS_DIR.

Dependencies:
    pip install geopandas shapely
"""

import csv
import json
import shutil
import sys
from pathlib import Path

# geopandas reads spatial file formats (shapefiles, GeoJSON, etc.) into a
# GeoDataFrame — a pandas DataFrame that also knows about geometry and
# coordinate reference systems (CRS).
try:
    import geopandas as gpd
except ImportError:
    sys.exit("Error: geopandas is required. Install with:  pip install geopandas")

# shapely handles the actual geometry objects. wkt.loads() parses a WKT string
# into a geometry object; mapping() converts that object to a GeoJSON-compatible
# dict that can be serialised with the standard json module.
try:
    from shapely import wkt
    from shapely.geometry import mapping
except ImportError:
    sys.exit("Error: shapely is required. Install with:  pip install shapely")

# WKT geometry strings (e.g. a polygon with thousands of coordinate pairs) can
# be extremely long — far beyond Python's csv module default field size limit.
# sys.maxsize is the largest integer Python can represent on this platform;
# 2147483647 is 2 GB, which is a common hard ceiling on 32-bit systems.
# min() picks whichever is smaller so we don't exceed what the platform allows.
csv.field_size_limit(min(sys.maxsize, 2147483647))

# __file__ is the path to this script file. resolve() makes it absolute,
# removing any ".." components. .parent three times walks up three directory
# levels (src/pipeline/ → src/ → project root) to find config.py.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Insert the project root at the front of the module search path so that
# "import config" finds our config.py rather than any installed package
# that might share the same name.
sys.path.insert(0, str(PROJECT_ROOT))
import config

# ---------------------------------------------------------------------------
# Per-file column filters for zipped shapefiles.
#
# Some provincial open-data shapefiles bundle multiple designation types
# (e.g. provincial parks, ecological reserves, and wildlife areas) in one
# file. FILE_FILTERS lets you keep only the rows you care about.
#
# Keys   — the file stem (filename without extension, e.g. "quebec")
# Values — a dict of {column_name: value_to_keep}
#
# Add an entry here whenever a source file needs to be filtered down.
# Files whose stem does not appear in this dict are processed unfiltered.
# ---------------------------------------------------------------------------
FILE_FILTERS: dict[str, dict] = {
    # Quebec's open-data shapefile uses DESIGNOM to distinguish park types.
    # "Parc national" is the designation for provincial parks specifically.
    "quebec": {"DESIGNOM": "Parc national"},
}

# ---------------------------------------------------------------------------
# Candidate column names for WKT geometry in CSV/TXT files.
#
# Different data providers use different column names for the geometry column.
# We try each name in order and use the first one that exists in the file.
# The comparison is case-insensitive (see _detect_geom_col below).
# ---------------------------------------------------------------------------
GEOM_COL_CANDIDATES = [
    "the_geom", "geom", "geometry", "wkt", "wkt_geom",
    "shape", "geo", "geographic_data", "spatial_data",
]


# ---------------------------------------------------------------------------
# Shapefile handler
# ---------------------------------------------------------------------------

def process_zip(input_path: Path, output_path: Path) -> None:
    """Convert a zipped shapefile to GeoJSON, reprojecting to EPSG:4326.

    Args:
        input_path:  Path to the .zip file containing the shapefile components
                     (.shp, .dbf, .shx, etc.).
        output_path: Path where the resulting .geojson file will be written.

    Behaviour:
        - Reads the shapefile directly from the zip without extracting it.
        - Reprojects geometry to WGS-84 (EPSG:4326), which is the coordinate
          system required by the GeoJSON specification.
        - Applies any column filters defined in FILE_FILTERS for this file.
        - Drops any features with null/missing geometry.
        - Delegates writing to _write_geodataframe().
        - Prints progress and a sample of the first feature's properties.
        - On read error, prints the error message and returns without writing.
    """
    # Look up whether this file has a filter defined. .get() returns None if
    # the stem is not in the dict, which is a safe "no filter" signal.
    filters = FILE_FILTERS.get(input_path.stem)

    # geopandas can read a shapefile directly from inside a zip using the
    # "zip://" URI prefix — no need to extract the archive first.
    try:
        gdf = gpd.read_file(f"zip://{input_path}")
    except Exception as e:
        # Catch-all: file may be corrupt, not a valid shapefile, missing
        # required components (.dbf, .shx), etc.
        print(f"  ERROR reading {input_path.name}: {e}")
        return  # Skip this file; don't crash the whole run

    print(f"  Loaded {len(gdf)} features | CRS: {gdf.crs}")

    # GeoJSON requires coordinates in WGS-84 (EPSG:4326 — decimal degrees,
    # latitude/longitude). Most Canadian open data is in a different CRS
    # (e.g. NAD83), so reprojection is nearly always needed.
    if gdf.crs is None:
        # Some shapefiles omit the .prj file that defines the CRS.
        # We assume EPSG:4326 and warn — the output may be wrong if the
        # assumption is incorrect, but at least the run continues.
        print("  WARNING: No CRS found; assuming EPSG:4326")
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        # to_crs() reprojects all geometry in the GeoDataFrame to the
        # target CRS, returning a new GeoDataFrame.
        print(f"  Reprojecting from {gdf.crs} → EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)

    # Apply column filters if defined for this file (e.g. Quebec).
    # filters is either None (no filter) or a dict of {col: value}.
    if filters:
        for col, val in filters.items():
            if col not in gdf.columns:
                # The expected column isn't present — warn but continue,
                # because it's better to keep all rows than to silently
                # produce an empty file.
                print(f"  WARNING: Filter column '{col}' not found; skipping filter")
                continue

            before = len(gdf)

            # Boolean indexing: gdf[col] == val produces a True/False Series;
            # passing it back into gdf[] keeps only the True rows.
            # reset_index(drop=True) re-numbers the rows from 0 so the index
            # stays clean after the subset operation.
            gdf = gdf[gdf[col] == val].reset_index(drop=True)

            print(f"  Filtered on {col}='{val}': {before} → {len(gdf)} features")

    # Remove any features whose geometry is null/missing. These would produce
    # invalid GeoJSON and cause errors in the matching stage.
    before = len(gdf)
    gdf = gdf[gdf.geometry.notna()].reset_index(drop=True)
    if len(gdf) < before:
        print(f"  Dropped {before - len(gdf)} features with null geometry")

    # Hand off to the shared writer so file-writing and logging are consistent
    # across all handlers that produce a GeoDataFrame.
    _write_geodataframe(gdf, output_path)


# ---------------------------------------------------------------------------
# WKT / CSV handler
# ---------------------------------------------------------------------------

def _detect_geom_col(fieldnames: list[str]) -> str | None:
    """Find the geometry column in a CSV by trying a list of common names.

    The comparison is case-insensitive: a column called "Geometry" or
    "GEOMETRY" will match the candidate "geometry".

    Args:
        fieldnames: The list of column names from the CSV header row.

    Returns:
        The original (un-lowercased) column name if a match is found,
        or None if no candidate name exists in the file.
    """
    # Build a dict mapping each lowercased field name back to its original
    # form. This lets us match case-insensitively while returning the name
    # exactly as it appears in the file (needed to look up the column later).
    lower_map = {f.lower(): f for f in fieldnames}

    # Try each candidate in preference order and return the first match.
    for candidate in GEOM_COL_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]  # Return the original-case name

    # None of the candidates matched — caller will handle this as an error.
    return None


def process_wkt(input_path: Path, output_path: Path) -> None:
    """Convert a CSV/TXT file with a WKT geometry column to GeoJSON.

    WKT (Well-Known Text) is a plain-text format for representing geometry,
    e.g. "POLYGON ((...))" or "POINT (...)". Some provinces distribute their
    park boundary data as CSV files with one WKT geometry string per row.

    This function:
        - Auto-detects the geometry column using _detect_geom_col().
        - Parses each WKT string into a geometry object with shapely.
        - Converts the geometry to a GeoJSON-compatible dict with mapping().
        - Preserves all non-geometry columns as GeoJSON feature properties.
        - Skips rows with missing or unparseable geometry (with a count).
        - Writes the result as a GeoJSON FeatureCollection.

    Args:
        input_path:  Path to the source .csv or .txt file.
        output_path: Path where the resulting .geojson file will be written.
    """
    # We'll build up a list of GeoJSON Feature dicts, one per CSV row.
    features = []

    try:
        # newline="" is required by the csv module — it lets the csv reader
        # handle line endings itself rather than having Python normalise them,
        # which can cause problems with quoted fields containing newlines.
        with input_path.open(newline="", encoding="utf-8") as f:
            # DictReader reads each row as an OrderedDict keyed by the header
            # row column names, which is much easier to work with than a plain
            # list of values.
            reader = csv.DictReader(f)

            # Try to find the geometry column before processing any rows.
            # reader.fieldnames is populated after the first read of the file
            # header. We use "or []" as a safety net if it comes back as None.
            geom_col = _detect_geom_col(reader.fieldnames or [])

            if geom_col is None:
                # Can't proceed without a geometry column — report what we
                # found and bail out, leaving no output file.
                print(f"  ERROR: Could not find a geometry column in {input_path.name}.")
                print(f"  Available columns: {', '.join(reader.fieldnames or [])}")
                print(f"  Expected one of: {', '.join(GEOM_COL_CANDIDATES)}")
                return

            print(f"  Geometry column: '{geom_col}'")

            skipped = 0  # Running count of rows we couldn't process

            # enumerate() gives us both the index (i) and the row dict so
            # we can report the row number in warning messages.
            for i, row in enumerate(reader):
                # row.pop() removes the geometry column from the dict and
                # returns its value. This means the remaining items in `row`
                # are exactly the non-geometry properties we want to preserve.
                # .strip() removes leading/trailing whitespace.
                wkt_str = row.pop(geom_col, "").strip()

                if not wkt_str:
                    # Empty geometry cell — skip silently (counted below).
                    skipped += 1
                    continue

                try:
                    # wkt.loads() parses the WKT string into a shapely
                    # geometry object (e.g. Polygon, MultiPolygon, Point).
                    geom = wkt.loads(wkt_str)
                except Exception as e:
                    # The WKT string is malformed — skip this row and warn.
                    # i + 1 because enumerate starts at 0 but rows are
                    # conventionally counted from 1 (row 0 is the header).
                    print(f"  Warning: Row {i + 1} has invalid WKT ({e}) — skipping.")
                    skipped += 1
                    continue

                # mapping() converts a shapely geometry object into a plain
                # Python dict matching the GeoJSON geometry structure, e.g.:
                # {"type": "Polygon", "coordinates": [...]}
                # This is necessary because json.dump() can't serialise
                # shapely objects directly.
                features.append({
                    "type": "Feature",
                    "geometry": mapping(geom),
                    # dict(row) creates a plain dict copy of the remaining
                    # columns (after the geometry column was popped above).
                    "properties": dict(row),
                })

        if skipped:
            print(f"  Skipped {skipped} row(s) with missing or invalid geometry")

    except Exception as e:
        # Catch errors opening or reading the file (permissions, encoding, etc.)
        print(f"  ERROR reading {input_path.name}: {e}")
        return

    # Wrap the list of Feature dicts in a FeatureCollection — this is the
    # top-level structure required by the GeoJSON specification (RFC 7946).
    geojson = {"type": "FeatureCollection", "features": features}

    # mkdir(parents=True) creates any missing parent directories;
    # exist_ok=True means it won't error if the folder already exists.
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        # indent=2 makes the file human-readable (one line per key/value).
        # ensure_ascii=False preserves accented characters (é, è, etc.) rather
        # than replacing them with escaped unicode sequences (\u00e9).
        json.dump(geojson, f, indent=2, ensure_ascii=False)

    print(f"  Saved {len(features)} features → {output_path}")

    # Print a sample of the first feature's properties as a sanity check —
    # makes it easy to spot if columns got mis-mapped or are unexpectedly empty.
    _print_sample_properties(features[0]["properties"] if features else {})


# ---------------------------------------------------------------------------
# GeoJSON passthrough handler
# ---------------------------------------------------------------------------

def process_geojson(input_path: Path, output_path: Path) -> None:
    """Copy an already-valid GeoJSON file to the output folder.

    No conversion is needed, but the copy is logged explicitly so it appears
    in the run record alongside converted files. This makes it clear the
    script ran and accounted for this file, rather than silently skipping it.

    shutil.copy2() is used instead of shutil.copy() because copy2() also
    preserves file metadata (timestamps, permissions), which is generally
    the more courteous thing to do with source data.

    Args:
        input_path:  Path to the source .geojson file.
        output_path: Path where the copy will be written.
    """
    shutil.copy2(input_path, output_path)
    print(f"  Copied → {output_path}")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _write_geodataframe(gdf: "gpd.GeoDataFrame", output_path: Path) -> None:
    """Write a GeoDataFrame to a GeoJSON file and print a property sample.

    This is a shared utility used by process_zip() (and any future handlers
    that produce a GeoDataFrame) so that file-writing and logging behaviour
    is consistent across all shapefile-like sources.

    The function name starts with an underscore to signal that it is a private
    helper — intended for use within this module only, not called externally.

    Args:
        gdf:         The GeoDataFrame to write. Must already be in EPSG:4326.
        output_path: Path where the .geojson file will be written.
    """
    # mkdir with parents=True and exist_ok=True ensures the output folder
    # exists without raising an error if it was already created.
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # geopandas' to_file() handles serialising geometry and properties to disk.
    # driver="GeoJSON" explicitly selects the GeoJSON output format.
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"  Saved {len(gdf)} features → {output_path}")

    # Re-read the file we just wrote so we can print a sample of the first
    # feature's properties. This acts as a quick sanity check that the output
    # looks reasonable before moving on to the next file.
    with open(output_path, encoding="utf-8") as f:
        data = json.load(f)
    if data["features"]:
        _print_sample_properties(data["features"][0].get("properties", {}))


def _print_sample_properties(props: dict) -> None:
    """Print up to 8 key-value pairs from a feature's properties dict.

    Used as a quick visual sanity check after each file is processed, so the
    user can see what columns and values are present without having to open
    the output file manually.

    The function name starts with an underscore to signal that it is a private
    helper — intended for use within this module only.

    Args:
        props: A dict of property names to values from a single GeoJSON feature.
               Typically the first feature in the file.
    """
    if props:
        print("  Sample properties from first feature:")
        # list(props.items()) converts the dict to a list of (key, value) tuples.
        # [:8] slices the first 8 items so we don't flood the console for
        # files with many columns.
        for k, v in list(props.items())[:8]:
            print(f"    {k}: {v}")


# ---------------------------------------------------------------------------
# Dispatch table
#
# A dispatch table is a dict that maps keys to functions. It lets us choose
# which handler to call based on file extension without a long if/elif chain.
# To add support for a new format, just add one line here and write the handler.
# ---------------------------------------------------------------------------

HANDLERS = {
    ".zip":     process_zip,      # Zipped shapefiles
    ".csv":     process_wkt,      # CSV files with WKT geometry column
    ".txt":     process_wkt,      # TXT files with WKT geometry column
    ".geojson": process_geojson,  # Already-valid GeoJSON (copy through)
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point: discover all supported files in RAW_PARKS_DIR and process each.

    For each supported file, the appropriate handler is selected from the
    HANDLERS dispatch table and called with the input and output paths.
    Unsupported file types are reported but do not cause an error.
    """
    input_folder  = Path(config.RAW_PARKS_DIR)
    output_folder = Path(config.PARKS_DIR)

    if not input_folder.exists():
        # sys.exit() prints the message and terminates with a non-zero exit
        # code, signalling failure to any calling process or shell script.
        sys.exit(
            f"Error: Raw parks folder not found: {input_folder}\n"
            f"Set RAW_PARKS_DIR in config.py."
        )

    # Build a sorted list of files whose extension is in the HANDLERS dict.
    # f.suffix returns the extension including the dot (e.g. ".zip").
    # .lower() normalises case so ".ZIP" and ".Zip" are treated the same.
    # f.is_file() excludes subdirectories.
    all_files = sorted(
        f for f in input_folder.iterdir()
        if f.is_file() and f.suffix.lower() in HANDLERS
    )

    # Separately collect files we can't handle, so the user knows they exist
    # and weren't silently ignored.
    unsupported = sorted(
        f for f in input_folder.iterdir()
        if f.is_file() and f.suffix.lower() not in HANDLERS
    )
    if unsupported:
        print("Skipping unsupported file types:")
        for f in unsupported:
            print(f"  {f.name}")

    if not all_files:
        print(f"No supported park boundary files found in {input_folder}")
        return  # Nothing to do — exit cleanly without an error code

    print(f"Found {len(all_files)} file(s) to process in {input_folder}")

    # Create the output folder if it doesn't already exist.
    output_folder.mkdir(parents=True, exist_ok=True)

    for input_path in all_files:
        suffix = input_path.suffix.lower()

        # Build the output path: same stem as the input, but always .geojson,
        # in the output folder. e.g. "new_brunswick.csv" → "new_brunswick.geojson"
        output_path = output_folder / (input_path.stem + ".geojson")

        # Look up the handler function for this file type.
        handler = HANDLERS[suffix]

        # Print a separator and the filename so each file's output is clearly
        # grouped in the console. "=" * 60 repeats the character 60 times.
        print(f"\n{'='*60}")
        print(f"Processing: {input_path.name}  [{suffix}]")

        # Call whichever handler function was selected — process_zip,
        # process_wkt, or process_geojson — passing the same two arguments.
        # This uniform interface is what makes the dispatch table pattern work.
        handler(input_path, output_path)

    print(f"\nDone. GeoJSON files written to {output_folder}")


# ---------------------------------------------------------------------------
# Script entry point guard
#
# This block only runs when the file is executed directly (e.g.
# "python 02_preprocess_park_boundaries.py"). If this file were imported as
# a module by another script, main() would NOT be called automatically.
# This is a standard Python pattern for files that can work both as a
# runnable script and as an importable module.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()