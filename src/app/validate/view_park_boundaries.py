#!/usr/bin/env python3
"""
view_park_boundaries.py
-----------------------
Interactive web viewer for park boundaries.

Parks are displayed in the sidebar with search, sorting, and filtering by
source file, province, and park type. Click a park to highlight it on the
map; click again to deselect.

Usage:
    python scripts/view_park_boundaries.py

    Then open http://127.0.0.1:5000 in your browser.
    Press Ctrl+C in the terminal to stop the server.

Requirements:
    pip install flask pandas openpyxl
"""

# Standard library imports — these come with Python, no installation needed.
import sys
import json
from pathlib import Path  # Path is a modern, cross-platform way to work with file paths.

# Third-party imports — these must be installed via pip.
import pandas as pd       # pandas is the standard library for reading spreadsheets and tabular data.
from flask import Flask, jsonify, render_template_string  # Flask is a lightweight web framework.

# =============================================================================
# PROJECT ROOT & CONFIG
# =============================================================================

# __file__ is the path to this script. We walk up four parent directories to
# reach the project root, then add it to sys.path so Python can find config.py.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Try to import the project config. If it's missing, print a helpful error and
# exit immediately — there's no point continuing without configuration values.
try:
    import config
    HOST = config.VIEWER_HOST   # e.g. "127.0.0.1"
    PORT = config.VIEWER_PORT   # e.g. 5000
except ImportError:
    print("ERROR: config.py not found.")
    print(f"  Expected location: {PROJECT_ROOT / 'config.py'}")
    sys.exit(1)  # sys.exit(1) means "exit with an error code"

# Build Path objects for the two key directories/files we need at runtime.
PARKS_DIR      = Path(config.PARKS_DIR)
FIELD_MAP_PATH = Path(config.DATA_OUTPUTS) / "Field_Names_By_Province.xlsx"


# =============================================================================
# FIELD MAP — loaded from Excel at startup
#
# SOURCE_FIELDS maps a lowercase keyword (matching part of a GeoJSON filename)
# to a dict with three keys — "name", "type", "area" — each holding a list
# containing the single GeoJSON property name to use for that attribute.
#
# Example entry:
#   "alberta": {
#       "name": ["NAME"],
#       "type": ["TYPE"],
#       "area": ["HECTARES"],
#   }
#
# "national_parks" is always checked first so that files like
# ON_National_Parks.geojson don't accidentally match the "ontario" entry.
# =============================================================================

def _load_field_map(path: Path) -> dict[str, dict]:
    """
    Read Field_Names_By_Province.xlsx and build the SOURCE_FIELDS lookup.

    The workbook has two sheets:

    - "ProvincialParks": one row per province. Columns are Province (sentence
      case name, e.g. "British Columbia"), Name Field, Type Field, Area Field.
      The province name is converted to a lowercase underscore keyword
      (e.g. "british_columbia") to match against the GeoJSON filename stem.

    - "National Parks": a single row with Name Field and Type Field only,
      stored under the key "national_parks".

    Some cells contain annotations that are stripped before use:
      - "PARKTYPE (coded)"      → strips "(coded)", leaving "PARKTYPE"
      - "TYPE_ENG + PROV_CLASS" → strips the "+ ..." part, leaving "TYPE_ENG"

    The actual acronym expansion and composite-field logic is handled later by
    PARK_TYPE_HOOKS, not here.

    Args:
        path: Path to the Excel workbook.

    Returns:
        A dict mapping filename keywords to field spec dicts, or an empty dict
        if the file doesn't exist (the caller falls back to generic field names).
    """
    if not path.exists():
        # Warn but don't crash — generic fallback chains will be used instead.
        print(f"WARNING: Field map not found at {path}. Falling back to generic field names.")
        return {}

    def _province_to_keyword(province: str) -> str:
        """
        Convert a sentence-case province name to a lowercase underscore keyword
        that can be matched against GeoJSON filename stems.

        For example:
            "British Columbia" → "british_columbia"
            "Nova Scotia"      → "nova_scotia"
            "Alberta"          → "alberta"

        Args:
            province: The province name as it appears in the spreadsheet.

        Returns:
            A lowercase, underscore-separated keyword string.
        """
        # .lower() converts to lowercase, .replace() swaps spaces for underscores.
        # These are chained — Python evaluates left to right, passing the result
        # of each method call into the next.
        return province.strip().lower().replace(" ", "_")

    def _parse_fields(raw: str | float) -> list[str]:
        """
        Convert a single spreadsheet cell value into an ordered list of
        GeoJSON field name candidates.

        Handles two annotation patterns written into the spreadsheet:
          - "(coded)" suffix  → stripped (acronym expansion is done by hooks)
          - "+ OTHER_FIELD"   → stripped (composite logic is done by hooks)

        Each area cell now contains exactly one field name, so no splitting
        on " or " is needed.

        Args:
            raw: The raw cell value from pandas (may be a float NaN if empty).

        Returns:
            A list containing the single cleaned field name, or an empty list
            if the cell is blank.
        """
        # pandas represents empty cells as float NaN, so we check for that too.
        if not raw or (isinstance(raw, float) and pd.isna(raw)):
            return []

        raw = str(raw).strip()

        # Strip the "(coded)" annotation — e.g. "PARKTYPE (coded)" → "PARKTYPE"
        raw = raw.split("(coded)")[0].strip()

        # Strip the "+ OTHER_FIELD" annotation — e.g. "TYPE_ENG + PROV_CLASS" → "TYPE_ENG"
        # The hook for that province will handle reading the second field itself.
        raw = raw.split("+")[0].strip()

        # Return as a single-element list to keep the return type consistent
        # with _first(), which always expects a list to iterate over.
        return [raw] if raw else []

    # pd.read_excel with sheet_name=None reads ALL sheets into a dict of
    # {sheet_name: DataFrame}.  This lets us handle both tabs in one call.
    xl = pd.read_excel(path, sheet_name=None)

    # result will become SOURCE_FIELDS once this function returns.
    result: dict[str, dict] = {}

    # --- Process the ProvincialParks tab ---
    # xl.get() returns an empty DataFrame if the sheet name doesn't exist,
    # which is safer than xl["ProvincialParks"] which would raise a KeyError.
    prov_df = xl.get("ProvincialParks", pd.DataFrame())

    # iterrows() yields (index, row) pairs; we use _ to discard the index
    # since we don't need row numbers here.
    for _, row in prov_df.iterrows():
        province = str(row.get("Province", "")).strip()

        # Skip blank rows (pandas may read trailing empty rows from Excel).
        if not province or province == "nan":
            continue

        # Convert "British Columbia" → "british_columbia" so we can match it
        # against the GeoJSON filename stem (e.g. "british_columbia_provincial_parks").
        keyword = _province_to_keyword(province)

        result[keyword] = {
            "name": _parse_fields(row.get("Name Field")),
            "type": _parse_fields(row.get("Type Field")),
            "area": _parse_fields(row.get("Area Field")),
        }

    # --- Process the National Parks tab ---
    nat_df = xl.get("National Parks", pd.DataFrame())
    if not nat_df.empty:
        # There's only one row on this tab — iloc[0] grabs the first (only) row.
        row = nat_df.iloc[0]
        result["national_parks"] = {
            "name": _parse_fields(row.get("Name Field")),
            "type": _parse_fields(row.get("Type Field")),
            "area": [],  # The National Parks tab has no area column.
        }

    return result


# Load the field map once at startup so every request can use it without
# re-reading the file. Module-level code in Python runs exactly once when the
# module is first imported (or when the script is run directly).
SOURCE_FIELDS: dict[str, dict] = _load_field_map(FIELD_MAP_PATH)


def _get_source_fields(stem_lower: str) -> dict | None:
    """
    Find and return the field spec for a given GeoJSON filename stem.

    We always check for "national_parks" first. Without this, a file called
    ON_National_Parks.geojson would match the "ontario" entry because "ontario"
    is a substring of "on_national_parks" — which is wrong.

    Args:
        stem_lower: The lowercase filename stem (e.g. "ontario_provincial_parks").

    Returns:
        The matching field spec dict from SOURCE_FIELDS, or None if no match.
        Returning None tells the caller to use the generic fallback chains.
    """
    # Explicit national parks check must come before the province keyword search.
    if "national_parks" in stem_lower:
        return SOURCE_FIELDS.get("national_parks")

    # next() with a generator expression returns the first match it finds, or
    # the default value (None) if nothing matches. This is more efficient than
    # building the whole list first, since it stops at the first hit.
    return next(
        (spec for key, spec in SOURCE_FIELDS.items()
         if key != "national_parks" and key in stem_lower),
        None,  # default returned when the generator is exhausted with no match
    )


def _first(props: dict, fields: list[str]):
    """
    Return the value of the first field in 'fields' that exists and is
    non-empty in the 'props' dictionary.

    This lets us express "try FIELD_A, then FIELD_B, then FIELD_C" cleanly,
    which is needed when a province might store area in either acres or hectares
    depending on the record.

    Args:
        props:  A GeoJSON feature's properties dict.
        fields: An ordered list of field names to try.

    Returns:
        The first matching value, or None if none of the fields are present
        or all of their values are empty/whitespace.
    """
    for f in fields:
        v = props.get(f)  # dict.get() returns None if the key doesn't exist
        # Check both that the value exists AND that it's not just whitespace.
        if v is not None and str(v).strip():
            return v
    return None  # Explicit return of None signals "nothing was found"


# =============================================================================
# FLASK APP
# =============================================================================

# Flask(__name__) creates the web application. Passing __name__ tells Flask
# where to look for templates and static files relative to this script.
app = Flask(__name__)

if not PARKS_DIR.exists():
    print(f"Warning: Parks folder not found: {PARKS_DIR}")


# =============================================================================
# PER-SOURCE PARK TYPE RESOLUTION
#
# Some provinces store park types as short acronyms (e.g. "PP" for Provincial
# Park), or need to combine two fields to build the final type string.  We
# handle this with a hook system: PARK_TYPE_HOOKS maps a filename keyword to a
# callable that receives the raw feature properties and the initial type string,
# and returns the final resolved type string.
#
# Hooks are called AFTER the raw type is read from the GeoJSON, so they always
# get a chance to refine or replace whatever was extracted.
#
# To add a new province:
#   - For acronym-only cases, add an entry to PARK_TYPE_HOOKS using acronym_map().
#   - For more complex logic, write a function and add it directly.
# =============================================================================

def to_sentence_case(text: str) -> str:
    """
    Convert an ALL-CAPS string to readable title/sentence case.

    Rules:
      - Every word is capitalised, EXCEPT small joining words (of, the, and…)
        which are kept lowercase — unless they are the very first word.
      - Hyphenated words are handled segment by segment: "NORTH-WEST" → "North-West".
      - If the text is NOT all-uppercase (i.e. it already has mixed casing),
        we return it untouched to avoid corrupting data that's already correct.

    Args:
        text: The string to convert.

    Returns:
        The converted string, or the original if it wasn't all-uppercase.

    Examples:
        "PROVINCIAL PARK"      → "Provincial Park"
        "PROTECTED AREA"       → "Protected Area"
        "NORTH-WEST TERRITORY" → "North-West Territory"
        "Provincial Park"      → "Provincial Park"  (already mixed-case, unchanged)
    """
    # isupper() returns True only if ALL cased characters are uppercase.
    # This guards against accidentally title-casing names that are already correct.
    if not text or not text.isupper():
        return text

    # Words that should stay lowercase when they appear in the middle of a string.
    LOWERCASE_WORDS = {
        "a", "an", "and", "as", "at", "but", "by", "for", "from",
        "in", "into", "nor", "of", "on", "or", "the", "to", "up",
        "via", "with",
    }

    def _title_word(word: str) -> str:
        """Capitalise each hyphen-separated segment of a word individually."""
        parts = word.split("-")
        # str.capitalize() lowercases everything then uppercases the first letter.
        return "-".join(p.capitalize() for p in parts)

    words  = text.split()
    result = []
    for idx, word in enumerate(words):
        lower = word.lower()
        # The first word is always capitalised, regardless of LOWERCASE_WORDS.
        if idx == 0 or lower not in LOWERCASE_WORDS:
            result.append(_title_word(word))
        else:
            result.append(lower)

    # " ".join(...) reassembles the list back into a single string.
    return " ".join(result)


# Sets of filename keywords identifying sources whose data is stored in ALL-CAPS.
# We convert names/types from these sources to title case after loading.
NAME_CASE_SOURCES: set[str] = {"british_columbia", "ontario"}
TYPE_CASE_SOURCES: set[str] = {"british_columbia"}


def acronym_map(mapping: dict[str, str]):
    """
    Factory function that creates a park type hook for acronym-based datasets.

    Instead of writing a separate named function for each province that uses
    acronyms, we call acronym_map() with the relevant dictionary and it returns
    a ready-to-use hook function. This pattern is called a "closure" — the
    inner _hook function "closes over" the mapping variable so it can use it
    even after acronym_map() has returned.

    Args:
        mapping: A dict of ACRONYM → "Full Name" pairs (keys are matched
                 case-insensitively via .upper()).

    Returns:
        A hook function with the signature (props, raw) -> str | None.

    Example:
        acronym_map({"PP": "Provincial Park"})
        # Returns a function that turns "PP" → "Provincial Park"
    """
    def _hook(props: dict, raw: str | None) -> str | None:
        """Expand a raw acronym using the mapping, or return it unchanged."""
        if raw is None:
            return None
        # dict.get(key, default) returns the default if the key isn't in the dict,
        # so unknown acronyms are returned as-is rather than becoming None.
        return mapping.get(raw.upper(), raw)

    return _hook  # We return the function itself, not the result of calling it.


def _ontario_park_type(props: dict, raw: str | None) -> str | None:
    """
    Resolve the park type for Ontario features.

    Ontario stores two relevant fields:
      - TYPE_ENG:                    "Provincial Park" or "Protected Area - Far North"
      - PROVINCIAL_PARK_CLASS_ENG:   "Recreational", "Wilderness", "Unclassified", etc.

    The desired output is:
      - "Provincial Park - Recreational"  (when class is set and not "Unclassified")
      - "Provincial Park"                 (when class is "Unclassified" or blank)
      - "Protected Area - Far North"      (verbatim from TYPE_ENG)

    This function is registered as a hook in PARK_TYPE_HOOKS so it runs
    automatically for any file whose name contains "ontario".

    Args:
        props: The full GeoJSON feature properties dict for this park.
        raw:   The type string already extracted by the field map lookup
               (will be the value of TYPE_ENG for Ontario files).

    Returns:
        The resolved park type string, or None if no type could be determined.
    """
    # Use TYPE_ENG directly; fall back to whatever raw was extracted generically.
    # The "or" chain short-circuits — it stops at the first truthy value.
    base = (props.get("TYPE_ENG") or raw or "").strip()

    if not base:
        return None  # No type information at all — leave it unset.

    if base == "Provincial Park":
        cls = (props.get("PROVINCIAL_PARK_CLASS_ENG") or "").strip()
        # Only append the class if it's meaningful — "Unclassified" is not useful.
        if cls and cls.lower() != "unclassified":
            # f-strings (f"...") let you embed variables directly in a string.
            return f"Provincial Park - {cls}"

    # For "Protected Area - Far North" (class will be "Unclassified"),
    # and for Provincial Parks with no meaningful class, return base as-is.
    return base


# PARK_TYPE_HOOKS: the central registry of per-source type resolution logic.
# Key:   a lowercase substring of the GeoJSON filename stem.
# Value: a callable (props, raw) -> str | None.
#
# The hook for a file is found by checking whether each key is a substring of
# the filename — the first match wins. Order matters if two keys could both
# match the same filename (e.g. "new" and "new_brunswick"), so be specific.
PARK_TYPE_HOOKS: dict[str, callable] = {
    "national_parks": acronym_map({
        "NP":  "National Park"
    }),
    "alberta": acronym_map({
        "ER":  "Ecological Reserve",
        "HR":  "Heritage Rangeland",
        "NA":  "Natural Area",
        "NP":  "National Park",
        "PP":  "Provincial Park",
        "PRA": "Provincial Recreation Area",
        "WA":  "Wilderness Area",
        "WP":  "Wilderness Park",
        "WPP": "Wildland",
    }),
    "saskatchewan": acronym_map({
        "RS":   "Recreation Site",
        "PA":   "Protected Area",
        "HS":   "Historic Site",
        "PP-H": "Provincial Park",
        "PP-W": "Provincial Park",
        "PP-N": "Provincial Park",
    }),
    "ontario": _ontario_park_type,  # Custom function — handles the composite logic.
}


# =============================================================================
# PARK BOUNDARY LOADING
# =============================================================================

def load_parks_from_file(path: Path, id_offset: int) -> list[dict]:
    """
    Load all park features from a single GeoJSON file and return them as a
    list of normalised park dicts ready for the API to serve.

    Each returned dict contains:
        id, name, source, park_type, area_ha, province, geometry, properties

    The id_offset ensures IDs are unique across all files — each file's parks
    are numbered starting from (id_offset) rather than from 0.

    Field extraction priority for name, type, and area:
        1. Field map (from Field_Names_By_Province.xlsx) — most specific.
        2. Generic fallback chain — used for unrecognised files.

    After extraction, a province-specific hook may further transform the type
    (e.g. expand acronyms, combine fields).

    Args:
        path:      Path to the .geojson file.
        id_offset: Integer to add to each feature's index to produce a unique ID.

    Returns:
        A list of park dicts, one per GeoJSON feature. Empty list on error.
    """
    # Open and parse the GeoJSON file. The 'with' statement ensures the file
    # is closed automatically even if an exception occurs.
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)  # json.load() parses the file into a Python dict/list.
    except Exception as e:
        # Catch any error (file not found, invalid JSON, etc.) and warn without crashing.
        print(f"  WARNING: Could not read {path.name}: {e}")
        return []

    # GeoJSON can be structured in several ways. We normalise all of them into
    # a flat list of feature dicts so the rest of the function is uniform.
    if data.get("type") == "FeatureCollection":
        features = data.get("features", [])   # Standard: a collection of features.
    elif data.get("type") == "Feature":
        features = [data]                      # A single feature — wrap it in a list.
    elif isinstance(data, list):
        features = data                        # A bare list of features (non-standard).
    else:
        print(f"  WARNING: Unrecognised GeoJSON structure in {path.name}")
        return []

    # Compute the lowercase stem once here — we reuse it many times in the loop.
    stem_lower = path.stem.lower()  # e.g. "alberta_provincial_parks"

    # Look up the field spec for this file. spec will be None for unrecognised files.
    spec = _get_source_fields(stem_lower)

    parks = []
    for i, feat in enumerate(features):
        # Skip anything that isn't a dict — malformed features do appear in real data.
        if not isinstance(feat, dict):
            continue

        # GeoJSON features have two main parts: "properties" (attributes) and
        # "geometry" (the shape). We default to empty dicts if either is missing.
        props = feat.get("properties") or {}
        geom  = feat.get("geometry")   or {}

        # ── Name ──────────────────────────────────────────────────────────────
        if spec and spec["name"]:
            # We know exactly which field to read for this file.
            # Fall back to a numbered placeholder if the field is empty.
            name = _first(props, spec["name"]) or f"Park {id_offset + i + 1}"
        else:
            # No field map entry — try a long list of common field names used
            # across different provinces and data sources.
            name = (
                props.get("name") or
                props.get("NAME") or
                props.get("park_name") or
                props.get("PARK_NAME") or
                props.get("PARKNM") or
                props.get("NAME_E") or
                f"Park {id_offset + i + 1}"  # Last resort: a numbered placeholder.
            )

        # ── Park type (raw value) ──────────────────────────────────────────────
        if spec and spec["type"]:
            park_type = _first(props, spec["type"])
        else:
            # Generic fallback — tries many common field names in order.
            park_type = (
                props.get("PARK_TYPE") or
                props.get("park_type") or
                props.get("PARKTYPE") or
                props.get("TYPE") or
                props.get("type") or
                props.get("TYPE_E") or
                props.get("TYPE_ENG") or
                None
            )

        # str.strip() removes leading/trailing whitespace — raw data often has extra spaces.
        if park_type:
            park_type = str(park_type).strip()

        # ── Park type hook ─────────────────────────────────────────────────────
        # Look for a registered hook whose key is a substring of this filename.
        # next(..., None) returns None if no hook matches — that's fine, we just skip it.
        hook = next((fn for key, fn in PARK_TYPE_HOOKS.items() if key in stem_lower), None)
        if hook:
            # The hook may expand an acronym, combine fields, or do nothing.
            park_type = hook(props, park_type)

        # ── Case normalisation ─────────────────────────────────────────────────
        # Some sources store everything in ALL-CAPS. Convert them to readable case.
        # any() returns True if at least one element in the iterable is truthy.
        if any(key in stem_lower for key in NAME_CASE_SOURCES):
            name = to_sentence_case(str(name).strip())
        if any(key in stem_lower for key in TYPE_CASE_SOURCES):
            if park_type:
                park_type = to_sentence_case(park_type)

        # ── Area ──────────────────────────────────────────────────────────────
        if spec and spec["area"]:
            area_ha = _first(props, spec["area"])
        else:
            area_ha = (
                props.get("area_ha") or
                props.get("HA_GIS") or
                props.get("AREA_HA") or
                props.get("Shape_Area")
            )

        # The filename stem (without extension) is used as the "source" identifier
        # in the UI, e.g. "alberta_provincial_parks".
        source   = path.stem

        # Province is an optional field that may not exist in many files.
        province = (
            props.get("province") or props.get("PROVINCE") or
            props.get("prov")     or props.get("PROV")     or ""
        )

        parks.append({
            "id":        id_offset + i,   # Globally unique integer ID for this park.
            "name":      str(name).strip(),
            "source":    source,
            "park_type": park_type,
            # Convert area to a float rounded to 1 decimal place.
            # The .replace(",", "") handles values like "1,234.5" (comma-formatted numbers).
            # The conditional expression (x if condition else y) only runs the conversion
            # when area_ha is truthy, avoiding errors on None/empty values.
            "area_ha":   round(float(str(area_ha).replace(",", "")), 1) if area_ha else None,
            "province":  province,
            "geometry":  geom,
            # Store ALL original properties so the detail view can show them.
            # Dict comprehension {k: v for k, v in ...} builds a new dict from pairs.
            "properties": {k: v for k, v in props.items()},
        })

    return parks


def load_all_park_boundaries() -> list[dict]:
    """
    Discover and load all .geojson files in PARKS_DIR.

    Each file is loaded by load_parks_from_file(), and the results are
    combined into a single flat list. The id_offset for each file is set to
    the current length of all_parks so that IDs don't overlap across files.

    Returns:
        A flat list of all park dicts from all files, or an empty list if the
        directory doesn't exist or contains no .geojson files.
    """
    if not PARKS_DIR.exists():
        return []

    # sorted() ensures files are always processed in the same order,
    # which keeps IDs stable across runs (assuming the file set doesn't change).
    geojson_files = sorted(PARKS_DIR.glob("*.geojson"))

    if not geojson_files:
        print(f"WARNING: No .geojson files found in {PARKS_DIR}")
        return []

    all_parks = []
    for path in geojson_files:
        # Pass the current list length as id_offset so each file's IDs
        # start where the previous file's IDs left off.
        parks = load_parks_from_file(path, id_offset=len(all_parks))
        print(f"  {path.name}: {len(parks)} features")
        # list.extend() appends all items from another list, unlike append()
        # which would add the whole list as a single nested element.
        all_parks.extend(parks)

    return all_parks


# Load all parks once at startup and keep them in memory. Flask is single-
# threaded by default, and the data doesn't change while the server is running,
# so this is safe and much faster than re-reading files on every request.
print(f"\nLoading park boundaries from: {PARKS_DIR}")
ALL_PARK_BOUNDARIES = load_all_park_boundaries()
print(f"Total: {len(ALL_PARK_BOUNDARIES)} park boundaries loaded\n")


# =============================================================================
# ROUTES
# =============================================================================
# Flask uses the @app.route() decorator to map URL paths to Python functions.
# When a browser requests that URL, Flask calls the function and returns its
# result as an HTTP response.  jsonify() converts a Python dict/list to JSON.

@app.route("/")
def index():
    """Serve the single-page HTML application."""
    # render_template_string() fills in any {{ variables }} in the HTML string
    # and returns the completed HTML. Since our template has no variables, it
    # just returns the string as-is.
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/boundaries")
def api_boundaries():
    """
    Return a JSON list of all park boundary summaries (no geometry).

    Geometry is deliberately excluded here because it can be very large.
    The browser fetches geometry separately via /api/boundary/<id> only
    when a specific park is selected.
    """
    return jsonify([
        {
            "id":        p["id"],
            "name":      p["name"],
            "source":    p["source"],
            "park_type": p["park_type"],
            "area_ha":   p["area_ha"],
            "province":  p["province"],
        }
        for p in ALL_PARK_BOUNDARIES  # List comprehension — builds the list in one expression.
    ])


@app.route("/api/boundary/<int:park_id>")
def api_boundary(park_id):
    """
    Return the full data for a single park, including its geometry.

    The <int:park_id> part of the route is a URL parameter — Flask extracts
    it from the URL and passes it as the park_id argument. The 'int:' prefix
    tells Flask to only match integer values and convert automatically.

    Args:
        park_id: The integer ID of the park to fetch.
    """
    # next() with a generator and a default is an efficient way to search a list
    # for the first item matching a condition, without loading everything into memory.
    park = next((p for p in ALL_PARK_BOUNDARIES if p["id"] == park_id), None)

    if park is None:
        # HTTP 404 means "not found". jsonify() + a second return value sets the
        # status code. Flask defaults to 200 (OK) when no code is provided.
        return jsonify({"error": "Park not found"}), 404

    return jsonify(park)


@app.route("/api/boundary-sources")
def api_boundary_sources():
    """
    Return a sorted list of unique source file stems.

    Used to populate the "Filter by source" dropdown in the sidebar.
    A set comprehension {expr for item in iterable} builds a set of unique
    values, then sorted() converts it to a sorted list.
    """
    seen = sorted({p["source"] for p in ALL_PARK_BOUNDARIES if p["source"]})
    return jsonify(seen)


@app.route("/api/boundary-types")
def api_boundary_types():
    """
    Return a sorted list of unique park type strings.

    Used as a fallback — the frontend currently builds this list client-side
    from the /api/boundaries data so it can filter by source dynamically.
    This endpoint is retained for convenience and potential future use.
    """
    seen = sorted({p["park_type"] for p in ALL_PARK_BOUNDARIES if p["park_type"]})
    return jsonify(seen)


# =============================================================================
# HTML TEMPLATE
# =============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Park Boundary Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  :root {
    --bg:        #141720;
    --surface:   #1e2130;
    --surface2:  #252a3a;
    --border:    #2e3450;
    --border2:   #3a4060;
    --text:      #c8ccd8;
    --text-dim:  #606880;
    --text-mid:  #8890a8;
    --accent:    #50c878;
    --sidebar-w: 340px;
  }
  body {
    font-family: 'Space Mono', monospace;
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    overflow: hidden;
  }

  /* ── Sidebar ── */
  #sidebar {
    width: var(--sidebar-w);
    height: 100vh;
    background: var(--surface);
    border-right: 1px solid var(--border);
    float: left;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* ── Panel header ── */
  .panel-header {
    padding: 12px 14px 10px;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }
  .panel-header-top {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
  }
  .panel-header-top h2 {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
    flex: 1;
    color: var(--accent);
  }
  #park-count { font-size: 10px; color: var(--text-dim); letter-spacing: 0.3px; }
  #loading-wrap { margin-top: 4px; }
  #progress-bar-bg { width: 100%; height: 3px; background: var(--border); border-radius: 2px; overflow: hidden; }
  #progress-bar-fill { height: 100%; width: 0%; background: var(--accent); border-radius: 2px; transition: width 0.2s ease; }
  #loading-label { font-size: 9px; color: var(--text-dim); margin-bottom: 4px; letter-spacing: 0.5px; text-transform: uppercase; }

  /* ── Selection bar ── */
  .selection-bar {
    display: none;
    align-items: center;
    padding: 5px 12px;
    font-size: 10px;
    flex-shrink: 0;
    gap: 5px;
    background: rgba(80,200,120,0.07);
    border-bottom: 1px solid rgba(80,200,120,0.2);
    color: var(--accent);
  }
  .selection-bar.visible { display: flex; }
  .sel-count { flex: 1; }
  .selection-bar button {
    background: none; border: 1px solid currentColor; border-radius: 4px;
    color: inherit; font-family: 'Space Mono', monospace;
    font-size: 9px; padding: 3px 7px; cursor: pointer; white-space: nowrap;
    transition: background 0.15s;
  }
  .selection-bar button:hover { background: rgba(255,255,255,0.08); }
  .btn-clear-sel { color: var(--text-dim) !important; border-color: var(--text-dim) !important; }

  /* ── Search ── */
  .search-wrap { padding: 10px 12px 6px; flex-shrink: 0; }
  .search-input {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border2);
    border-radius: 5px;
    color: var(--text);
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    padding: 7px 11px;
    outline: none;
    transition: border-color 0.15s;
  }
  .search-input:focus { border-color: var(--accent); }
  .search-input::placeholder { color: var(--text-dim); }

  /* ── Section labels ── */
  .ctrl-section-label {
    font-size: 8px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase;
    color: var(--text-dim); padding: 5px 14px 2px; flex-shrink: 0;
    border-top: 1px solid var(--border); margin-top: 2px;
  }

  /* ── Sort bar ── */
  #park-sort-bar { display: flex; gap: 5px; padding: 4px 12px; flex-shrink: 0; }
  .sort-btn {
    font-family: 'Space Mono', monospace; font-size: 9px; letter-spacing: 0.5px;
    padding: 4px 9px; border: 1px solid var(--border2); border-radius: 4px;
    background: transparent; color: var(--text-dim); cursor: pointer;
    transition: all 0.15s; text-transform: uppercase; flex: 1; white-space: nowrap;
  }
  .sort-btn:hover { border-color: var(--accent); color: var(--text); }
  .sort-btn.active { background: var(--accent); color: var(--bg); border-color: var(--accent); font-weight: 700; }

  /* ── Filter rows ── */
  #park-filter-row {
    display: flex; gap: 5px; padding: 4px 12px 6px; flex-shrink: 0;
    flex-wrap: wrap;
  }
  #park-filter-row > .filter-select { flex: 1; min-width: calc(50% - 3px); }
  .filter-select {
    width: 100%; background: var(--bg); border: 1px solid var(--border2); border-radius: 5px;
    color: var(--text); font-family: 'Space Mono', monospace; font-size: 10px;
    padding: 6px 24px 6px 8px; outline: none; cursor: pointer;
    transition: border-color 0.15s; appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%235a6478'/%3E%3C/svg%3E");
    background-repeat: no-repeat; background-position: right 8px center;
  }
  .filter-select:focus { border-color: var(--accent); }
  .filter-select option { background: var(--surface); }

  /* ── Select All button ── */
  #btn-select-all-parks {
    font-family: 'Space Mono', monospace; font-size: 9px; letter-spacing: 0.5px;
    padding: 6px 10px; border: 1px solid var(--accent); border-radius: 5px;
    background: transparent; color: var(--accent); cursor: pointer;
    transition: all 0.15s; text-transform: uppercase; white-space: nowrap;
    flex-shrink: 0; align-self: flex-start;
  }
  #btn-select-all-parks:hover { background: rgba(80,200,120,0.12); }
  #btn-select-all-parks.active { background: var(--accent); color: var(--bg); font-weight: 700; }

  /* ── Park list ── */
  #park-list { flex: 1; overflow-y: auto; padding: 6px; }
  .park-item {
    background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    padding: 10px 12px; margin-bottom: 5px; cursor: pointer;
    transition: background 0.12s; font-size: 11px; position: relative;
  }
  .park-item:hover { background: var(--surface2); }
  .park-item.active { background: rgba(80,200,120,0.05); padding-left: 15px; }
  .park-item.active::before {
    content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 3px;
    border-radius: 6px 0 0 6px; background: var(--park-color, var(--accent));
  }
  .park-item .park-name {
    font-weight: 700; color: var(--text); margin-bottom: 4px;
    display: flex; align-items: center; justify-content: space-between;
  }
  .source-badge {
    font-size: 9px; letter-spacing: 0.5px; padding: 2px 6px; border-radius: 3px;
    text-transform: uppercase; font-weight: 700;
    background: rgba(80,200,120,0.18); color: var(--accent); flex-shrink: 0;
    margin-left: 6px; max-width: 110px; overflow: hidden;
    text-overflow: ellipsis; white-space: nowrap;
  }
  .type-badge {
    font-size: 9px; letter-spacing: 0.4px; padding: 1px 5px; border-radius: 3px;
    background: rgba(0,153,255,0.14); color: #4db8ff;
    border: 1px solid rgba(0,153,255,0.22); white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; max-width: 200px;
    display: inline-block; margin-top: 3px;
  }
  .park-item .park-meta { font-size: 10px; color: var(--text-mid); display: flex; gap: 10px; margin-top: 4px; flex-wrap: wrap; }

  /* ── Map ── */
  #map-container { margin-left: var(--sidebar-w); height: 100vh; position: relative; }
  #map { width: 100%; height: 100%; }

  /* ── Leaflet overrides ── */
  .leaflet-popup-content-wrapper {
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border2); border-radius: 6px;
    font-family: 'Space Mono', monospace; font-size: 11px;
  }
  .leaflet-popup-content { margin: 10px 12px; line-height: 1.6; }
  .leaflet-popup-tip { background: var(--surface); }
  .leaflet-popup-content strong { color: var(--accent); }
  .leaflet-control-zoom a {
    background: var(--surface) !important;
    color: var(--text) !important;
    border-color: var(--border2) !important;
  }
  .leaflet-control-zoom a:hover { background: var(--surface2) !important; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 5px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 3px; }
</style>
</head>
<body>

<div id="sidebar">

  <div class="panel-header">
    <div class="panel-header-top">
      <h2>🌲 Park Boundaries</h2>
    </div>
    <div id="park-count"></div>
    <div id="loading-wrap">
      <div id="loading-label">Loading…</div>
      <div id="progress-bar-bg"><div id="progress-bar-fill"></div></div>
    </div>
  </div>

  <div class="selection-bar" id="park-sel-bar">
    <span class="sel-count" id="park-sel-count"></span>
    <button id="btn-fit-parks">Fit map</button>
    <button class="btn-clear-sel" id="btn-clear-parks">Clear</button>
  </div>

  <div class="search-wrap">
    <input type="text" id="park-search" class="search-input" placeholder="Search parks…" />
  </div>

  <div class="ctrl-section-label">Sort by</div>
  <div id="park-sort-bar">
    <button class="sort-btn active" data-psort="name">Name</button>
    <button class="sort-btn" data-psort="area">Area</button>
    <button class="sort-btn" data-psort="type">Type</button>
    <button class="sort-btn" data-psort="source">Source</button>
  </div>

  <div class="ctrl-section-label">Filter by</div>
  <div id="park-filter-row">
    <select id="park-source-filter" class="filter-select">
      <option value="">🌲 All files</option>
    </select>
    <select id="park-type-filter" class="filter-select">
      <option value="">🏷 All types</option>
    </select>
    <button id="btn-select-all-parks" title="Select / deselect all visible parks">Select All</button>
  </div>

  <div id="park-list"></div>

</div>

<div id="map-container">
  <div id="map"></div>
</div>

<script>
// ═══════════════════════════════════════════════════════════════════
// MAP SETUP
// ═══════════════════════════════════════════════════════════════════
const map = L.map('map', { zoomControl: true }).setView([57.0, -96.0], 4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© OpenStreetMap',
  maxZoom: 19,
}).addTo(map);

// ═══════════════════════════════════════════════════════════════════
// COLOUR PALETTE
// ═══════════════════════════════════════════════════════════════════
const PARK_COLOURS = [
  '#50c878','#00bfff','#dda0dd','#f4a460','#87cefa',
  '#98fb98','#ffa07a','#b0e0e6','#f0e68c','#afeeee',
];
let parkColourIdx = 0;

// ═══════════════════════════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════════════════════════
let allBoundaries      = [];
let filteredBoundaries = [];
let currentParkSort    = 'name';
const selectedParks    = new Map(); // id -> { colour, layers, parkData }
const boundaryGeomCache = {};       // id -> full boundary data with geometry

// ═══════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════
function fmtArea(ha) {
  if (ha == null) return null;
  if (ha >= 10000) return (ha / 10000).toFixed(1) + ' Mha';
  if (ha >= 1000)  return (ha / 1000).toFixed(1)  + ' kha';
  return ha.toFixed(1) + ' ha';
}

// ═══════════════════════════════════════════════════════════════════
// SELECTION BAR
// ═══════════════════════════════════════════════════════════════════
function updateParkSelBar() {
  const bar = document.getElementById('park-sel-bar');
  const n = selectedParks.size;
  if (n === 0) { bar.classList.remove('visible'); }
  else {
    bar.classList.add('visible');
    document.getElementById('park-sel-count').textContent =
      n + ' park' + (n !== 1 ? 's' : '') + ' selected';
  }
}

document.getElementById('btn-fit-parks').onclick  = fitParks;
document.getElementById('btn-clear-parks').onclick = clearParks;

// ═══════════════════════════════════════════════════════════════════
// FIT / CLEAR
// ═══════════════════════════════════════════════════════════════════
function layerBounds(layers) {
  const pts = [];
  layers.forEach(l => {
    if (l.getLatLngs) l.getLatLngs().flat(Infinity).forEach(ll => pts.push(ll));
    else if (l.getLatLng) pts.push(l.getLatLng());
  });
  return pts;
}

function fitParks() {
  const pts = [];
  selectedParks.forEach(({ layers }) => pts.push(...layerBounds(layers)));
  if (pts.length) map.fitBounds(L.latLngBounds(pts), { padding: [40, 40] });
}

function clearParks() {
  selectedParks.forEach(({ layers }) => layers.forEach(l => map.removeLayer(l)));
  selectedParks.clear();
  parkColourIdx = 0;
  document.querySelectorAll('.park-item.active').forEach(el => {
    el.classList.remove('active');
    el.style.removeProperty('--park-color');
  });
  document.getElementById('btn-select-all-parks').classList.remove('active');
  updateParkSelBar();
}

// ═══════════════════════════════════════════════════════════════════
// SELECT ALL PARKS (filtered list)
// ═══════════════════════════════════════════════════════════════════
async function selectAllParks() {
  const btn = document.getElementById('btn-select-all-parks');
  if (filteredBoundaries.length === 0) return;
  const allSelected = filteredBoundaries.every(p => selectedParks.has(p.id));
  if (allSelected) {
    filteredBoundaries.forEach(p => {
      if (!selectedParks.has(p.id)) return;
      selectedParks.get(p.id).layers.forEach(l => map.removeLayer(l));
      selectedParks.delete(p.id);
      const item = document.querySelector('.park-item[data-id="' + p.id + '"]');
      if (item) { item.classList.remove('active'); item.style.removeProperty('--park-color'); }
    });
    btn.classList.remove('active');
    updateParkSelBar();
    return;
  }
  btn.classList.add('active');
  const toSelect = filteredBoundaries.filter(p => !selectedParks.has(p.id));
  await Promise.all(toSelect.map(p => togglePark(p.id)));
}

document.getElementById('btn-select-all-parks').onclick = selectAllParks;

// ═══════════════════════════════════════════════════════════════════
// GEOMETRY RENDERING
// ═══════════════════════════════════════════════════════════════════
function geomToLeafletLayers(geom, colour) {
  const layers = [];
  if (!geom || !geom.type) return layers;
  const style = { color: colour, weight: 2, opacity: 0.9, fillColor: colour, fillOpacity: 0.15 };

  function addPolygon(rings) {
    const latlngs = rings.map(ring => ring.map(([lon, lat]) => [lat, lon]));
    layers.push(L.polygon(latlngs, style));
  }
  if (geom.type === 'Polygon')           addPolygon(geom.coordinates);
  else if (geom.type === 'MultiPolygon') geom.coordinates.forEach(poly => addPolygon(poly));
  else if (geom.type === 'Point') {
    const [lon, lat] = geom.coordinates;
    layers.push(L.circleMarker([lat, lon], { radius: 8, color: colour, fillColor: colour, fillOpacity: 0.6, weight: 2 }));
  } else if (geom.type === 'MultiPoint') {
    geom.coordinates.forEach(([lon, lat]) =>
      layers.push(L.circleMarker([lat, lon], { radius: 6, color: colour, fillColor: colour, fillOpacity: 0.6, weight: 2 })));
  } else if (geom.type === 'LineString') {
    layers.push(L.polyline(geom.coordinates.map(([lon, lat]) => [lat, lon]), { color: colour, weight: 3, opacity: 0.9 }));
  } else if (geom.type === 'MultiLineString') {
    geom.coordinates.forEach(line =>
      layers.push(L.polyline(line.map(([lon, lat]) => [lat, lon]), { color: colour, weight: 3, opacity: 0.9 })));
  }
  return layers;
}

// ═══════════════════════════════════════════════════════════════════
// TOGGLE PARK BOUNDARY
// ═══════════════════════════════════════════════════════════════════
async function togglePark(id) {
  if (selectedParks.has(id)) {
    selectedParks.get(id).layers.forEach(l => map.removeLayer(l));
    selectedParks.delete(id);
    const item = document.querySelector('.park-item[data-id="' + id + '"]');
    if (item) { item.classList.remove('active'); item.style.removeProperty('--park-color'); }
    const allSelected = filteredBoundaries.length > 0 && filteredBoundaries.every(p => selectedParks.has(p.id));
    document.getElementById('btn-select-all-parks').classList.toggle('active', allSelected);
    updateParkSelBar(); fitParks(); return;
  }
  const res  = await fetch('/api/boundary/' + id);
  const data = await res.json();
  if (data.error) { alert(data.error); return; }
  boundaryGeomCache[id] = data;
  const colour = PARK_COLOURS[parkColourIdx++ % PARK_COLOURS.length];
  const layers = geomToLeafletLayers(data.geometry, colour);
  if (layers.length === 0) { alert('No renderable geometry for: ' + data.name); return; }
  layers.forEach(l => {
    l.addTo(map);
    l.bindPopup(
      '<strong style="color:' + colour + '">' + data.name + '</strong>' +
      '<br>File: ' + data.source +
      (data.park_type ? '<br>Type: ' + data.park_type : '') +
      (data.province  ? '<br>Province: ' + data.province : '') +
      (data.area_ha   ? '<br>Area: ' + fmtArea(data.area_ha) : '')
    );
  });
  selectedParks.set(id, { colour, layers, parkData: data });
  const item = document.querySelector('.park-item[data-id="' + id + '"]');
  if (item) { item.classList.add('active'); item.style.setProperty('--park-color', colour); }
  fitParks(); updateParkSelBar();
}

// ═══════════════════════════════════════════════════════════════════
// RENDER PARK LIST
// ═══════════════════════════════════════════════════════════════════
function renderParkList() {
  const list = document.getElementById('park-list');
  const q    = document.getElementById('park-search').value.toLowerCase();
  const src  = document.getElementById('park-source-filter').value;
  const typ  = document.getElementById('park-type-filter').value;

  filteredBoundaries = allBoundaries.filter(p => {
    if (src && p.source !== src) return false;
    if (typ && p.park_type !== typ) return false;
    if (q && !(p.name || '').toLowerCase().includes(q) &&
             !(p.source    || '').toLowerCase().includes(q) &&
             !(p.park_type || '').toLowerCase().includes(q) &&
             !(p.province  || '').toLowerCase().includes(q)) return false;
    return true;
  });

  if      (currentParkSort === 'area')   filteredBoundaries.sort((a, b) => (b.area_ha || 0) - (a.area_ha || 0));
  else if (currentParkSort === 'source') filteredBoundaries.sort((a, b) => (a.source || '').localeCompare(b.source || '') || (a.name || '').localeCompare(b.name || ''));
  else if (currentParkSort === 'type')   filteredBoundaries.sort((a, b) => (a.park_type || '').localeCompare(b.park_type || '') || (a.name || '').localeCompare(b.name || ''));
  else                                   filteredBoundaries.sort((a, b) => (a.name || '').localeCompare(b.name || ''));

  list.innerHTML = '';
  if (!filteredBoundaries.length) {
    list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-dim)">No parks found</div>';
    return;
  }

  filteredBoundaries.forEach(p => {
    const div = document.createElement('div');
    div.className = 'park-item' + (selectedParks.has(p.id) ? ' active' : '');
    div.dataset.id = p.id;
    if (selectedParks.has(p.id)) div.style.setProperty('--park-color', selectedParks.get(p.id).colour);

    const badge     = p.source    ? '<span class="source-badge">' + p.source + '</span>' : '';
    const typeBadge = p.park_type ? '<span class="type-badge">🏷 ' + p.park_type + '</span>' : '';
    const meta = [];
    if (p.area_ha  != null) meta.push('📐 ' + fmtArea(p.area_ha));
    if (p.province)          meta.push('📍 ' + p.province);

    div.innerHTML =
      '<div class="park-name">' + (p.name || 'Unnamed') + badge + '</div>' +
      (typeBadge ? '<div>' + typeBadge + '</div>' : '') +
      (meta.length ? '<div class="park-meta">' + meta.join('') + '</div>' : '');
    div.onclick = () => togglePark(p.id);
    list.appendChild(div);
  });
}

// ═══════════════════════════════════════════════════════════════════
// WIRE UP CONTROLS
// ═══════════════════════════════════════════════════════════════════

// Repopulate the type dropdown to only show types present in the
// currently selected source file (or all types when no source is selected).
function updateTypeFilter() {
  const src = document.getElementById('park-source-filter').value;
  const sel = document.getElementById('park-type-filter');
  const prev = sel.value; // preserve selection if still valid

  const types = [...new Set(
    allBoundaries
      .filter(p => !src || p.source === src)
      .map(p => p.park_type)
      .filter(Boolean)
  )].sort();

  sel.innerHTML = '<option value="">🏷 All types</option>';
  types.forEach(t => {
    const opt = document.createElement('option');
    opt.value = t; opt.textContent = t;
    sel.appendChild(opt);
  });

  // Restore previous selection only if it still exists in the new list
  sel.value = types.includes(prev) ? prev : '';
}

document.querySelectorAll('.sort-btn[data-psort]').forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll('.sort-btn[data-psort]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active'); currentParkSort = btn.dataset.psort; renderParkList();
  };
});
document.getElementById('park-search').addEventListener('input', renderParkList);
document.getElementById('park-source-filter').addEventListener('change', () => {
  updateTypeFilter();
  renderParkList();
});
document.getElementById('park-type-filter').addEventListener('change', renderParkList);

// ═══════════════════════════════════════════════════════════════════
// BOOTSTRAP
// ═══════════════════════════════════════════════════════════════════
async function init() {
  const fill  = document.getElementById('progress-bar-fill');
  const label = document.getElementById('loading-label');
  fill.style.width = '20%';

  const [boundariesRes, sourcesRes] = await Promise.all([
    fetch('/api/boundaries'),
    fetch('/api/boundary-sources'),
  ]);
  fill.style.width = '70%'; label.textContent = 'Parsing data…';

  allBoundaries = await boundariesRes.json();
  const sources = await sourcesRes.json();

  fill.style.width = '90%'; label.textContent = 'Rendering…';
  await new Promise(r => setTimeout(r, 30));

  // Populate source filter
  sources.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s;
    document.getElementById('park-source-filter').appendChild(opt);
  });

  // Populate park type filter (all types, no source restriction yet)
  updateTypeFilter();

  renderParkList();

  fill.style.width = '100%';
  await new Promise(r => setTimeout(r, 250));
  document.getElementById('loading-wrap').style.display = 'none';

  const countEl = document.getElementById('park-count');
  countEl.textContent =
    allBoundaries.length + ' park' + (allBoundaries.length !== 1 ? 's' : '') +
    (types.length ? ' · ' + types.length + ' type' + (types.length !== 1 ? 's' : '') : '');
}

init();
</script>
</body>
</html>
"""


# =============================================================================
# MAIN
# =============================================================================
# The `if __name__ == "__main__":` guard ensures this block only runs when the
# script is executed directly (e.g. `python view_park_boundaries.py`), not when
# it's imported as a module by another script.  This is a Python convention.

if __name__ == "__main__":
    import argparse

    # argparse lets users override defaults from the command line, e.g.:
    #   python view_park_boundaries.py --port 8080
    parser = argparse.ArgumentParser(description="Park Boundary Viewer")
    parser.add_argument("--host", default=HOST, help=f"Host to bind (default: {HOST})")
    parser.add_argument("--port", type=int, default=PORT, help=f"Port to bind (default: {PORT})")
    args = parser.parse_args()

    # Allow command-line arguments to override the config values.
    HOST = args.host
    PORT = args.port

    print()
    print("  Park Boundary Viewer")
    print("  " + "-" * 40)
    print(f"  Parks dir  : {PARKS_DIR}")
    print(f"  Boundaries : {len(ALL_PARK_BOUNDARIES)} loaded")
    print(f"  Browser    : http://{HOST}:{PORT}")
    print("  Stop       : Ctrl+C")
    print()

    # debug=False is important for anything beyond local development — debug
    # mode exposes an interactive Python console in the browser which is a
    # serious security risk if the server is accessible to other people.
    app.run(host=HOST, port=PORT, debug=False)
