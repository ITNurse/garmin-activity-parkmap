"""
config.py
---------
Central configuration for the Garmin Geo Data project.

All scripts import from here so that path changes only need to be made
in one place. To relocate the project, update PROJECT_ROOT below.

Usage in scripts:
    import sys
    from pathlib import Path
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # from src/<folder>/
    sys.path.insert(0, str(PROJECT_ROOT))
    import config
"""

from pathlib import Path
from typing import Optional

# =============================================================================
# PROJECT ROOT
# All other paths are derived from this. Change this if you move the project.
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.resolve()

# =============================================================================
# INPUT DATA — RAW SOURCE FILES
# =============================================================================

RAW_DATA = PROJECT_ROOT/ "data" / "raw"
FIT_DIR = RAW_DATA / "fit_files"
STRAVA_DIR = RAW_DATA / "strava"
RAW_PARK_DIR = RAW_DATA / "parks"
RAW_PARKS_GEOJSON = RAW_PARK_DIR / "geojson_files"
RAW_PARKS_WKT = RAW_PARK_DIR / "wkt_files"
RAW_PARKS_SHAPEFILES = RAW_PARK_DIR / "shapefiles"


# =============================================================================
# PROCESSED DATA  (gitignored — derived from personal activity files)
# =============================================================================

PROCESSED_DATA = PROJECT_ROOT / "data" / "processed"

# .track.json files — one per GPS activity
TRACKS_DIR = PROCESSED_DATA / "tracks"
PARKS_DIR = PROCESSED_DATA / "parks"

# Processed parks files
PARKS_CUSTOM = PARKS_DIR / "custom_parks.geojson"
PARKS_NATIONAL = PARKS_DIR / "nb_national_parks.geojson"
PARKS_PARKS = PARKS_DIR / "nb_provincial_parks.geojson"

# Corrections audit log
CORRECTIONS_LOG = PROCESSED_DATA / "corrections_log.csv"

# =============================================================================
# OUTPUTS  (gitignored)
# =============================================================================

DATA_OUTPUTS = PROJECT_ROOT / "data" / "outputs"

# Profile workbook from src/profiling/01_profile_fit_files.py
PROFILE_OUTPUT_DIR = DATA_OUTPUTS

# =============================================================================
# RUNTIME DEFAULTS
# =============================================================================

# Park matching: fraction of GPS points that must fall inside a boundary
# for the activity to be considered a visit to that park.
# 0.20 = at least 20% of points inside.  Raise to reduce false positives.
PARK_MATCH_THRESHOLD = 0.20

# Web viewer (src/validation/view_tracks.py)
VIEWER_HOST = "127.0.0.1"   # localhost only — change to 0.0.0.0 to expose on network
VIEWER_PORT = 5000

# =============================================================================
# ENSURE DIRECTORIES EXIST AT IMPORT TIME
# Scripts can rely on these folders being present without creating them manually.
# =============================================================================

for _dir in (FIT_DIR, PARKS_DIR, PROCESSED_DATA, TRACKS_DIR, DATA_OUTPUTS):
    _dir.mkdir(parents=True, exist_ok=True)
