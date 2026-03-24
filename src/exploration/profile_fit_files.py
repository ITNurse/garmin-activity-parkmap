"""
01_profile_fit_files.py
=======================
Loops through every .fit file in the configured directory, profiles all
message types and fields, classifies each file by type, and writes two
worksheets to an Excel workbook:

    Sheet 1 — field_inventory
        All unique fields observed across all .fit files, with data type,
        example value, message type, and which file classifications contain them.

    Sheet 2 — gps_file_summary
        One row per .fit file that contains GPS (lat/lon) data, with key
        metadata fields for downstream processing.

Usage:
    python src/profiling/01_profile_fit_files.py

Output:
    data/outputs/fit_profile_YYYYMMDD_HHMMSS.xlsx
"""

import re
import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
from fitparse import FitFile, FitParseError
from tqdm import tqdm

# =============================================================================
# CONFIGURATION — paths pulled from config.py at project root
# =============================================================================
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import config
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FIT_DIR      = config.FIT_DIR
OUTPUT_DIR   = config.DATA_OUTPUTS

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# =============================================================================
# FILE TYPE CLASSIFICATION
# =============================================================================

def classify_fit_file(message_types: set, sport: str | None, has_gps: bool) -> str:
    """
    Infer a human-readable file classification from observed message types
    and sport field.

    Classification priority (first match wins):
        Activity/GPS        — session + record messages + GPS coordinates
        Activity/No GPS     — session + record messages, no GPS
        Monitoring          — monitoring or monitoring_info messages
        Course              — course or course_point messages
        Workout             — workout or workout_step messages
        Settings            — device_settings or user_profile messages
        Goals               — goal messages
        Unknown             — anything else
    """
    mt = message_types  # shorthand

    if "session" in mt and "record" in mt and has_gps:
        return "Activity/GPS"
    if "session" in mt and "record" in mt:
        return "Activity/No GPS"
    if mt & {"monitoring", "monitoring_info"}:
        return "Monitoring"
    if mt & {"course", "course_point"}:
        return "Course"
    if mt & {"workout", "workout_step"}:
        return "Workout"
    if mt & {"device_settings", "user_profile", "zones_target"}:
        return "Settings"
    if "goal" in mt:
        return "Goals"
    return "Unknown"

# =============================================================================
# UNIT HELPERS
# =============================================================================

SEMICIRCLE_TO_DEG = 180.0 / (2 ** 31)

def to_degrees(value) -> float | None:
    """Convert Garmin semicircle integer to decimal degrees."""
    if value is None:
        return None
    try:
        return round(float(value) * SEMICIRCLE_TO_DEG, 7)
    except (TypeError, ValueError):
        return None

def safe_str(value) -> str:
    """
    Return a clean string representation of any value, truncated to 80 chars.
    Strips control characters that openpyxl cannot write to Excel cells
    (any character with ASCII value < 32, except tab/newline/carriage return).
    """
    if value is None:
        return ""
    s = str(value)
    # Remove illegal XML/Excel control characters
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)
    return s[:80] + "…" if len(s) > 80 else s

# =============================================================================
# CORE PARSING
# =============================================================================

def parse_fit_file(fit_path: Path) -> dict:
    """
    Parse a single .fit file and return a structured result dict containing:
        - message_types : set of message type names observed
        - fields        : dict of {(message_type, field_name): {type, examples}}
        - gps_records   : list of dicts with lat, lon, timestamp (raw record msgs)
        - session_meta  : dict with start_time, end_time, sport, sub_sport
        - has_gps       : bool
        - error         : str or None
    """
    result = {
        "message_types": set(),
        "fields": defaultdict(lambda: {"type": None, "examples": []}),
        "gps_records": [],
        "session_meta": {"start_time": None, "end_time": None,
                         "sport": None, "sub_sport": None},
        "has_gps": False,
        "error": None,
    }

    try:
        fit = FitFile(str(fit_path))

        for message in fit.get_messages():
            msg_name = message.name
            result["message_types"].add(msg_name)

            field_dict = {}
            for field in message.fields:
                field_dict[field.name] = field.value

                key = (msg_name, field.name)
                entry = result["fields"][key]

                # Capture data type from Python type of value
                if entry["type"] is None and field.value is not None:
                    entry["type"] = type(field.value).__name__

                # Keep up to 3 distinct non-None example values
                if field.value is not None and len(entry["examples"]) < 3:
                    sv = safe_str(field.value)
                    if sv not in entry["examples"]:
                        entry["examples"].append(sv)

            # ---- Extract session metadata ----
            if msg_name == "session":
                meta = result["session_meta"]
                if meta["start_time"] is None:
                    meta["start_time"] = field_dict.get("start_time")
                if meta["end_time"] is None:
                    meta["end_time"] = field_dict.get("timestamp")
                if meta["sport"] is None:
                    meta["sport"] = safe_str(field_dict.get("sport"))


            # ---- Extract GPS record points ----
            if msg_name == "record":
                lat_raw = field_dict.get("position_lat")
                lon_raw = field_dict.get("position_long")
                ts      = field_dict.get("timestamp")
                lat     = to_degrees(lat_raw)
                lon     = to_degrees(lon_raw)
                if lat is not None and lon is not None:
                    result["gps_records"].append({
                        "lat": lat,
                        "lon": lon,
                        "timestamp": ts,
                    })

    except FitParseError as e:
        result["error"] = f"FitParseError: {e}"
    except Exception as e:
        result["error"] = f"Unexpected error: {e}"

    result["has_gps"] = len(result["gps_records"]) > 0
    return result

# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fit_files = sorted(FIT_DIR.glob("*.fit"))
    if not fit_files:
        log.error("No .fit files found in: %s", FIT_DIR)
        sys.exit(1)

    log.info("Found %d .fit files in %s", len(fit_files), FIT_DIR)

    # ------------------------------------------------------------------
    # Pass 1: Parse every file
    # ------------------------------------------------------------------
    all_fields   = defaultdict(lambda: {"type": None, "examples": [],
                                        "message_types": set(), "file_classes": set()})
    gps_rows     = []
    error_count  = 0

    for fit_path in tqdm(fit_files, desc="Profiling .fit files", unit="file"):
        result = parse_fit_file(fit_path)

        if result["error"]:
            log.warning("%-50s  ERROR: %s", fit_path.name, result["error"])
            error_count += 1
            continue

        file_class = classify_fit_file(
            result["message_types"],
            result["session_meta"]["sport"],
            result["has_gps"],
        )

        # ---- Accumulate field inventory ----
        for (msg_name, field_name), entry in result["fields"].items():
            key = (msg_name, field_name)
            inv = all_fields[key]
            inv["message_types"].add(msg_name)
            inv["file_classes"].add(file_class)
            if inv["type"] is None and entry["type"] is not None:
                inv["type"] = entry["type"]
            for ex in entry["examples"]:
                if ex not in inv["examples"] and len(inv["examples"]) < 3:
                    inv["examples"].append(ex)

        # ---- GPS file summary row ----
        if result["has_gps"]:
            meta    = result["session_meta"]
            records = result["gps_records"]

            row = {
                "filename":        fit_path.name,
                "file_class":      file_class,
                "start_time":      meta["start_time"],
                "end_time":        meta["end_time"],
                "sport":           meta["sport"],
                "gps_point_count": len(records),
            }

            # First GPS point
            if len(records) >= 1:
                row["first_lat"]       = records[0]["lat"]
                row["first_lon"]       = records[0]["lon"]
                row["first_timestamp"] = records[0]["timestamp"]
            else:
                row.update(first_lat=None, first_lon=None, first_timestamp=None)

            # Second GPS point
            if len(records) >= 2:
                row["second_lat"]       = records[1]["lat"]
                row["second_lon"]       = records[1]["lon"]
                row["second_timestamp"] = records[1]["timestamp"]
            else:
                row.update(second_lat=None, second_lon=None, second_timestamp=None)

            gps_rows.append(row)

    # ------------------------------------------------------------------
    # Build DataFrames
    # ------------------------------------------------------------------

    # Sheet 1 — Field Inventory
    inventory_rows = []
    for (msg_name, field_name), inv in sorted(all_fields.items()):
        inventory_rows.append({
            "message_type":    msg_name,
            "field_name":      field_name,
            "data_type":       inv["type"] or "unknown",
            "example_value_1": inv["examples"][0] if len(inv["examples"]) > 0 else "",
            "example_value_2": inv["examples"][1] if len(inv["examples"]) > 1 else "",
            "example_value_3": inv["examples"][2] if len(inv["examples"]) > 2 else "",
            "file_classes":    ", ".join(sorted(inv["file_classes"])),
        })

    df_inventory = pd.DataFrame(inventory_rows)

    # Sheet 2 — GPS File Summary
    df_gps = pd.DataFrame(gps_rows) if gps_rows else pd.DataFrame()

    # ------------------------------------------------------------------
    # Write Excel workbook
    # ------------------------------------------------------------------
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"fit_profile_{timestamp}.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

        df_inventory.to_excel(writer, sheet_name="field_inventory",
                              index=False, freeze_panes=(1, 0))

        if not df_gps.empty:
            df_gps.to_excel(writer, sheet_name="gps_file_summary",
                            index=False, freeze_panes=(1, 0))

            # Auto-size columns (best-effort)
            ws = writer.sheets["gps_file_summary"]
            for col in ws.columns:
                max_len = max(
                    len(str(cell.value)) if cell.value is not None else 0
                    for cell in col
                )
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)
        else:
            pd.DataFrame({"note": ["No GPS files found."]}).to_excel(
                writer, sheet_name="gps_file_summary", index=False
            )

        # Auto-size field_inventory columns too
        ws_inv = writer.sheets["field_inventory"]
        for col in ws_inv.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col
            )
            ws_inv.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    # ------------------------------------------------------------------
    # Summary report to console
    # ------------------------------------------------------------------
    log.info("=" * 60)
    log.info("PROFILING COMPLETE")
    log.info("  Total .fit files processed : %d", len(fit_files))
    log.info("  Files with GPS data        : %d", len(gps_rows))
    log.info("  Files with parse errors    : %d", error_count)
    log.info("  Unique (message, field)    : %d", len(all_fields))
    log.info("  Output written to          : %s", output_path)
    log.info("=" * 60)

    if error_count > 0:
        log.warning("%d file(s) could not be parsed. Check logs above.", error_count)


if __name__ == "__main__":
    main()
