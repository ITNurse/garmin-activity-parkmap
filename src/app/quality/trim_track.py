#!/usr/bin/env python3
"""
trim_track.py
-------------
Interactive tool to fix tracks where you forgot to stop your watch.

Opens the track file, shows GPS points with timestamps and speeds, and
lets you specify a cutoff index. Saves the corrected track and logs the
change to the corrections log.

Metadata (distance, speed, duration, elevation) is recalculated
automatically from GPS coordinates after every trim.

Usage:
    python scripts/trim_track.py <activity_id>
    python scripts/trim_track.py 2023-07-14-08-15-32

    # Or omit the ID and enter it when prompted
    python scripts/trim_track.py
"""

import sys
import json
import math
import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import config

CORRECTIONS_LOG = config.CORRECTIONS_LOG


# =============================================================================
# METADATA CALCULATION
# =============================================================================

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the distance between two GPS points in metres using the
    Haversine formula. Accurate to within 0.5% for distances under 500 km.
    """
    R = 6_371_000  # Earth radius in metres
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi    = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (math.sin(d_phi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def calculate_distance(track_points: list) -> float:
    """Sum Haversine distances between consecutive GPS points."""
    total = 0.0
    for i in range(len(track_points) - 1):
        p1, p2 = track_points[i], track_points[i + 1]
        total += haversine_distance(p1["lat"], p1["lon"], p2["lat"], p2["lon"])
    return total


def calculate_elapsed_time(track_points: list) -> float | None:
    """Return elapsed seconds between first and last GPS timestamp."""
    if len(track_points) < 2:
        return None
    t_first = track_points[0].get("t")
    t_last  = track_points[-1].get("t")
    if not t_first or not t_last:
        return None
    try:
        t1 = datetime.fromisoformat(t_first.replace("Z", "+00:00"))
        t2 = datetime.fromisoformat(t_last.replace("Z", "+00:00"))
        return (t2 - t1).total_seconds()
    except Exception:
        return None


def calculate_elevation(track_points: list) -> tuple[float, float]:
    """Return (total_gain_m, total_loss_m) from altitude fields."""
    gain = loss = 0.0
    for i in range(len(track_points) - 1):
        alt1 = track_points[i].get("alt")
        alt2 = track_points[i + 1].get("alt")
        if alt1 is None or alt2 is None:
            continue
        diff = alt2 - alt1
        if diff > 0:
            gain += diff
        else:
            loss += abs(diff)
    return gain, loss


def recalculate_track_metadata(data: dict, reason: str) -> dict:
    """
    Recalculate all computable metadata fields from the GPS track array.
    Updates data['meta'] in place.
    Returns a dict of {field: (old_value, new_value)} for changed fields.
    """
    track_points = data.get("track", [])
    if not track_points:
        return {}

    meta    = data.setdefault("meta", {})
    changes = {}

    # ---- Distance ----
    new_dist = calculate_distance(track_points)
    old_dist = meta.get("total_distance_m")
    if old_dist != new_dist:
        changes["total_distance_m"] = (old_dist, new_dist)
        meta["total_distance_m"]    = round(new_dist, 2)

    # ---- Elapsed time ----
    new_elapsed = calculate_elapsed_time(track_points)
    old_elapsed = meta.get("total_elapsed_time_s")
    if new_elapsed is not None and old_elapsed != new_elapsed:
        changes["total_elapsed_time_s"] = (old_elapsed, new_elapsed)
        meta["total_elapsed_time_s"]    = round(new_elapsed, 3)

    # ---- Average speed ----
    if new_dist and new_elapsed and new_elapsed > 0:
        new_speed = new_dist / new_elapsed
        old_speed = meta.get("avg_speed_ms")
        if old_speed != new_speed:
            changes["avg_speed_ms"] = (old_speed, new_speed)
            meta["avg_speed_ms"]    = round(new_speed, 3)

    # ---- Elevation ----
    new_gain, new_loss = calculate_elevation(track_points)
    old_gain = meta.get("total_ascent_m")
    old_loss = meta.get("total_descent_m")
    if new_gain > 0 and old_gain != new_gain:
        changes["total_ascent_m"] = (old_gain, new_gain)
        meta["total_ascent_m"]    = round(new_gain, 1)
    if new_loss > 0 and old_loss != new_loss:
        changes["total_descent_m"] = (old_loss, new_loss)
        meta["total_descent_m"]    = round(new_loss, 1)

    # ---- End time (always sync to last GPS point) ----
    last_t = track_points[-1].get("t")
    if last_t:
        meta["end_time"] = last_t

    if changes:
        data["metadata_recalculated"]         = True
        data["metadata_recalculated_date"]    = datetime.now().strftime("%Y-%m-%d")
        data["metadata_recalculation_reason"] = reason

    return changes


# =============================================================================
# LOAD / DISPLAY
# =============================================================================

def load_track(activity_id: str) -> tuple[Path, dict] | None:
    """Find and load a .track.json file by activity ID stem."""
    candidates = sorted(config.TRACKS_DIR.glob(f"{activity_id}*.track.json"))
    if not candidates:
        print(f"❌ Track not found: {activity_id}")
        print(f"   Searched in: {config.TRACKS_DIR}")
        return None
    path = candidates[0]
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return path, data


def show_summary(data: dict):
    print("\n" + "=" * 60)
    print(f"Activity ID   : {data.get('activity_id', 'unknown')}")
    print(f"Activity Type : {data.get('activity_type', 'unknown')}")
    print(f"Start Time    : {data.get('start_time_utc', 'unknown')}")
    print(f"Total Points  : {data.get('point_count', len(data.get('track', [])))}")

    meta = data.get("meta", {})
    if meta.get("total_distance_m") is not None:
        print(f"Distance      : {meta['total_distance_m'] / 1000:.2f} km")
    if meta.get("total_elapsed_time_s") is not None:
        print(f"Duration      : {int(meta['total_elapsed_time_s'] / 60)} min")

    if data.get("corrected"):
        print(f"\n⚠️  Previously corrected on {data.get('corrected_date', 'unknown')}")
        print(f"   Reason: {data.get('correction_reason', 'N/A')}")
    print("=" * 60)


def show_points(track_points: list, start_idx: int, count: int = 20):
    end_idx = min(start_idx + count, len(track_points))
    print(f"\nPoints {start_idx} – {end_idx - 1}  (of {len(track_points)} total):")
    print("-" * 80)
    print(f"{'Idx':<6} {'Timestamp':<26} {'Lat':>11} {'Lon':>11}  {'km/h':>6}")
    print("-" * 80)
    for i in range(start_idx, end_idx):
        p     = track_points[i]
        ts    = p.get("t", "N/A")
        lat   = p.get("lat", 0)
        lon   = p.get("lon", 0)
        spd   = p.get("spd")
        speed = f"{spd * 3.6:.1f}" if spd is not None else "  N/A"
        print(f"{i:<6} {ts:<26} {lat:>11.6f} {lon:>11.6f}  {speed:>6}")
    remaining = len(track_points) - end_idx
    if remaining > 0:
        print(f"  ... ({remaining} more points)")
    print("-" * 80)


# =============================================================================
# LOGGING
# =============================================================================

def log_correction(activity_id: str, reason: str,
                   points_before: int, points_after: int):
    CORRECTIONS_LOG.parent.mkdir(parents=True, exist_ok=True)
    file_exists = CORRECTIONS_LOG.exists()
    with open(CORRECTIONS_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "activity_id", "reason",
                              "points_before", "points_after", "user"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            activity_id,
            reason,
            points_before,
            points_after,
            "manual",
        ])
    print(f"\n✅ Correction logged to: {CORRECTIONS_LOG}")


# =============================================================================
# INTERACTIVE TRIM
# =============================================================================

def trim_interactive(path: Path, data: dict):
    track_points = data.get("track", [])
    if not track_points:
        print("❌ No GPS points in this track.")
        return

    total = len(track_points)
    print(f"\nThis track has {total} GPS points.")
    print("\nCommands:")
    print("  <number>     — jump to that point index")
    print("  trim <N>     — keep only the first N points (discard the rest)")
    print("  quit         — exit without changes")

    # Start display near the end of the track where stale data usually lives
    current_idx = max(0, total - 40)

    while True:
        show_points(track_points, current_idx)
        cmd = input("\n> ").strip().lower()

        if cmd == "quit":
            print("Exiting without changes.")
            return

        if cmd.startswith("trim "):
            try:
                cutoff = int(cmd.split()[1])
            except (IndexError, ValueError):
                print("❌ Usage: trim <number>")
                continue

            if cutoff <= 0 or cutoff > total:
                print(f"❌ Cutoff must be between 1 and {total}.")
                continue

            print(f"\n⚠️  This will keep points 0–{cutoff - 1} and permanently "
                  f"delete the last {total - cutoff} points.")
            confirm = input("   Type 'yes' to confirm: ").strip().lower()
            if confirm != "yes":
                print("Cancelled.")
                continue

            reason = input(
                "   Reason (e.g. 'forgot to stop watch, drove home'): "
            ).strip()
            if not reason:
                reason = "Manual trim — forgot to stop watch"

            # ---- Perform trim ----
            points_before = total
            data["track"]             = track_points[:cutoff]
            data["point_count"]       = cutoff
            data["corrected"]         = True
            data["corrected_date"]    = datetime.now().strftime("%Y-%m-%d")
            data["correction_reason"] = reason

            # Sync end_time to the new last point
            last_t = data["track"][-1].get("t")
            if last_t:
                data.setdefault("meta", {})["end_time"] = last_t

            # Recalculate derived metadata from the trimmed track
            recalculate_track_metadata(data, reason=reason)

            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            log_correction(
                data.get("activity_id", path.stem),
                reason, points_before, cutoff
            )
            print(f"\n✅ Saved: {path.name}")
            print(f"   Points: {points_before} → {cutoff}")
            return

        # ---- Jump to index ----
        try:
            idx = int(cmd)
            if 0 <= idx < total:
                current_idx = idx
            else:
                print(f"❌ Index must be between 0 and {total - 1}.")
        except ValueError:
            print("❌ Unknown command. Type a number, 'trim <N>', or 'quit'.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    activity_id = sys.argv[1] if len(sys.argv) > 1 else input(
        "Enter activity_id (shown as 🔑 in the viewer, e.g. 2023_07_14_081532_filename): "
    ).strip()

    if not activity_id:
        print("❌ No activity ID provided.")
        sys.exit(1)

    result = load_track(activity_id)
    if result is None:
        sys.exit(1)

    path, data = result
    show_summary(data)

    print("\nOptions:")
    print("  1 — Trim GPS points")
    print("  2 — Cancel")
    choice = input("\n> ").strip()

    if choice == "1":
        trim_interactive(path, data)
    else:
        print("Cancelled.")


if __name__ == "__main__":
    main()
