#!/usr/bin/env python3
"""
view_tracks.py
--------------
Interactive web viewer for Garmin activity tracks.
Serves a local map interface with filtering by activity type, park, and date.

Reads .track.json files from data/processed/tracks/ as produced by
02_preprocess_fit_files.py and 03_match_parks.py.

Usage:
    python scripts/view_tracks.py

    Then open http://127.0.0.1:5000 in your browser.
    Press Ctrl+C in the terminal to stop the server.

Requirements:
    pip install flask
    (already included in requirements.txt)
"""

import sys
import json
import csv
import importlib.util
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, render_template_string, request

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
TRACKS_DIR = PROJECT_ROOT / "data" / "processed" / "tracks"
try:
    import config
    HOST = config.VIEWER_HOST
    PORT = config.VIEWER_PORT
except ImportError:
    print("ERROR: config.py not found.")
    print(f"  Expected location: {PROJECT_ROOT / 'config.py'}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Import trim logic from src/03_quality/trim_track.py
# ---------------------------------------------------------------------------
_TRIM_TRACK_PATH = Path(__file__).resolve().parent.parent / "03_quality" / "trim_track.py"
_spec = importlib.util.spec_from_file_location("trim_track", _TRIM_TRACK_PATH)
_trim_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_trim_mod)

recalculate_track_metadata = _trim_mod.recalculate_track_metadata
CORRECTIONS_LOG = config.CORRECTIONS_LOG

app = Flask(__name__)

if not TRACKS_DIR.exists():
    print(f"Warning: Tracks folder not found: {TRACKS_DIR}")
    print("  Run 02_preprocess_fit_files.py first.")


# =============================================================================
# TRACK LOADING
# =============================================================================

def load_track_file(path: Path) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def normalise_track(raw: dict, path: Path) -> dict | None:
    """
    Accept either format and return a normalised dict the viewer can use.

    Format A — internal format:
        { "track": [...], "meta": {...}, "point_count": N, ... }

    Format B — GeoJSON FeatureCollection (from 01_preprocess_fit_files.py):
        { "type": "FeatureCollection", "features": [...], "parks": [...] }
    """
    if raw is None:
        return None

    if "track" in raw:
        return raw

    features = raw.get("features")
    if not features:
        return None

    track_points = []
    for feat in features:
        geom   = feat.get("geometry") or {}
        props  = feat.get("properties") or {}
        coords = geom.get("coordinates")
        if not coords or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        alt      = coords[2] if len(coords) > 2 else None
        point    = {"lat": lat, "lon": lon}
        if props.get("timestamp"):
            point["t"] = props["timestamp"]
        if alt is not None:
            point["alt"] = alt
        track_points.append(point)

    if not track_points:
        return None

    first_props = features[0].get("properties") or {}
    sport_raw   = first_props.get("sport", "")
    sport       = sport_raw if sport_raw and sport_raw != "nan" else "unknown"
    start_time  = first_props.get("start_time") or first_props.get("timestamp")
    end_time    = first_props.get("end_time")

    import math
    def haversine(p1, p2):
        R    = 6_371_000
        lat1 = math.radians(p1["lat"]); lat2 = math.radians(p2["lat"])
        dlat = lat2 - lat1
        dlon = math.radians(p2["lon"] - p1["lon"])
        a    = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))

    total_dist = sum(
        haversine(track_points[i], track_points[i + 1])
        for i in range(len(track_points) - 1)
    )

    elapsed_s = None
    if start_time and end_time:
        try:
            def _parse(s):
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            elapsed_s = (_parse(end_time) - _parse(start_time)).total_seconds()
        except Exception:
            pass

    return {
        "track":           track_points,
        "point_count":     len(track_points),
        "first_timestamp": start_time,
        "first_lat":       track_points[0]["lat"],
        "first_lon":       track_points[0]["lon"],
        "activity_id":     path.stem.replace(".track", ""),
        "corrected":       raw.get("corrected", False),
        "parks":           raw.get("parks", []),
        "meta": {
            "sport":                sport,
            "total_distance_m":     round(total_dist, 1) if total_dist else None,
            "total_elapsed_time_s": elapsed_s,
            "avg_heart_rate":       None,
            "total_ascent_m":       None,
        },
    }


def scan_tracks() -> list[dict]:
    """Return a sorted list of track summary dicts for the sidebar."""
    if not TRACKS_DIR.exists():
        return []

    entries = []
    for path in TRACKS_DIR.rglob("*.track.json"):
        raw   = load_track_file(path)
        track = normalise_track(raw, path)
        if track is None:
            continue

        meta       = track.get("meta", {})
        dist       = meta.get("total_distance_m")
        dist_km    = round(dist / 1000, 2) if dist else None
        display_id = path.name.removesuffix(".track.json")

        entries.append({
            "id":              display_id,
            "display_id":      display_id,
            "activity_id":     track.get("activity_id", display_id),
            "filename":        path.name,
            "first_timestamp": track.get("first_timestamp"),
            "first_lat":       track.get("first_lat"),
            "first_lon":       track.get("first_lon"),
            "point_count":     track.get("point_count", 0),
            "sport":           meta.get("sport") or "unknown",
            "distance_km":     dist_km,
            "avg_hr":          meta.get("avg_heart_rate"),
            "duration_min":    round(meta.get("total_elapsed_time_s") / 60, 1) if meta.get("total_elapsed_time_s") else None,
            "corrected":       track.get("corrected", False),
            "parks": [
                p if isinstance(p, dict) else {"name": p, "source": "custom"}
                for p in track.get("parks", [])
            ],
        })

    entries.sort(
        key=lambda x: x.get("first_timestamp") or "",
        reverse=True,
    )
    return entries


def find_track_path(track_id: str) -> Path | None:
    candidates = list(TRACKS_DIR.rglob(f"{track_id}*.json"))
    return candidates[0] if candidates else None


# =============================================================================
# ROUTES
# =============================================================================

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/tracks")
def api_tracks():
    return jsonify(scan_tracks())


@app.route("/api/parks")
def api_parks():
    if not TRACKS_DIR.exists():
        return jsonify([])
    seen = {}
    for path in TRACKS_DIR.rglob("*.track.json"):
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
            for p in d.get("parks", []):
                if isinstance(p, dict):
                    seen[p["name"]] = p.get("source", "custom")
                else:
                    seen[p] = "custom"
        except Exception:
            pass
    return jsonify([
        {"name": name, "source": source}
        for name, source in sorted(seen.items())
    ])


@app.route("/api/track/<path:track_id>")
def api_track(track_id):
    candidates = list(TRACKS_DIR.rglob(f"{track_id}*.json"))
    if not candidates:
        return jsonify({"error": "Track not found"}), 404
    raw   = load_track_file(candidates[0])
    track = normalise_track(raw, candidates[0])
    if track is None:
        return jsonify({"error": "Could not load track"}), 500
    return jsonify(track)


@app.route("/api/track/<path:track_id>/set_sport", methods=["PATCH"])
def api_set_sport(track_id):
    body      = request.get_json(silent=True) or {}
    new_sport = (body.get("sport") or "").strip().lower()
    if not new_sport:
        return jsonify({"error": "Missing sport value"}), 400

    path = find_track_path(track_id)
    if path is None:
        return jsonify({"error": "Track not found"}), 404

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return jsonify({"error": f"Could not read file: {e}"}), 500

    if "meta" in data:
        data["meta"]["sport"] = new_sport
    elif "features" in data and data["features"]:
        for feat in data["features"]:
            props = feat.get("properties") or {}
            props["sport"] = new_sport
            feat["properties"] = props
    else:
        return jsonify({"error": "Unrecognised track format"}), 500

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return jsonify({"error": f"Could not write file: {e}"}), 500

    return jsonify({"ok": True, "sport": new_sport})


@app.route("/api/track/<path:track_id>/trim", methods=["POST"])
def api_trim(track_id):
    """
    Trim a track to the first `cutoff` points.

    Body (JSON):
        { "cutoff": <int>, "reason": "<string>" }

    Returns the updated track summary on success.
    """
    body   = request.get_json(silent=True) or {}
    cutoff = body.get("cutoff")
    reason = (body.get("reason") or "").strip() or "Manual trim via viewer"

    if cutoff is None or not isinstance(cutoff, int) or cutoff < 1:
        return jsonify({"error": "cutoff must be a positive integer"}), 400

    path = find_track_path(track_id)
    if path is None:
        return jsonify({"error": "Track not found"}), 404

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return jsonify({"error": f"Could not read file: {e}"}), 500

    track_points  = data.get("track", [])
    points_before = len(track_points)

    if cutoff >= points_before:
        return jsonify({"error": f"cutoff ({cutoff}) must be less than total points ({points_before})"}), 400

    # --- Perform trim ---
    data["track"]             = track_points[:cutoff]
    data["point_count"]       = cutoff
    data["corrected"]         = True
    data["corrected_date"]    = datetime.now().strftime("%Y-%m-%d")
    data["correction_reason"] = reason

    last_t = data["track"][-1].get("t")
    if last_t:
        data.setdefault("meta", {})["end_time"] = last_t

    recalculate_track_metadata(data, reason=reason)

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        return jsonify({"error": f"Could not write file: {e}"}), 500

    try:
        CORRECTIONS_LOG.parent.mkdir(parents=True, exist_ok=True)
        file_exists = CORRECTIONS_LOG.exists()
        with open(CORRECTIONS_LOG, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "activity_id", "reason",
                                  "points_before", "points_after", "user"])
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                track_id, reason, points_before, cutoff, "viewer",
            ])
    except Exception:
        pass

    meta    = data.get("meta", {})
    dist    = meta.get("total_distance_m")
    elapsed = meta.get("total_elapsed_time_s")

    return jsonify({
        "ok":            True,
        "points_before": points_before,
        "points_after":  cutoff,
        "distance_km":   round(dist / 1000, 2) if dist else None,
        "duration_min":  round(elapsed / 60, 1) if elapsed else None,
        "track":         data,
    })


# =============================================================================
# HTML TEMPLATE
# =============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>FIT Track Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  :root {
    --bg: #1a1d24;
    --surface: #242831;
    --border: #3a3f4b;
    --text: #d4d7dd;
    --text-dim: #898e9a;
    --accent: #00e5a0;
    --accent-alt: #0099ff;
    --danger: #ff6464;
    --sidebar-w: 320px;
  }
  body { font-family: 'Space Mono', monospace; background: var(--bg); color: var(--text); height: 100vh; overflow: hidden; }
  #sidebar {
    width: var(--sidebar-w); height: 100vh; background: var(--surface);
    border-right: 1px solid var(--border); float: left;
    display: flex; flex-direction: column; overflow: hidden;
  }
  #sidebar-header { padding: 16px 14px 12px; border-bottom: 1px solid var(--border); flex-shrink: 0; }
  #sidebar-header h1 { font-size: 14px; font-weight: 700; letter-spacing: 0.5px; color: var(--accent); text-transform: uppercase; }
  #sidebar-header h1 span.dot {
    display: inline-block; width: 6px; height: 6px; background: var(--accent);
    border-radius: 50%; margin-left: 6px; vertical-align: middle;
  }
  #track-count { font-size: 10px; color: var(--text-dim); margin-top: 6px; letter-spacing: 0.3px; }
  #loading-wrap { margin-top: 10px; }
  #progress-bar-bg { width: 100%; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
  #progress-bar-fill { height: 100%; width: 0%; background: var(--accent); border-radius: 2px; transition: width 0.2s ease; }
  #selection-bar {
    display: none; align-items: center; padding: 6px 14px;
    background: #1e2940; border-bottom: 1px solid #2a3f6a;
    font-size: 10px; color: var(--accent-alt); flex-shrink: 0; gap: 6px;
  }
  #selection-bar.visible { display: flex; }
  #sel-count { flex: 1; }
  #selection-bar button {
    background: none; border: 1px solid currentColor; border-radius: 4px;
    color: inherit; font-family: 'Space Mono', monospace;
    font-size: 9px; padding: 3px 7px; cursor: pointer; white-space: nowrap;
  }
  #selection-bar button:hover { background: rgba(0,153,255,0.15); }
  #btn-clear-sel { color: var(--text-dim) !important; border-color: var(--text-dim) !important; }
  #search {
    width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    color: var(--text); font-family: 'Space Mono', monospace; font-size: 11px;
    padding: 8px 12px; margin-top: 12px; outline: none; transition: border-color 0.2s;
  }
  #search:focus { border-color: var(--accent); }
  #search::placeholder { color: var(--text-dim); }
  #sort-bar { display: flex; gap: 6px; padding: 12px 14px 6px; flex-shrink: 0; flex-wrap: wrap; }
  .sort-btn {
    font-family: 'Space Mono', monospace; font-size: 10px; letter-spacing: 0.3px;
    padding: 5px 10px; border: 1px solid var(--border); border-radius: 4px;
    background: transparent; color: var(--text-dim); cursor: pointer;
    transition: all 0.2s; text-transform: uppercase; flex: 1;
  }
  .sort-btn:hover { background: var(--bg); border-color: var(--accent); color: var(--text); }
  .sort-btn.active { background: var(--accent); color: var(--bg); border-color: var(--accent); }
  #btn-unknown {
    font-family: 'Space Mono', monospace; font-size: 10px; letter-spacing: 0.3px;
    padding: 5px 10px; border: 1px solid #ffaa00; border-radius: 4px;
    background: transparent; color: #ffaa00; cursor: pointer;
    transition: all 0.2s; text-transform: uppercase; white-space: nowrap;
  }
  #btn-unknown:hover { background: rgba(255,170,0,0.1); }
  #btn-unknown.active { background: #ffaa00; color: var(--bg); }

  /* ── Filter row (sport + park side by side) ── */
  #filter-row { display: flex; gap: 6px; padding: 6px 14px 0; flex-shrink: 0; }
  #filter-row > * { flex: 1; min-width: 0; }
  .filter-select {
    width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    color: var(--text); font-family: 'Space Mono', monospace; font-size: 11px;
    padding: 7px 24px 7px 8px; outline: none; cursor: pointer;
    transition: border-color 0.2s; appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%235a6478'/%3E%3C/svg%3E");
    background-repeat: no-repeat; background-position: right 8px center;
  }
  .filter-select:focus { border-color: var(--accent); }
  .filter-select option { background: var(--surface); }

  .park-pill {
    display: inline-block; font-size: 9px; letter-spacing: 0.5px;
    padding: 1px 5px; border-radius: 3px; margin-top: 3px; margin-right: 2px;
    border: 1px solid transparent;
  }
  .park-pill.source-custom     { background: rgba(0,153,255,0.15); color:#4db8ff; border-color:rgba(0,153,255,0.25); }
  .park-pill.source-national   { background: rgba(255,80,80,0.15);  color:#ff6b6b; border-color:rgba(255,80,80,0.25); }
  .park-pill.source-provincial { background: rgba(80,200,120,0.15); color:#50c878; border-color:rgba(80,200,120,0.25); }

  #track-list { flex: 1; overflow-y: auto; padding: 6px; }
  .track-item {
    background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    padding: 10px 12px; margin-bottom: 6px; cursor: pointer;
    transition: all 0.15s; font-size: 11px; position: relative;
  }
  .track-item:hover { background: #1f2229; }
  .track-item.active { background: rgba(0,229,160,0.06); padding-left: 16px; }
  .track-item.active::before {
    content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
    border-radius: 6px 0 0 6px; background: var(--track-color, var(--accent));
  }
  .track-item .ts {
    font-weight: 600; color: var(--text); margin-bottom: 4px;
    display: flex; align-items: center; justify-content: space-between;
  }
  .track-item .sport-badge {
    font-size: 9px; letter-spacing: 0.5px; padding: 2px 6px;
    border-radius: 3px; text-transform: uppercase; font-weight: 600;
  }
  .track-item .sport-badge.walking  { background: rgba(100,210,255,0.2); color: #64d2ff; }
  .track-item .sport-badge.hiking   { background: rgba(255,180,100,0.2); color: #ffb464; }
  .track-item .sport-badge.cycling  { background: rgba(255,100,100,0.2); color: #ff6464; }
  .track-item .sport-badge.running  { background: rgba(150,100,255,0.2); color: #9664ff; }
  .track-item .sport-badge.swimming { background: rgba(0,180,255,0.2);   color: #00b4ff; }
  .track-item .sport-badge.unknown  { background: rgba(255,170,0,0.2);   color: #ffaa00; }
  .track-item .coords { font-size: 10px; color: var(--text-dim); margin-bottom: 4px; }
  .track-item .display-id {
    font-size: 9px; color: #5a7fa8; margin-bottom: 4px;
    font-family: monospace; letter-spacing: 0.2px; cursor: text; user-select: all;
  }
  .track-item .stats { font-size: 10px; color: var(--text-dim); display: flex; gap: 10px; flex-wrap: wrap; }

  /* ── Sport editor ── */
  .sport-editor {
    display: flex; align-items: center; gap: 5px;
    margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--border);
  }
  .sport-editor select,
  .sport-editor input[type="text"] {
    flex: 1; background: var(--surface); border: 1px solid var(--border); border-radius: 4px;
    color: var(--text); font-family: 'Space Mono', monospace; font-size: 10px; padding: 4px 7px;
    outline: none; min-width: 0; transition: border-color 0.2s;
  }
  .sport-editor select:focus,
  .sport-editor input[type="text"]:focus { border-color: var(--accent); }
  .sport-editor input[type="text"]::placeholder { color: var(--text-dim); }
  .sport-editor button.save-sport {
    background: var(--accent); border: none; border-radius: 4px; color: var(--bg);
    font-family: 'Space Mono', monospace; font-size: 10px; font-weight: 700;
    padding: 4px 10px; cursor: pointer; white-space: nowrap; transition: opacity 0.2s; flex-shrink: 0;
  }
  .sport-editor button.save-sport:hover { opacity: 0.85; }
  .sport-editor button.save-sport:disabled { opacity: 0.4; cursor: default; }
  .sport-editor .save-status { font-size: 9px; flex-shrink: 0; }
  .sport-editor .save-status.ok  { color: var(--accent); }
  .sport-editor .save-status.err { color: var(--danger); }

  /* ── Trim panel ── */
  .trim-panel {
    margin-top: 8px; padding: 10px 10px 8px;
    border: 1px solid rgba(255,100,100,0.3); border-radius: 6px;
    background: rgba(255,100,100,0.04);
  }
  .trim-panel-title {
    font-size: 9px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; color: var(--danger); margin-bottom: 8px;
  }
  .trim-row { display: flex; align-items: center; gap: 6px; margin-bottom: 6px; }
  .trim-row label { font-size: 9px; color: var(--text-dim); white-space: nowrap; width: 52px; flex-shrink: 0; }
  .trim-row input[type="number"],
  .trim-row input[type="text"] {
    flex: 1; min-width: 0; background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; color: var(--text); font-family: 'Space Mono', monospace;
    font-size: 10px; padding: 4px 7px; outline: none; transition: border-color 0.2s;
  }
  .trim-row input:focus { border-color: var(--danger); }
  .trim-row input::placeholder { color: var(--text-dim); }
  .trim-row input[type="number"]::-webkit-inner-spin-button { opacity: 1; }
  .trim-hint { font-size: 9px; color: var(--text-dim); margin-bottom: 8px; line-height: 1.5; }
  .trim-actions { display: flex; align-items: center; gap: 6px; }
  .btn-trim {
    background: var(--danger); border: none; border-radius: 4px; color: #fff;
    font-family: 'Space Mono', monospace; font-size: 10px; font-weight: 700;
    padding: 5px 12px; cursor: pointer; transition: opacity 0.2s; flex-shrink: 0;
  }
  .btn-trim:hover { opacity: 0.85; }
  .btn-trim:disabled { opacity: 0.4; cursor: default; }
  .trim-status { font-size: 9px; flex: 1; }
  .trim-status.ok  { color: var(--accent); }
  .trim-status.err { color: var(--danger); }

  /* ── Map ── */
  #map-container { margin-left: var(--sidebar-w); height: 100vh; position: relative; }
  #map { width: 100%; height: 100%; }
  #empty {
    position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
    text-align: center; color: var(--text-dim); font-size: 13px;
  }
  #info-panel {
    position: absolute; top: 20px; right: 20px; background: var(--surface);
    border: 1px solid var(--border); border-radius: 8px; padding: 0; width: 300px;
    max-height: calc(100vh - 80px); overflow-y: auto; font-size: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3); display: none;
  }
  #info-panel.visible { display: block; }
  .info-card { padding: 14px 16px; border-bottom: 1px solid var(--border); }
  .info-card:last-child { border-bottom: none; }
  .info-card-title {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 8px; display: flex; align-items: center; gap: 8px;
  }
  .info-swatch { display: inline-block; width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .info-card .row {
    display: flex; justify-content: space-between; margin-bottom: 4px; padding: 3px 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
  }
  .info-card .row:last-child { border: none; }
  .info-card .label { color: var(--text-dim); }
  .info-card .value { color: var(--text); font-weight: 600; }
  .leaflet-popup-content-wrapper {
    background: var(--surface); color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; font-family: 'Space Mono', monospace; font-size: 11px;
  }
  .leaflet-popup-content { margin: 10px 12px; line-height: 1.6; }
  .leaflet-popup-tip { background: var(--surface); }
  .leaflet-popup-content strong { color: var(--accent); }
</style>
</head>
<body>

<div id="sidebar">
  <div id="sidebar-header">
    <h1>FIT Tracks<span class="dot"></span></h1>
    <div id="track-count" style="display:none"></div>
    <div id="loading-wrap">
      <div id="loading-label" style="font-size:10px;color:var(--text-dim);margin-bottom:5px;letter-spacing:0.3px">Loading tracks…</div>
      <div id="progress-bar-bg"><div id="progress-bar-fill"></div></div>
    </div>
    <input type="text" id="search" placeholder="Search activities..." />
  </div>

  <div id="selection-bar">
    <span id="sel-count"></span>
    <button id="btn-fit-all">Fit map</button>
    <button id="btn-clear-sel">Clear</button>
  </div>

  <div id="sort-bar">
    <button class="sort-btn active" data-sort="date">Date</button>
    <button class="sort-btn" data-sort="sport">Sport</button>
    <button class="sort-btn" data-sort="distance">Dist</button>
    <button class="sort-btn" data-sort="duration">Dur</button>
    <button class="sort-btn" data-sort="points">Pts</button>
    <button id="btn-unknown" title="Show only tracks with unknown sport">? Sport</button>
  </div>

  <div id="filter-row">
    <select id="sport-filter" class="filter-select">
      <option value="">🏃 All sports</option>
    </select>
    <select id="park-filter" class="filter-select">
      <option value="">🌲 All parks</option>
      <option value="__none__">— No park</option>
    </select>
  </div>

  <div id="track-list"></div>
</div>

<div id="map-container">
  <div id="map"></div>
  <div id="empty"><p>Click tracks in the sidebar to show them on the map.<br><small style="color:var(--text-dim)">Click again to deselect.</small></p></div>
  <div id="info-panel"></div>
</div>

<script>
const map = L.map('map').setView([60.0, -96.0], 4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© OpenStreetMap'
}).addTo(map);

const TRACK_COLOURS = [
  '#00e5a0','#0099ff','#ff6464','#ffb464','#c864ff',
  '#ff64c8','#64d2ff','#a0ff64','#ffd700','#ff8c00',
];
function colourForIndex(i) { return TRACK_COLOURS[i % TRACK_COLOURS.length]; }

// ── State ─────────────────────────────────────────────────────────────────
let allTracks       = [];
let filteredTracks  = [];
let currentSort     = 'date';
let showUnknownOnly = false;
const selected      = new Map();

const SPORT_OPTIONS = ['walking','hiking','cycling','running','swimming','other'];

// ── Helpers ───────────────────────────────────────────────────────────────
function fmtDate(ts) {
  if (!ts) return 'Unknown';
  return new Date(ts).toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
}
function fmtCoord(lat, lon) {
  if (lat == null || lon == null) return '';
  return lat.toFixed(5) + ', ' + lon.toFixed(5);
}
function fmtDist(km) {
  return km >= 1 ? km.toFixed(1) + ' km' : Math.round(km * 1000) + ' m';
}
function isUnknownSport(sport) {
  return !sport || sport === 'unknown' || sport === 'nan' || sport === 'other';
}

// ── Populate sport filter from loaded tracks ──────────────────────────────
function populateSportFilter() {
  const sel    = document.getElementById('sport-filter');
  const sports = [...new Set(allTracks.map(t => (t.sport || 'unknown').toLowerCase()))].sort();
  // Keep only the "All sports" placeholder, then add discovered sports
  while (sel.options.length > 1) sel.remove(1);
  sports.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s;
    opt.textContent = s.charAt(0).toUpperCase() + s.slice(1);
    sel.appendChild(opt);
  });
}

// ── Selection toolbar ─────────────────────────────────────────────────────
function updateSelectionBar() {
  const bar = document.getElementById('selection-bar');
  const n   = selected.size;
  if (n === 0) { bar.classList.remove('visible'); }
  else {
    bar.classList.add('visible');
    document.getElementById('sel-count').textContent =
      n + ' track' + (n !== 1 ? 's' : '') + ' selected';
  }
}

// ── Info panel ────────────────────────────────────────────────────────────
function updateInfoPanel() {
  const panel = document.getElementById('info-panel');
  if (selected.size === 0) {
    panel.className = ''; panel.innerHTML = '';
    document.getElementById('empty').style.display = ''; return;
  }
  document.getElementById('empty').style.display = 'none';
  panel.className = 'visible'; panel.innerHTML = '';

  selected.forEach(({ colour, trackData }, id) => {
    const meta = trackData.meta || {};
    const card = document.createElement('div');
    card.className = 'info-card';
    card.setAttribute('data-info-id', id);
    card.innerHTML =
      '<div class="info-card-title">' +
        '<span class="info-swatch" style="background:' + colour + '"></span>' +
        fmtDate(trackData.first_timestamp) + ' — ' + (meta.sport || 'activity') +
      '</div>' +
      (meta.total_distance_m
        ? '<div class="row"><span class="label">Distance</span><span class="value">' + fmtDist(meta.total_distance_m / 1000) + '</span></div>' : '') +
      (meta.total_elapsed_time_s
        ? '<div class="row"><span class="label">Duration</span><span class="value" data-field="duration">' + Math.floor(meta.total_elapsed_time_s / 60) + ' min</span></div>' : '') +
      (meta.avg_heart_rate
        ? '<div class="row"><span class="label">Avg HR</span><span class="value">' + meta.avg_heart_rate + ' bpm</span></div>' : '') +
      (meta.total_ascent_m
        ? '<div class="row"><span class="label">Ascent</span><span class="value">' + Math.round(meta.total_ascent_m) + ' m</span></div>' : '') +
      '<div class="row"><span class="label">Points</span><span class="value" data-field="points">' + trackData.point_count + '</span></div>';
    panel.appendChild(card);
  });
}

// ── Sport editor ──────────────────────────────────────────────────────────
function buildSportEditor(trackId, currentSport) {
  const wrap = document.createElement('div');
  wrap.className = 'sport-editor';
  wrap.addEventListener('click', e => e.stopPropagation());

  const sel = document.createElement('select');
  SPORT_OPTIONS.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s.charAt(0).toUpperCase() + s.slice(1);
    if (s === currentSport) opt.selected = true;
    sel.appendChild(opt);
  });
  const otherOpt = document.createElement('option');
  otherOpt.value = '__other__'; otherOpt.textContent = 'Other…';
  sel.appendChild(otherOpt);

  const textInput = document.createElement('input');
  textInput.type = 'text'; textInput.placeholder = 'Enter sport…'; textInput.style.display = 'none';
  sel.addEventListener('change', () => {
    textInput.style.display = sel.value === '__other__' ? 'block' : 'none';
    if (sel.value !== '__other__') textInput.value = '';
  });

  const saveBtn = document.createElement('button');
  saveBtn.className = 'save-sport'; saveBtn.textContent = 'Save';
  const status = document.createElement('span');
  status.className = 'save-status';

  saveBtn.addEventListener('click', async () => {
    const value = sel.value === '__other__' ? textInput.value.trim().toLowerCase() : sel.value;
    if (!value || value === '__other__') {
      status.textContent = 'Enter a sport'; status.className = 'save-status err'; return;
    }
    saveBtn.disabled = true; status.textContent = '…'; status.className = 'save-status';
    try {
      const res  = await fetch('/api/track/' + encodeURIComponent(trackId) + '/set_sport', {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sport: value }),
      });
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || 'Save failed');
      const entry = allTracks.find(t => t.id === trackId);
      if (entry) entry.sport = value;
      const item = document.querySelector('.track-item[data-id="' + trackId + '"]');
      if (item) {
        const badge = item.querySelector('.sport-badge');
        if (badge) { badge.textContent = value; badge.className = 'sport-badge ' + value; }
      }
      // Refresh sport filter options to include any newly introduced sport
      populateSportFilter();
      status.textContent = '✓'; status.className = 'save-status ok';
      if (showUnknownOnly) setTimeout(() => renderList(), 800);
    } catch (err) {
      status.textContent = '✗ ' + err.message; status.className = 'save-status err';
      saveBtn.disabled = false;
    }
  });

  wrap.appendChild(sel); wrap.appendChild(textInput);
  wrap.appendChild(saveBtn); wrap.appendChild(status);
  return wrap;
}

// ── Trim panel ────────────────────────────────────────────────────────────
function buildTrimPanel(trackId, totalPoints) {
  const wrap = document.createElement('div');
  wrap.className = 'trim-panel';
  wrap.addEventListener('click', e => e.stopPropagation());

  wrap.innerHTML =
    '<div class="trim-panel-title">✂ Trim track</div>' +
    '<div class="trim-hint">Keep first N points (total: ' + totalPoints + ').</div>' +
    '<div class="trim-row">' +
      '<label>Keep pts</label>' +
      '<input type="number" class="trim-cutoff" min="1" max="' + (totalPoints - 1) + '" placeholder="e.g. ' + Math.floor(totalPoints * 0.8) + '">' +
    '</div>' +
    '<div class="trim-row">' +
      '<label>Reason</label>' +
      '<input type="text" class="trim-reason" placeholder="e.g. forgot to stop watch">' +
    '</div>' +
    '<div class="trim-actions">' +
      '<button class="btn-trim">Trim</button>' +
      '<span class="trim-status"></span>' +
    '</div>';

  const cutoffInput = wrap.querySelector('.trim-cutoff');
  const reasonInput = wrap.querySelector('.trim-reason');
  const trimBtn     = wrap.querySelector('.btn-trim');
  const statusEl    = wrap.querySelector('.trim-status');

  trimBtn.addEventListener('click', async () => {
    const cutoff = parseInt(cutoffInput.value, 10);
    const reason = reasonInput.value.trim();
    if (!cutoff || cutoff < 1 || cutoff >= totalPoints) {
      statusEl.textContent = 'Enter a value between 1 and ' + (totalPoints - 1);
      statusEl.className   = 'trim-status err'; return;
    }
    trimBtn.disabled = true; statusEl.textContent = 'Trimming…'; statusEl.className = 'trim-status';
    try {
      const res  = await fetch('/api/track/' + encodeURIComponent(trackId) + '/trim', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cutoff, reason: reason || 'Manual trim via viewer' }),
      });
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || 'Trim failed');
      statusEl.textContent = '✓ ' + data.points_before + ' → ' + data.points_after + ' pts';
      statusEl.className   = 'trim-status ok';
      const entry = allTracks.find(t => t.id === trackId);
      if (entry) {
        entry.point_count  = data.points_after;
        entry.distance_km  = data.distance_km;
        entry.duration_min = data.duration_min;
        entry.corrected    = true;
      }
      await reloadSelectedTrack(trackId, data.track);
    } catch (err) {
      statusEl.textContent = '✗ ' + err.message; statusEl.className = 'trim-status err';
      trimBtn.disabled = false;
    }
  });

  return wrap;
}

// ── Reload a track on the map after trim ──────────────────────────────────
async function reloadSelectedTrack(id, updatedTrackData) {
  const entry = selected.get(id);
  if (!entry) return;
  entry.layers.forEach(l => map.removeLayer(l));

  const colour = entry.colour;
  const layers = [];
  const coords = updatedTrackData.track.map(p => [p.lat, p.lon]);

  const line = L.polyline(coords, { color: colour, weight: 4, opacity: 0.85 }).addTo(map);
  layers.push(line);

  updatedTrackData.track.forEach((p, idx) => {
    const marker = L.circleMarker([p.lat, p.lon], {
      radius: 3, fillColor: colour, fillOpacity: 0.6, color: '#ffffff', weight: 1
    }).addTo(map);
    let popup = '<strong>Point ' + (idx + 1) + '</strong><br>' +
                'Lat: ' + p.lat.toFixed(6) + '<br>Lon: ' + p.lon.toFixed(6);
    if (p.t)           popup += '<br>Time: '       + p.t;
    if (p.alt != null) popup += '<br>Altitude: '   + p.alt.toFixed(1) + ' m';
    if (p.hr  != null) popup += '<br>Heart Rate: ' + p.hr + ' bpm';
    if (p.spd != null) popup += '<br>Speed: '      + (p.spd * 3.6).toFixed(1) + ' km/h';
    marker.bindPopup(popup);
    layers.push(marker);
  });

  const startM = L.circleMarker(coords[0], {
    color: colour, radius: 7, fillOpacity: 0.9, weight: 2, fillColor: colour
  }).addTo(map);
  startM.bindPopup('<strong>Start</strong><br>' + fmtDate(updatedTrackData.first_timestamp));
  layers.push(startM);

  const endM = L.circleMarker(coords[coords.length - 1], {
    color: '#ffffff', radius: 5, fillOpacity: 0.9, weight: 2, fillColor: colour
  }).addTo(map);
  endM.bindPopup('<strong>End</strong>');
  layers.push(endM);

  selected.set(id, { colour, layers, trackData: updatedTrackData });

  const item = document.querySelector('.track-item[data-id="' + id + '"]');
  if (item) {
    const statsEl = item.querySelector('.stats');
    if (statsEl) {
      const e = allTracks.find(t => t.id === id);
      if (e) statsEl.innerHTML =
        '<span>📍 ' + e.point_count + ' pts</span>' +
        (e.distance_km  != null ? '<span>↔ ' + fmtDist(e.distance_km)      + '</span>' : '') +
        (e.duration_min != null ? '<span>⏱ ' + e.duration_min + ' min</span>' : '') +
        (e.avg_hr       != null ? '<span>♥ '  + e.avg_hr      + ' bpm</span>' : '');
    }
    const tsEl = item.querySelector('.ts');
    if (tsEl && !tsEl.querySelector('.corrected-badge')) {
      const badge = document.createElement('span');
      badge.className = 'corrected-badge';
      badge.style.cssText = 'font-size:9px;color:#ffaa00;margin-left:6px;';
      badge.title = 'Manually corrected'; badge.textContent = '✏️';
      tsEl.insertBefore(badge, tsEl.querySelector('.sport-badge'));
    }
  }

  const infoCard = document.querySelector('[data-info-id="' + id + '"]');
  if (infoCard) {
    const ptsEl = infoCard.querySelector('[data-field="points"]');
    if (ptsEl) ptsEl.textContent = updatedTrackData.point_count;
    const durEl = infoCard.querySelector('[data-field="duration"]');
    if (durEl && updatedTrackData.meta && updatedTrackData.meta.total_elapsed_time_s)
      durEl.textContent = Math.floor(updatedTrackData.meta.total_elapsed_time_s / 60) + ' min';
  }

  fitAll();
}

// ── Toggle a track on/off ─────────────────────────────────────────────────
async function toggleTrack(id) {
  if (selected.has(id)) {
    const entry = selected.get(id);
    entry.layers.forEach(l => map.removeLayer(l));
    selected.delete(id);
    const item = document.querySelector('.track-item[data-id="' + id + '"]');
    if (item) {
      item.classList.remove('active'); item.style.removeProperty('--track-color');
      item.querySelector('.sport-editor')?.remove();
      item.querySelector('.trim-panel')?.remove();
    }
    updateSelectionBar(); updateInfoPanel(); fitAll(); return;
  }

  const res  = await fetch('/api/track/' + encodeURIComponent(id));
  const data = await res.json();
  if (data.error) { alert(data.error); return; }

  const colour = colourForIndex(selected.size);
  const layers = [];
  const coords = data.track.map(p => [p.lat, p.lon]);

  const line = L.polyline(coords, { color: colour, weight: 4, opacity: 0.85 }).addTo(map);
  layers.push(line);

  data.track.forEach((p, idx) => {
    const marker = L.circleMarker([p.lat, p.lon], {
      radius: 3, fillColor: colour, fillOpacity: 0.6, color: '#ffffff', weight: 1
    }).addTo(map);
    let popup = '<strong>Point ' + (idx + 1) + '</strong><br>' +
                'Lat: ' + p.lat.toFixed(6) + '<br>Lon: ' + p.lon.toFixed(6);
    if (p.t)           popup += '<br>Time: '       + p.t;
    if (p.alt != null) popup += '<br>Altitude: '   + p.alt.toFixed(1) + ' m';
    if (p.hr  != null) popup += '<br>Heart Rate: ' + p.hr + ' bpm';
    if (p.spd != null) popup += '<br>Speed: '      + (p.spd * 3.6).toFixed(1) + ' km/h';
    marker.bindPopup(popup);
    layers.push(marker);
  });

  const startM = L.circleMarker(coords[0], {
    color: colour, radius: 7, fillOpacity: 0.9, weight: 2, fillColor: colour
  }).addTo(map);
  startM.bindPopup('<strong>Start</strong><br>' + fmtDate(data.first_timestamp));
  layers.push(startM);

  const endM = L.circleMarker(coords[coords.length - 1], {
    color: '#ffffff', radius: 5, fillOpacity: 0.9, weight: 2, fillColor: colour
  }).addTo(map);
  endM.bindPopup('<strong>End</strong>');
  layers.push(endM);

  selected.set(id, { colour, layers, trackData: data });

  const item = document.querySelector('.track-item[data-id="' + id + '"]');
  if (item) {
    item.classList.add('active'); item.style.setProperty('--track-color', colour);
    const currentSport = (data.meta && data.meta.sport) || 'unknown';
    item.appendChild(buildSportEditor(id, currentSport));
    item.appendChild(buildTrimPanel(id, data.point_count));
  }

  fitAll(); updateSelectionBar(); updateInfoPanel();
}

// ── Fit map to all selected tracks ────────────────────────────────────────
function fitAll() {
  if (selected.size === 0) return;
  const allLatLngs = [];
  selected.forEach(({ layers }) => {
    layers.forEach(l => {
      if (l.getLatLngs) allLatLngs.push(...l.getLatLngs().flat());
      else if (l.getLatLng) allLatLngs.push(l.getLatLng());
    });
  });
  if (allLatLngs.length) map.fitBounds(L.latLngBounds(allLatLngs), { padding: [40, 40] });
}

// ── Clear all ─────────────────────────────────────────────────────────────
function clearAll() {
  selected.forEach(({ layers }) => layers.forEach(l => map.removeLayer(l)));
  selected.clear();
  document.querySelectorAll('.track-item.active').forEach(el => {
    el.classList.remove('active'); el.style.removeProperty('--track-color');
    el.querySelector('.sport-editor')?.remove();
    el.querySelector('.trim-panel')?.remove();
  });
  updateSelectionBar(); updateInfoPanel();
}

// ── Render sidebar list ───────────────────────────────────────────────────
function renderList() {
  const list        = document.getElementById('track-list');
  const q           = document.getElementById('search').value.toLowerCase();
  const parkFilter  = document.getElementById('park-filter').value;
  const sportFilter = document.getElementById('sport-filter').value;

  filteredTracks = allTracks.filter(t => {
    if (showUnknownOnly && !isUnknownSport(t.sport)) return false;
    if (sportFilter && (t.sport || 'unknown').toLowerCase() !== sportFilter) return false;
    if (q && !(
      (t.first_timestamp || '').toLowerCase().includes(q) ||
      (t.sport           || '').toLowerCase().includes(q) ||
      (t.parks || []).some(p => p.name.toLowerCase().includes(q))
    )) return false;
    if (parkFilter === '__none__') return (t.parks || []).length === 0;
    if (parkFilter) return (t.parks || []).some(p =>
      (typeof p === 'object' ? p.name : p) === parkFilter
    );
    return true;
  });

  if      (currentSort === 'sport')    filteredTracks.sort((a, b) => (a.sport || '').localeCompare(b.sport || ''));
  else if (currentSort === 'distance') filteredTracks.sort((a, b) => (b.distance_km  || 0) - (a.distance_km  || 0));
  else if (currentSort === 'duration') filteredTracks.sort((a, b) => (b.duration_min || 0) - (a.duration_min || 0));
  else if (currentSort === 'points')   filteredTracks.sort((a, b) => (b.point_count  || 0) - (a.point_count  || 0));
  else                                 filteredTracks.sort((a, b) => (b.first_timestamp || '').localeCompare(a.first_timestamp || ''));

  list.innerHTML = '';
  if (!filteredTracks.length) {
    list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-dim)">No tracks found</div>';
    return;
  }

  filteredTracks.forEach(t => {
    const div = document.createElement('div');
    div.className = 'track-item' + (selected.has(t.id) ? ' active' : '');
    div.dataset.id = t.id;
    if (selected.has(t.id)) div.style.setProperty('--track-color', selected.get(t.id).colour);

    const sc        = (t.sport || 'unknown').toLowerCase();
    const parkPills = (t.parks || []).map(p => {
      const name   = typeof p === 'object' ? p.name   : p;
      const source = typeof p === 'object' ? p.source : 'custom';
      const icon   = source === 'national' ? '🍁' : source === 'provincial' ? '🌳' : '🌲';
      return '<span class="park-pill source-' + source + '">' + icon + ' ' + name + '</span>';
    }).join('');

    const correctedBadge = t.corrected
      ? '<span class="corrected-badge" style="font-size:9px;color:#ffaa00;margin-left:6px;" title="Manually corrected">✏️</span>'
      : '';

    div.innerHTML =
      '<div class="ts">' + fmtDate(t.first_timestamp) + correctedBadge +
        '<span class="sport-badge ' + sc + '">' + (t.sport || 'unknown') + '</span>' +
      '</div>' +
      '<div class="coords">' + fmtCoord(t.first_lat, t.first_lon) + '</div>' +
      '<div class="display-id" title="Filename (use with trim_track.py)">' + t.display_id + '</div>' +
      '<div class="stats">' +
        '<span>📍 ' + t.point_count + ' pts</span>' +
        (t.distance_km  != null ? '<span>↔ ' + fmtDist(t.distance_km)      + '</span>' : '') +
        (t.duration_min != null ? '<span>⏱ ' + t.duration_min + ' min</span>' : '') +
        (t.avg_hr       != null ? '<span>♥ '  + t.avg_hr      + ' bpm</span>' : '') +
      '</div>' +
      (parkPills ? '<div style="margin-top:4px">' + parkPills + '</div>' : '');

    div.onclick = () => toggleTrack(t.id);
    list.appendChild(div);

    if (selected.has(t.id)) {
      const trackData    = selected.get(t.id).trackData;
      const currentSport = (trackData.meta && trackData.meta.sport) || 'unknown';
      div.appendChild(buildSportEditor(t.id, currentSport));
      div.appendChild(buildTrimPanel(t.id, trackData.point_count));
    }
  });
}

// ── Controls ──────────────────────────────────────────────────────────────
document.querySelectorAll('.sort-btn').forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active'); currentSort = btn.dataset.sort; renderList();
  };
});
document.getElementById('btn-unknown').onclick = function() {
  showUnknownOnly = !showUnknownOnly;
  this.classList.toggle('active', showUnknownOnly);
  // Clear sport filter when toggling unknown-only, since they'd conflict
  if (showUnknownOnly) document.getElementById('sport-filter').value = '';
  renderList();
};
document.getElementById('search').addEventListener('input', renderList);
document.getElementById('park-filter').addEventListener('change', renderList);
document.getElementById('sport-filter').addEventListener('change', function() {
  // If a specific sport is selected, turn off the unknown-only toggle
  if (this.value) {
    showUnknownOnly = false;
    document.getElementById('btn-unknown').classList.remove('active');
  }
  renderList();
});
document.getElementById('btn-fit-all').onclick   = fitAll;
document.getElementById('btn-clear-sel').onclick = clearAll;

// ── Bootstrap ─────────────────────────────────────────────────────────────
async function init() {
  const fill  = document.getElementById('progress-bar-fill');
  const label = document.getElementById('loading-label');
  fill.style.width = '30%';

  const [tracksRes, parksRes] = await Promise.all([fetch('/api/tracks'), fetch('/api/parks')]);
  fill.style.width = '60%'; label.textContent = 'Parsing tracks…';

  allTracks = await tracksRes.json();
  const parkNames = await parksRes.json();
  fill.style.width = '85%'; label.textContent = 'Rendering…';

  await new Promise(r => setTimeout(r, 30));

  // Populate sport filter from actual track data
  populateSportFilter();

  // Populate park filter
  const parkSel = document.getElementById('park-filter');
  parkNames.forEach(p => {
    const name   = typeof p === 'object' ? p.name   : p;
    const source = typeof p === 'object' ? p.source : 'custom';
    const icon   = source === 'national' ? '🍁' : source === 'provincial' ? '🌳' : '🌲';
    const opt = document.createElement('option');
    opt.value = name; opt.textContent = icon + ' ' + name;
    parkSel.appendChild(opt);
  });

  // Hide the filter row entirely if there are no parks at all
  document.getElementById('filter-row').style.display =
    parkNames.length ? 'flex' : 'flex'; // always show (sport filter is always useful)

  renderList();
  fill.style.width = '100%';
  await new Promise(r => setTimeout(r, 250));
  document.getElementById('loading-wrap').style.display = 'none';

  const matched      = allTracks.filter(t => (t.parks || []).length > 0).length;
  const unknownCount = allTracks.filter(t => isUnknownSport(t.sport)).length;
  const countEl      = document.getElementById('track-count');
  countEl.textContent =
    allTracks.length + ' track' + (allTracks.length !== 1 ? 's' : '') + ' found' +
    (matched      ? ' · ' + matched      + ' in a park'    : '') +
    (unknownCount ? ' · ' + unknownCount + ' unknown sport' : '');
  countEl.style.display = '';
}

init();
</script>
</body>
</html>
"""


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="FIT Track Viewer")
    parser.add_argument(
        "--subfolder", default=None, metavar="SUBFOLDER",
        help="Restrict viewer to a specific subfolder of the tracks directory (e.g. '2021-05')",
    )
    args = parser.parse_args()

    if args.subfolder:
        TRACKS_DIR = TRACKS_DIR / args.subfolder
        if not TRACKS_DIR.exists():
            print(f"ERROR: Subfolder not found: {TRACKS_DIR}")
            sys.exit(1)

    print()
    print("  FIT Track Viewer")
    print("  " + "-" * 40)
    print(f"  Tracks folder : {TRACKS_DIR}")
    if args.subfolder:
        print(f"  Subfolder     : {args.subfolder}")
    print(f"  Open browser  : http://{HOST}:{PORT}")
    print("  Stop server   : Ctrl+C")
    print()
    app.run(host=HOST, port=PORT, debug=False)
