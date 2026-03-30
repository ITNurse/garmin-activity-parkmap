#!/usr/bin/env python3
"""
view_parks.py
-------------
Interactive web viewer for park boundaries.

Parks are displayed in the sidebar with search, sorting, and filtering by
source file, province, and park type. Click a park to highlight it on the
map; click again to deselect.

Usage:
    python scripts/view_parks.py

    Then open http://127.0.0.1:5000 in your browser.
    Press Ctrl+C in the terminal to stop the server.

Requirements:
    pip install flask
"""

import sys
import json
from pathlib import Path
from flask import Flask, jsonify, render_template_string

# =============================================================================
# PROJECT ROOT & CONFIG
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import config
    HOST = config.VIEWER_HOST
    PORT = config.VIEWER_PORT
except ImportError:
    print("ERROR: config.py not found.")
    print(f"  Expected location: {PROJECT_ROOT / 'config.py'}")
    sys.exit(1)

PARKS_DIR = Path(config.PARKS_DIR)

# =============================================================================
# FLASK APP
# =============================================================================

app = Flask(__name__)

if not PARKS_DIR.exists():
    print(f"Warning: Parks folder not found: {PARKS_DIR}")


# =============================================================================
# PARK TYPE ACRONYM EXPANSIONS
# Keys are upper-cased for case-insensitive matching.
# Add new per-source dictionaries here as needed.
# =============================================================================

ALBERTA_PARK_TYPES: dict[str, str] = {
    "ER":  "Ecological Reserve",
    "HR":  "Heritage Rangeland",
    "NA":  "Natural Area",
    "NP":  "National Park",
    "PP":  "Provincial Park",
    "PRA": "Provincial Recreation Area",
    "WA":  "Wilderness Area",
    "WP":  "Wilderness Park",
    "WPP": "Wildland",
}

# =============================================================================
# PARK BOUNDARY LOADING
# =============================================================================

def load_parks_from_file(path: Path, id_offset: int) -> list[dict]:
    """Load and normalise park features from a single GeoJSON file."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"  WARNING: Could not read {path.name}: {e}")
        return []

    if data.get("type") == "FeatureCollection":
        features = data.get("features", [])
    elif data.get("type") == "Feature":
        features = [data]
    elif isinstance(data, list):
        features = data
    else:
        print(f"  WARNING: Unrecognised GeoJSON structure in {path.name}")
        return []

    parks = []
    for i, feat in enumerate(features):
        if not isinstance(feat, dict):
            continue
        props = feat.get("properties") or {}
        geom  = feat.get("geometry") or {}

        name = (
            props.get("PROTECTED_AREA_NAME_ENG") or
            props.get("PROTECTED_LANDS_NAME") or
            props.get("Pro_Name") or
            props.get("name") or
            props.get("NAME") or
            props.get("park_name") or
            props.get("PARK_NAME") or
            props.get("PARKNM") or
            props.get("NAME_E") or
            props.get("NAMEEN") or
            props.get("label") or
            props.get("LABEL") or
            props.get("TOPONYME") or
            f"Park {id_offset + i + 1}"
        )

        # Extract park type from common field names
        park_type = (
            props.get("PARK_TYPE") or
            props.get("park_type") or
            props.get("PARKTYPE") or
            props.get("TYPE") or
            props.get("type") or
            props.get("TYPE_E") or
            props.get("TYPE_ENG") or
            props.get("PROTECTED_AREA_TYPE") or
            props.get("PROTECTED_LANDS_DESIGNATION") or
            props.get("IUCN_CAT") or
            props.get("iucn_cat") or
            props.get("DESIGNATION") or
            props.get("designation") or
            props.get("LAND_CLASS") or
            props.get("land_class") or
            props.get("MGMT_CLASS") or
            props.get("CLASS") or
            props.get("Protect1") or
            None
        )
        if park_type:
            park_type = str(park_type).strip()
            # Expand Alberta park type acronyms when the source file is an Alberta dataset
            if "alberta" in path.stem.lower():
                park_type = ALBERTA_PARK_TYPES.get(park_type.upper(), park_type)

        source   = path.stem
        area_ha  = props.get("area_ha") or props.get("HA_GIS") or props.get("AREA_HA") or props.get("Shape_Area")
        province = props.get("province") or props.get("PROVINCE") or props.get("prov") or props.get("PROV") or ""

        parks.append({
            "id":         id_offset + i,
            "name":       str(name).strip(),
            "source":     source,
            "park_type":  park_type,
            "area_ha":    round(float(str(area_ha).replace(",", "")), 1) if area_ha else None,
            "province":   province,
            "geometry":   geom,
            "properties": {k: v for k, v in props.items()},
        })

    return parks


def load_all_park_boundaries() -> list[dict]:
    if not PARKS_DIR.exists():
        return []
    geojson_files = sorted(PARKS_DIR.glob("*.geojson"))
    if not geojson_files:
        print(f"WARNING: No .geojson files found in {PARKS_DIR}")
        return []
    all_parks = []
    for path in geojson_files:
        parks = load_parks_from_file(path, id_offset=len(all_parks))
        print(f"  {path.name}: {len(parks)} features")
        all_parks.extend(parks)
    return all_parks


print(f"\nLoading park boundaries from: {PARKS_DIR}")
ALL_PARK_BOUNDARIES = load_all_park_boundaries()
print(f"Total: {len(ALL_PARK_BOUNDARIES)} park boundaries loaded\n")


# =============================================================================
# ROUTES
# =============================================================================

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/boundaries")
def api_boundaries():
    """Return sidebar summaries of park boundaries (no geometry)."""
    return jsonify([
        {
            "id":        p["id"],
            "name":      p["name"],
            "source":    p["source"],
            "park_type": p["park_type"],
            "area_ha":   p["area_ha"],
            "province":  p["province"],
        }
        for p in ALL_PARK_BOUNDARIES
    ])


@app.route("/api/boundary/<int:park_id>")
def api_boundary(park_id):
    """Return full park boundary data including geometry."""
    park = next((p for p in ALL_PARK_BOUNDARIES if p["id"] == park_id), None)
    if park is None:
        return jsonify({"error": "Park not found"}), 404
    return jsonify(park)


@app.route("/api/boundary-sources")
def api_boundary_sources():
    seen = sorted({p["source"] for p in ALL_PARK_BOUNDARIES if p["source"]})
    return jsonify(seen)


@app.route("/api/boundary-types")
def api_boundary_types():
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
  #empty {
    position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
    text-align: center; color: var(--text-dim); font-size: 13px; pointer-events: none;
  }

  /* ── Info panel (top-right) ── */
  #info-panel {
    position: absolute; top: 16px; right: 16px;
    background: var(--surface); border: 1px solid var(--border2);
    border-radius: 8px; padding: 0; width: 290px;
    max-height: calc(100vh - 80px); overflow-y: auto;
    font-size: 12px; box-shadow: 0 6px 20px rgba(0,0,0,0.4); display: none;
    z-index: 800;
  }
  #info-panel.visible { display: block; }
  .info-card { padding: 12px 14px; border-bottom: 1px solid var(--border); }
  .info-card:last-child { border-bottom: none; }
  .info-card-title {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 8px; display: flex; align-items: center; gap: 8px;
  }
  .info-swatch { display: inline-block; width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
  .info-card .row {
    display: flex; justify-content: space-between; margin-bottom: 3px;
    padding: 2px 0; border-bottom: 1px solid rgba(255,255,255,0.04); word-break: break-word;
  }
  .info-card .row:last-child { border: none; }
  .info-card .label { color: var(--text-dim); flex-shrink: 0; margin-right: 8px; }
  .info-card .value { color: var(--text); font-weight: 700; text-align: right; }

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
  <div id="empty">
    <p>Select a park boundary from the sidebar.<br>
    <small style="color:var(--text-dim)">Click again to deselect.</small></p>
  </div>
  <div id="info-panel"></div>
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
  updateInfoPanel();
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
    updateInfoPanel();
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
// INFO PANEL
// ═══════════════════════════════════════════════════════════════════
function updateInfoPanel() {
  const panel = document.getElementById('info-panel');
  if (selectedParks.size === 0) {
    panel.className = ''; panel.innerHTML = '';
    document.getElementById('empty').style.display = '';
    return;
  }
  document.getElementById('empty').style.display = 'none';
  panel.className = 'visible'; panel.innerHTML = '';

  selectedParks.forEach(({ colour, parkData }) => {
    const card = document.createElement('div');
    card.className = 'info-card';
    const skip = new Set(['geometry', 'type', 'id']);
    const propRows = Object.entries(parkData.properties || {})
      .filter(([k]) => !skip.has(k.toLowerCase()))
      .slice(0, 12)
      .map(([k, v]) =>
        '<div class="row"><span class="label">' + k + '</span><span class="value">' + (v != null ? String(v) : '—') + '</span></div>'
      ).join('');
    card.innerHTML =
      '<div class="info-card-title"><span class="info-swatch" style="background:' + colour + '"></span>' + (parkData.name || 'Unnamed') + '</div>' +
      '<div class="row"><span class="label">File</span><span class="value">' + parkData.source + '</span></div>' +
      (parkData.park_type ? '<div class="row"><span class="label">Type</span><span class="value">' + parkData.park_type + '</span></div>' : '') +
      (parkData.area_ha != null ? '<div class="row"><span class="label">Area</span><span class="value">' + fmtArea(parkData.area_ha) + '</span></div>' : '') +
      (parkData.province ? '<div class="row"><span class="label">Province</span><span class="value">' + parkData.province + '</span></div>' : '') +
      propRows;
    panel.appendChild(card);
  });
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
    updateParkSelBar(); updateInfoPanel(); fitParks(); return;
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
  fitParks(); updateParkSelBar(); updateInfoPanel();
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
document.querySelectorAll('.sort-btn[data-psort]').forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll('.sort-btn[data-psort]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active'); currentParkSort = btn.dataset.psort; renderParkList();
  };
});
document.getElementById('park-search').addEventListener('input', renderParkList);
document.getElementById('park-source-filter').addEventListener('change', renderParkList);
document.getElementById('park-type-filter').addEventListener('change', renderParkList);

// ═══════════════════════════════════════════════════════════════════
// BOOTSTRAP
// ═══════════════════════════════════════════════════════════════════
async function init() {
  const fill  = document.getElementById('progress-bar-fill');
  const label = document.getElementById('loading-label');
  fill.style.width = '20%';

  const [boundariesRes, sourcesRes, typesRes] = await Promise.all([
    fetch('/api/boundaries'),
    fetch('/api/boundary-sources'),
    fetch('/api/boundary-types'),
  ]);
  fill.style.width = '70%'; label.textContent = 'Parsing data…';

  allBoundaries = await boundariesRes.json();
  const sources = await sourcesRes.json();
  const types   = await typesRes.json();

  fill.style.width = '90%'; label.textContent = 'Rendering…';
  await new Promise(r => setTimeout(r, 30));

  // Populate source filter
  sources.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s;
    document.getElementById('park-source-filter').appendChild(opt);
  });

  // Populate park type filter
  types.forEach(t => {
    const opt = document.createElement('option');
    opt.value = t; opt.textContent = t;
    document.getElementById('park-type-filter').appendChild(opt);
  });

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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Park Boundary Viewer")
    parser.add_argument("--host", default=HOST, help=f"Host to bind (default: {HOST})")
    parser.add_argument("--port", type=int, default=PORT, help=f"Port to bind (default: {PORT})")
    args = parser.parse_args()

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
    app.run(host=HOST, port=PORT, debug=False)
