#!/usr/bin/env python3
"""
view_park_boundaries.py
-----------------------
Interactive web viewer for all GeoJSON park boundary files in config.PARKS_DIR.
Renders park polygons on a map with a filterable sidebar.

Usage:
    python scripts/view_park_boundaries.py

    Then open http://127.0.0.1:5001 in your browser.
    Press Ctrl+C in the terminal to stop the server.

Requirements:
    pip install flask
"""

import sys
import json
import argparse
from pathlib import Path
from flask import Flask, jsonify, render_template_string

# =============================================================================
# PROJECT ROOT
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config

# =============================================================================
# ARGUMENT PARSING
# =============================================================================

parser = argparse.ArgumentParser(description="Interactive park boundary viewer")
parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
parser.add_argument("--port", type=int, default=5001, help="Port to bind (default: 5001)")
args = parser.parse_args()

HOST = args.host
PORT = args.port

PARKS_DIR = Path(config.PARKS_DIR)
if not PARKS_DIR.exists():
    print(f"ERROR: PARKS_DIR not found: {PARKS_DIR}")
    sys.exit(1)


# =============================================================================
# PARK LOADING
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
            props.get("name") or
            props.get("NAME") or
            props.get("park_name") or
            props.get("PARK_NAME") or
            props.get("NAME_E") or
            props.get("NAMEEN") or
            props.get("label") or
            props.get("LABEL") or
            f"Park {id_offset + i + 1}"
        )

        source   = path.stem  # use filename (without extension) as the source label
        area_ha  = props.get("area_ha") or props.get("AREA_HA") or props.get("Shape_Area")
        province = props.get("province") or props.get("PROVINCE") or props.get("prov") or props.get("PROV") or ""

        parks.append({
            "id":         id_offset + i,
            "name":       str(name).strip(),
            "source":     source,
            "area_ha":    round(float(area_ha), 1) if area_ha else None,
            "province":   province,
            "geometry":   geom,
            "properties": {k: v for k, v in props.items()},
        })

    return parks


def load_all_parks() -> list[dict]:
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


print(f"\nLoading parks from: {PARKS_DIR}")
ALL_PARKS = load_all_parks()
print(f"Total: {len(ALL_PARKS)} parks loaded\n")


# =============================================================================
# FLASK APP
# =============================================================================

app = Flask(__name__)


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/parks")
def api_parks():
    """Return sidebar summaries (no geometry)."""
    return jsonify([
        {
            "id":       p["id"],
            "name":     p["name"],
            "source":   p["source"],
            "area_ha":  p["area_ha"],
            "province": p["province"],
        }
        for p in ALL_PARKS
    ])


@app.route("/api/park/<int:park_id>")
def api_park(park_id):
    """Return full park data including geometry."""
    # IDs are sequential offsets — find by id field
    park = next((p for p in ALL_PARKS if p["id"] == park_id), None)
    if park is None:
        return jsonify({"error": "Park not found"}), 404
    return jsonify(park)


@app.route("/api/sources")
def api_sources():
    """Return sorted list of unique source file names."""
    seen = sorted({p["source"] for p in ALL_PARKS if p["source"]})
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
    --sidebar-w: 320px;
  }
  body {
    font-family: 'Space Mono', monospace;
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    overflow: hidden;
  }
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
  #sidebar-header {
    padding: 16px 14px 12px;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }
  #sidebar-header h1 {
    font-size: 14px;
    font-weight: 700;
    letter-spacing: 0.5px;
    color: var(--accent);
    text-transform: uppercase;
  }
  #sidebar-header h1 span.dot {
    display: inline-block;
    width: 6px; height: 6px;
    background: var(--accent);
    border-radius: 50%;
    margin-left: 6px;
    vertical-align: middle;
  }
  #park-count { font-size: 10px; color: var(--text-dim); margin-top: 6px; letter-spacing: 0.3px; }
  #selection-bar {
    display: none;
    align-items: center;
    padding: 6px 14px;
    background: #1e2940;
    border-bottom: 1px solid #2a3f6a;
    font-size: 10px;
    color: var(--accent-alt);
    flex-shrink: 0;
    gap: 6px;
  }
  #selection-bar.visible { display: flex; }
  #sel-count { flex: 1; }
  #selection-bar button {
    background: none;
    border: 1px solid currentColor;
    border-radius: 4px;
    color: inherit;
    font-family: 'Space Mono', monospace;
    font-size: 9px;
    padding: 3px 7px;
    cursor: pointer;
    white-space: nowrap;
  }
  #selection-bar button:hover { background: rgba(0,153,255,0.15); }
  #btn-clear-sel { color: var(--text-dim) !important; border-color: var(--text-dim) !important; }
  #search {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    padding: 8px 12px;
    margin-top: 12px;
    outline: none;
    transition: border-color 0.2s;
  }
  #search:focus { border-color: var(--accent); }
  #search::placeholder { color: var(--text-dim); }
  #sort-bar { display: flex; gap: 6px; padding: 12px 14px 6px; flex-shrink: 0; }
  .sort-btn {
    font-family: 'Space Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.3px;
    padding: 5px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: transparent;
    color: var(--text-dim);
    cursor: pointer;
    transition: all 0.2s;
    text-transform: uppercase;
    flex: 1;
  }
  .sort-btn:hover { background: var(--bg); border-color: var(--accent); color: var(--text); }
  .sort-btn.active { background: var(--accent); color: var(--bg); border-color: var(--accent); }
  #source-filter-wrap { padding: 6px 14px 0; }
  #source-filter {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-family: 'Space Mono', monospace;
    font-size: 11px;
    padding: 7px 10px;
    outline: none;
    cursor: pointer;
    transition: border-color 0.2s;
    appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%235a6478'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 10px center;
    padding-right: 28px;
  }
  #source-filter:focus { border-color: var(--accent); }
  #source-filter option { background: var(--surface); }
  #park-list { flex: 1; overflow-y: auto; padding: 6px; }
  .park-item {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px 12px;
    margin-bottom: 6px;
    cursor: pointer;
    transition: all 0.15s;
    font-size: 11px;
    position: relative;
  }
  .park-item:hover { background: #1f2229; }
  .park-item.active { background: rgba(0,229,160,0.06); padding-left: 16px; }
  .park-item.active::before {
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 4px;
    border-radius: 6px 0 0 6px;
    background: var(--park-color, var(--accent));
  }
  .park-item .park-name {
    font-weight: 600;
    color: var(--text);
    margin-bottom: 4px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .source-badge {
    font-size: 9px;
    letter-spacing: 0.5px;
    padding: 2px 6px;
    border-radius: 3px;
    text-transform: uppercase;
    font-weight: 600;
    background: rgba(0,229,160,0.15);
    color: var(--accent);
    flex-shrink: 0;
    margin-left: 6px;
    max-width: 100px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .park-item .park-meta { font-size: 10px; color: var(--text-dim); display: flex; gap: 10px; margin-top: 4px; }
  #map-container { margin-left: var(--sidebar-w); height: 100vh; position: relative; }
  #map { width: 100%; height: 100%; }
  #empty {
    position: absolute; top: 50%; left: 50%;
    transform: translate(-50%, -50%);
    text-align: center; color: var(--text-dim); font-size: 13px; pointer-events: none;
  }
  #info-panel {
    position: absolute; top: 20px; right: 20px;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 0; width: 300px;
    max-height: calc(100vh - 80px); overflow-y: auto;
    font-size: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); display: none;
  }
  #info-panel.visible { display: block; }
  .info-card { padding: 14px 16px; border-bottom: 1px solid var(--border); }
  .info-card:last-child { border-bottom: none; }
  .info-card-title {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 8px;
    display: flex; align-items: center; gap: 8px;
  }
  .info-swatch { display: inline-block; width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .info-card .row {
    display: flex; justify-content: space-between; margin-bottom: 4px;
    padding: 3px 0; border-bottom: 1px solid rgba(255,255,255,0.05); word-break: break-word;
  }
  .info-card .row:last-child { border: none; }
  .info-card .label { color: var(--text-dim); flex-shrink: 0; margin-right: 8px; }
  .info-card .value { color: var(--text); font-weight: 600; text-align: right; }
  .leaflet-popup-content-wrapper {
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 6px;
    font-family: 'Space Mono', monospace; font-size: 11px;
  }
  .leaflet-popup-content { margin: 10px 12px; line-height: 1.6; }
  .leaflet-popup-tip { background: var(--surface); }
  .leaflet-popup-content strong { color: var(--accent); }
</style>
</head>
<body>

<div id="sidebar">
  <div id="sidebar-header">
    <h1>Park Boundaries<span class="dot"></span></h1>
    <div id="park-count">Loading...</div>
    <input type="text" id="search" placeholder="Search parks..." />
  </div>
  <div id="selection-bar">
    <span id="sel-count"></span>
    <button id="btn-fit-all">Fit map</button>
    <button id="btn-clear-sel">Clear</button>
  </div>
  <div id="sort-bar">
    <button class="sort-btn active" data-sort="name">Name</button>
    <button class="sort-btn" data-sort="area">Area</button>
    <button class="sort-btn" data-sort="source">Source</button>
  </div>
  <div id="source-filter-wrap" style="display:none">
    <select id="source-filter">
      <option value="">🌲 All files</option>
    </select>
  </div>
  <div id="park-list"></div>
</div>

<div id="map-container">
  <div id="map"></div>
  <div id="empty"><p>Click parks in the sidebar to show them on the map.<br><small style="color:var(--text-dim)">Click again to deselect.</small></p></div>
  <div id="info-panel"></div>
</div>

<script>
const map = L.map('map').setView([56.0, -96.0], 4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© OpenStreetMap'
}).addTo(map);

const PARK_COLOURS = [
  '#00e5a0','#0099ff','#ff6464','#ffb464','#c864ff',
  '#ff64c8','#64d2ff','#a0ff64','#ffd700','#ff8c00',
];
let colourCounter = 0;
function nextColour() { return PARK_COLOURS[colourCounter++ % PARK_COLOURS.length]; }

let allParks = [], filteredParks = [], currentSort = 'name';
const selected = new Map();

function fmtArea(ha) {
  if (ha == null) return null;
  if (ha >= 10000) return (ha / 10000).toFixed(1) + ' Mha';
  if (ha >= 1000)  return (ha / 1000).toFixed(1) + ' kha';
  return ha.toFixed(1) + ' ha';
}

function updateSelectionBar() {
  const bar = document.getElementById('selection-bar');
  const n = selected.size;
  if (n === 0) { bar.classList.remove('visible'); }
  else {
    bar.classList.add('visible');
    document.getElementById('sel-count').textContent = n + ' park' + (n !== 1 ? 's' : '') + ' selected';
  }
}

function updateInfoPanel() {
  const panel = document.getElementById('info-panel');
  if (selected.size === 0) {
    panel.className = ''; panel.innerHTML = '';
    document.getElementById('empty').style.display = '';
    return;
  }
  document.getElementById('empty').style.display = 'none';
  panel.className = 'visible';
  panel.innerHTML = '';
  selected.forEach(({ colour, parkData }) => {
    const card = document.createElement('div');
    card.className = 'info-card';
    const skip = new Set(['geometry', 'type', 'id']);
    const propRows = Object.entries(parkData.properties || {})
      .filter(([k]) => !skip.has(k.toLowerCase()))
      .slice(0, 12)
      .map(([k, v]) =>
        '<div class="row"><span class="label">' + k + '</span>' +
        '<span class="value">' + (v != null ? String(v) : '—') + '</span></div>'
      ).join('');
    card.innerHTML =
      '<div class="info-card-title"><span class="info-swatch" style="background:' + colour + '"></span>' + parkData.name + '</div>' +
      '<div class="row"><span class="label">File</span><span class="value">' + parkData.source + '</span></div>' +
      (parkData.area_ha != null ? '<div class="row"><span class="label">Area</span><span class="value">' + fmtArea(parkData.area_ha) + '</span></div>' : '') +
      (parkData.province ? '<div class="row"><span class="label">Province</span><span class="value">' + parkData.province + '</span></div>' : '') +
      propRows;
    panel.appendChild(card);
  });
}

function geomToLeafletLayers(geom, colour) {
  const layers = [];
  if (!geom || !geom.type) return layers;
  const style = { color: colour, weight: 2, opacity: 0.9, fillColor: colour, fillOpacity: 0.18 };
  function addPolygon(rings) {
    const latlngs = rings.map(ring => ring.map(([lon, lat]) => [lat, lon]));
    layers.push(L.polygon(latlngs, style));
  }
  if (geom.type === 'Polygon') addPolygon(geom.coordinates);
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

function layerBounds(layers) {
  const pts = [];
  layers.forEach(l => {
    if (l.getLatLngs) l.getLatLngs().flat(Infinity).forEach(ll => pts.push(ll));
    else if (l.getLatLng) pts.push(l.getLatLng());
  });
  return pts;
}

function fitAll() {
  if (selected.size === 0) return;
  const all = [];
  selected.forEach(({ layers }) => all.push(...layerBounds(layers)));
  if (all.length) map.fitBounds(L.latLngBounds(all), { padding: [40, 40] });
}

function clearAll() {
  selected.forEach(({ layers }) => layers.forEach(l => map.removeLayer(l)));
  selected.clear(); colourCounter = 0;
  document.querySelectorAll('.park-item.active').forEach(el => {
    el.classList.remove('active'); el.style.removeProperty('--park-color');
  });
  updateSelectionBar(); updateInfoPanel();
}

async function togglePark(id) {
  if (selected.has(id)) {
    selected.get(id).layers.forEach(l => map.removeLayer(l));
    selected.delete(id);
    const item = document.querySelector('.park-item[data-id="' + id + '"]');
    if (item) { item.classList.remove('active'); item.style.removeProperty('--park-color'); }
    updateSelectionBar(); updateInfoPanel(); fitAll();
    return;
  }
  const res  = await fetch('/api/park/' + id);
  const data = await res.json();
  if (data.error) { alert(data.error); return; }
  const colour = nextColour();
  const layers = geomToLeafletLayers(data.geometry, colour);
  if (layers.length === 0) { alert('No renderable geometry for: ' + data.name); return; }
  layers.forEach(l => {
    l.addTo(map);
    l.bindPopup('<strong>' + data.name + '</strong>' +
      '<br>File: ' + data.source +
      (data.province ? '<br>Province: ' + data.province : '') +
      (data.area_ha  ? '<br>Area: ' + fmtArea(data.area_ha) : ''));
  });
  selected.set(id, { colour, layers, parkData: data });
  const item = document.querySelector('.park-item[data-id="' + id + '"]');
  if (item) { item.classList.add('active'); item.style.setProperty('--park-color', colour); }
  fitAll(); updateSelectionBar(); updateInfoPanel();
}

function renderList() {
  const list = document.getElementById('park-list');
  const q    = document.getElementById('search').value.toLowerCase();
  const src  = document.getElementById('source-filter').value;

  filteredParks = allParks.filter(p => {
    if (q && !(p.name || '').toLowerCase().includes(q) &&
             !(p.source   || '').toLowerCase().includes(q) &&
             !(p.province || '').toLowerCase().includes(q)) return false;
    if (src && p.source !== src) return false;
    return true;
  });

  if (currentSort === 'area')   filteredParks.sort((a, b) => (b.area_ha || 0) - (a.area_ha || 0));
  else if (currentSort === 'source') filteredParks.sort((a, b) => (a.source || '').localeCompare(b.source || '') || (a.name || '').localeCompare(b.name || ''));
  else filteredParks.sort((a, b) => (a.name || '').localeCompare(b.name || ''));

  list.innerHTML = '';
  if (!filteredParks.length) {
    list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-dim)">No parks found</div>';
    return;
  }
  filteredParks.forEach(p => {
    const div = document.createElement('div');
    div.className = 'park-item' + (selected.has(p.id) ? ' active' : '');
    div.dataset.id = p.id;
    if (selected.has(p.id)) div.style.setProperty('--park-color', selected.get(p.id).colour);
    const badge = p.source ? '<span class="source-badge">' + p.source + '</span>' : '';
    const meta = [];
    if (p.area_ha  != null) meta.push('📐 ' + fmtArea(p.area_ha));
    if (p.province)          meta.push('📍 ' + p.province);
    div.innerHTML =
      '<div class="park-name">' + (p.name || 'Unnamed') + badge + '</div>' +
      (meta.length ? '<div class="park-meta">' + meta.join('') + '</div>' : '');
    div.onclick = () => togglePark(p.id);
    list.appendChild(div);
  });
}

document.querySelectorAll('.sort-btn').forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentSort = btn.dataset.sort;
    renderList();
  };
});
document.getElementById('search').addEventListener('input', renderList);
document.getElementById('source-filter').addEventListener('change', renderList);
document.getElementById('btn-fit-all').onclick  = fitAll;
document.getElementById('btn-clear-sel').onclick = clearAll;

async function init() {
  const [parksRes, sourcesRes] = await Promise.all([fetch('/api/parks'), fetch('/api/sources')]);
  allParks = await parksRes.json();
  const sources = await sourcesRes.json();

  const sel = document.getElementById('source-filter');
  if (sources.length > 1) {
    sources.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s; opt.textContent = s;
      sel.appendChild(opt);
    });
    document.getElementById('source-filter-wrap').style.display = 'block';
  }

  document.getElementById('park-count').textContent =
    allParks.length + ' park' + (allParks.length !== 1 ? 's' : '') +
    ' from ' + sources.length + ' file' + (sources.length !== 1 ? 's' : '');

  renderList();
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
    print("  Park Boundary Viewer")
    print("  " + "-" * 40)
    print(f"  Folder  : {PARKS_DIR}")
    print(f"  Parks   : {len(ALL_PARKS)} features loaded")
    print(f"  Browser : http://{HOST}:{PORT}")
    print("  Stop    : Ctrl+C\n")
    app.run(host=HOST, port=PORT, debug=False)
