# Analyzing Recreational GPS Activity Within Park Boundaries Using Python and Geospatial Analytics

A Python data pipeline that parses Garmin `.fit` files, intersects GPS tracks with Canadian park boundary data, and feeds a Power BI dashboard for analyzing outdoor activity patterns by park, activity type, and season.

---

## The Problem

Garmin Connect makes it easy to review individual activities, but it doesn't provide a way to answer broader questions like: *how many times have I visited a specific park this year?* or *does my park usage vary by season?*

This project was built to answer those questions. The core objective: map GPS-enabled activity recordings from a Garmin fitness watch to park boundary data, so that activities can be grouped, filtered, and analyzed by park.

**Business questions driving the analysis:**
- How do activity frequency, duration, and distance vary across parks, activity types, and seasons?
- What seasonal trends emerge in park usage and activity intensity?
- Which parks see the most visits, and how does this vary by activity type and season?

---

## How This Project Evolved

This project was not built in one pass. It evolved through three distinct phases, each building on the last.

**Phase 1 — Proof of Concept**
Before building a full pipeline, a [proof-of-concept repository](https://github.com/ITNurse/garmin_project_POC) was created to validate the core spatial question: can GPS points from a `.fit` file be reliably intersected with park boundary polygons using open data and Python? The POC used both New Brunswick and Ontario synthetic data, and confirmed the approach was viable. This gave confidence to invest in a full pipeline.

**Phase 2 — Full Pipeline (Personal Data, New Brunswick)**
The complete pipeline was developed and validated against real personal Garmin activity data from New Brunswick. This version lives locally and is intentionally excluded from version control. Personal GPS traces, precise timestamps, and any visualizations derived from real movement data are not shared publicly.

**Phase 3 — Public Repository (Synthetic Data, Ontario)**
To make the project shareable without exposing personal location data, a synthetic dataset was generated using trail geometries sourced from OpenStreetMap via the Overpass API, covering parks in Ontario and Quebec. Tracks were re-dated using per-trail scheduling scripts to simulate a realistic multi-year activity history. The public repository ([garmin_project](https://github.com/ITNurse/garmin_project)) runs the identical pipeline against this synthetic data.

---

## Design Principles

**Privacy by Design**
Raw GPS traces and precise timestamps are not shared publicly or stored in version control.

**Data Minimisation**
Only the data necessary to answer the business questions is retained in final outputs.

**Separation of Code and Data**
Raw activity files are stored locally and excluded from Git via `.gitignore`.

**Reproducibility Without Exposure**
The pipeline is fully documented so it can be reproduced without distributing personal location data.

**Ethical Data Handling**
This project acknowledges the safety risks associated with public exposure of personal movement data and is designed to mitigate those risks throughout.

---

## Tech Stack

| Component | Tool |
|---|---|
| Language | Python |
| IDE | Visual Studio Code |
| Key libraries | `fitparse`, `geopandas`, `shapely`, `flask`, `matplotlib`, `contextily` |
| Activity data | Garmin full account export (`.fit` files) |
| Park boundary data | New Brunswick Open Data (WKT/CSV), Government of Canada CLAB (Shapefile), custom hand-drawn boundaries (GeoJSON via geojson.io) |
| Visualization | Power BI |

---

## Data Sources

| Dataset | Source | Format |
|---|---|---|
| Personal GPS activity tracks | Garmin account export via [Garmin Account Management Centre](https://support.garmin.com/en-CA/?faq=W1TvTPW8JZ6LfJSfK512Q8) | `.fit` |
| NB provincial park boundaries | [New Brunswick Open Data Portal](https://gnb.socrata.com/GeoNB/Provincial-Parks-Parcs-provinciaux/ixbz-22zx) | WKT/CSV |
| National park boundaries | [CLAB — Canada Lands Administrative Boundaries](https://open.canada.ca/data/en/dataset/ba1c1246-a9b1-4a8d-b0d0-37c9ca0d1e03) | Shapefile |
| Municipal and private park boundaries | Hand-drawn using [geojson.io](https://geojson.io) | GeoJSON |

---

## Pipeline Overview

### Stage 1 — Fit File Profiling

Before any conversion, `01_profile_fit_files.py` scans the full `.fit` archive and produces an Excel report listing every field with its data type and an example value. Critically, it identifies which files contain GPS coordinates — reducing the downstream conversion workload from ~15,000 files to ~1,400. It also flags edge cases such as unusually long activities (e.g. a watch left recording during a drive).

Profiling before building the conversion pipeline meant the conversion script was written with a clear picture of the data it would be processing.

### Stage 2 — Fit File Conversion

`01_preprocess_fit_files.py` parses each GPS-bearing `.fit` file into a `.track.json` file containing timestamped GPS points, plus metadata extracted directly from the `.fit` file: activity type, total distance, and total elapsed time.

### Stage 3 — Data Quality Review

`view_tracks.py` provides a Flask-based interactive map viewer for visually validating converted tracks against the Garmin portal. Where activities were recorded past their actual endpoint, `trim_track.py` allows manual trimming to the correct endpoint. `recalculate_metadata.py` updates distance and elapsed time after trimming. All corrections are logged and reviewable via `list_corrections.py`.

### Stage 4 — Park Boundary Preprocessing

Park boundary data arrives in multiple formats depending on source. The `02_preprocess_*` scripts normalize all sources to GeoJSON. Three formats are supported: WKT/CSV (New Brunswick Open Data), Shapefiles (federal CLAB data), and hand-drawn GeoJSON (municipal and private parks). `view_park_boundaries.py` provides visual inspection of any boundary file before use.

### Stage 5 — Park Matching

`03_match_geojson_parks.py` loops through all `.track.json` files and tests each GPS point against the loaded park boundary polygons, assigning a park name and park type to each track where a spatial match is found. The updated `view_tracks.py` includes park assignment as a filter in the web UI, enabling validation that tracks are correctly associated with their parks.

### Stage 6 — Power BI Report

The final output is an interactive Power BI report that connects directly to the folder of `.track.json` files. It includes activity count, average distance, and average duration by year-month (compared to the prior year), activity count by park, and filtering by park type, park name, activity type, distance, year, and month.

---

## Challenges and Lessons Learned

### 1. Profile before you build

The first instinct when faced with ~15,000 unknown binary files might be to start converting immediately. Instead, a profiling script was built first. This revealed that only ~1,400 files contained GPS data, identified the full field schema, and surfaced a significant data quality issue (see below) — all before any conversion code was written. The payoff was a conversion pipeline built on accurate assumptions rather than discovered mid-run.

### 2. .fit files are not .gpx files

The Garmin data export contains a `.fit` file for every recorded activity — a proprietary binary format that is not human-readable and cannot be opened with standard tools. The expectation going in was `.gpx` files (familiar from Strava exports). `fitparse` was the key library for parsing these files, but understanding which fields were available and how they were structured required experimentation. Not all `.fit` files contain GPS data — indoor activities such as strength training generate `.fit` files with no location points at all.

### 3. What the app shows is not always what's in the file

Profiling surfaced the `.fit` file with the highest GPS point count. Looking it up revealed the reason: the watch had been left recording through a long drive after the activity ended. The activity had been trimmed in Garmin Connect's portal — and appeared trimmed there — but the underlying `.fit` file retained every point in full.

This finding drove the entire data quality stage of the pipeline: `view_tracks.py` for visual validation, `trim_track.py` for correcting affected files, and `recalculate_metadata.py` for updating derived fields after trimming.

> **The lesson: source system UI ≠ underlying raw data.** What an application displays and what its raw files contain are not guaranteed to be the same thing.

### 4. Joining GPS files to activity metadata required more investigation than expected

`.fit` files do not contain an activity ID field, and the numeric filenames do not correspond to the activity IDs in the metadata file. An initial approach attempted a timestamp join in Power Query using a 120-second buffer to accommodate minor timestamp misalignments — and this worked, but was more complex than necessary.

Later investigation revealed that joining on the timestamp of the first GPS-bearing point (rather than simply the first timestamp in the file) would have produced a clean exact match without any buffer. The metadata join was ultimately rendered unnecessary when the conversion script was updated to extract activity type, distance, and elapsed time directly from the `.fit` file itself.

The buffer join approach was a useful exercise — it demonstrated that approximate joins on temporal data are achievable in Power Query — but the cleaner solution was available in the source data all along.

### 5. Open park boundary data comes in multiple formats

New Brunswick provincial park boundaries were available as WKT coordinates in a CSV file. Federal national park boundaries required downloading a Shapefile (`.shp`, `.shx`, `.dbf`). Municipal and private parks — which account for a significant proportion of actual activity locations — were not available in any open dataset and had to be created manually using [geojson.io](https://geojson.io).

Each format required a dedicated preprocessing script to normalize to GeoJSON before the park matching stage could run against a consistent input.

### 6. Making the project public required building a second dataset

Personal GPS data carries real privacy and safety implications. Making the project publicly available on GitHub while keeping personal movement data private required generating a synthetic dataset that preserved the full structure and behaviour of the pipeline.

Trail geometries were sourced from OpenStreetMap via the Overpass API for parks in Ontario and Quebec. Individual track files were then re-dated using per-trail scheduling scripts to simulate a realistic multi-year activity history with varied activity types and seasonal patterns. This was non-trivial work — but it produced a public repository that demonstrates the complete pipeline without exposing any personal location data.

### 7. The project scope evolved as understanding deepened

Several components were built, then superseded by better approaches discovered later. The metadata timestamp join was replaced by extracting fields directly from `.fit` files. The initial conversion script was rewritten once profiling had clarified which files needed processing. This is a natural part of exploratory technical work — earlier approaches were not wasted effort, they were how the better approaches were found.

---

## Repository Structure

```
garmin_project/
|-- config.py
|-- requirements.txt
|-- SETUP.md
|
|-- docs/
|   |-- architecture.md
|   +-- data_dictionary.md
|
|-- src/
|   |-- pipeline/
|   |   |-- 01_preprocess_fit_files.py
|   |   |-- 02_preprocess_wkt_boundaries.py
|   |   |-- 02_preprocess_shapefile_boundaries.py
|   |   +-- 03_match_geojson_parks.py
|   |
|   |-- profiling/
|   |   |-- 01_profile_fit_files.py
|   |   |-- 02_profile_shapefile_boundaries.py
|   |   +-- 03_profile_wkt_boundaries.py
|   |
|   |-- quality/
|   |   |-- trim_track.py
|   |   |-- recalculate_metadata.py
|   |   +-- list_corrections.py
|   |
|   |-- validation/
|   |   |-- view_tracks.py
|   |   +-- view_park_boundaries.py
|   |
|   +-- synthetic_data_creation/
|       |-- restamp_track_batch.py
|       +-- [trail]_schedule.csv
|
+-- reports/
    |-- garmin_geo_analysis.pbix
    +-- [images]
```

---

## Related

- [garmin_project_POC](https://github.com/ITNurse/garmin_project_POC) — proof of concept validating the core track-to-park spatial matching logic.
- [garmin_project](https://github.com/ITNurse/garmin_project) — full pipeline with synthetic Ontario dataset.
