# -*- coding: utf-8 -*-
import re as _re

"""
THAW - Streamlit Dashboard Output preview page
Dr. Stefan Fugger
Created in Feb 2026
"""
import streamlit as st
import os
import glob
import numpy as np
import folium
import json
import csv
import subprocess
import sys
from datetime import datetime, timedelta
import rasterio
from rasterio.warp import transform_bounds, calculate_default_transform, reproject, Resampling
from rasterio.crs import CRS as RioCRS
from streamlit_folium import st_folium
from folium.plugins import MeasureControl, Draw, Fullscreen
from tracking_viewer import render_tracking_viewer
import matplotlib.pyplot as plt
import base64
from PIL import Image

# --- 1. Function Definitions ---
def load_gee_creds():
    """Reads stored GEE credentials from the temp file."""
    if os.path.exists(CRED_FILE):
        with open(CRED_FILE, "r") as f:
            lines = [line.strip() for line in f.readlines()]
            if len(lines) >= 2:
                return lines[0], lines[1]
    return None, None

def get_vis_params(filename):
    for key, vis in VIS_BY_LAYER.items():
        if key in filename:
            return vis
    return {'min': -30, 'max': 0, 'palette': 'gray'}

def make_combined_legend(layers_present, vis_by_layer):
    """Single semi-transparent box with one gradient bar per visible layer."""
    LAYER_META = {
        "z_score":         dict(title="Z-Score",        unit=""),
        "potential_water": dict(title="Potential Water", unit=""),
        "mean_diff":       dict(title="Mean Diff",       unit=" dB"),
    }
    steps = 5
    blocks = ""
    for key, meta in LAYER_META.items():
        if key not in layers_present:
            continue
        vis  = vis_by_layer[key]
        cmap = plt.get_cmap(vis["palette"])
        stops = ", ".join(
            "#{:02x}{:02x}{:02x}".format(
                int(cmap(k/steps)[0]*255),
                int(cmap(k/steps)[1]*255),
                int(cmap(k/steps)[2]*255),
            )
            for k in range(steps + 1)
        )
        blocks += (
            '<div style="margin-bottom:10px;">'
            f'<div style="font-size:12px;font-weight:bold;margin-bottom:3px;">{meta["title"]}</div>'
            f'<div style="height:12px;width:160px;background:linear-gradient(to right,{stops});'
            'border:1px solid #aaa;border-radius:2px;"></div>'
            '<div style="display:flex;justify-content:space-between;width:160px;">'
            f'<span style="font-size:10px;">{vis["min"]}{meta["unit"]}</span>'
            f'<span style="font-size:10px;">{(vis["min"]+vis["max"])/2:.1f}{meta["unit"]}</span>'
            f'<span style="font-size:10px;">{vis["max"]}{meta["unit"]}</span>'
            '</div></div>'
        )
    if not blocks:
        return None
    html = (
        '<div style="position:fixed;bottom:40px;left:50px;z-index:9999;'
        'background:rgba(255,255,255,0.82);border:1px solid #bbb;'
        'border-radius:8px;padding:10px 14px;font-family:Arial,sans-serif;'
        'pointer-events:none;min-width:190px;">'
        '<div style="font-size:13px;font-weight:bold;margin-bottom:8px;'
        'border-bottom:1px solid #ccc;padding-bottom:4px;">Legend</div>'
        + blocks +
        '</div>'
    )
    return folium.Element(html)


def write_timetrack_config(folder_path, aoi, start_date, end_date, selected_ids, proj_id, sa_path):
    """
    Saves config using relative paths and GEE auth info to ensure transferability.
    """
    cfg_path = os.path.join(CONFIG_DIR, "timetrack_config.json")
    
    # Convert the absolute folder_path to a path relative to ROOT_DIR
    rel_output_path = os.path.relpath(folder_path, ROOT_DIR)

    config_data = {
        "aoi_bbox": aoi,
        "start_date": start_date,
        "end_date": end_date,
        "cluster_ids": selected_ids,
        "rel_output_dir": rel_output_path, 
        "project_id": proj_id,
        "service_account_path": sa_path,
        "processed_at": datetime.now().isoformat()
    }
    
    with open(cfg_path, "w") as f:
        json.dump(config_data, f, indent=4)
    
    return cfg_path

# --- 2. Directory & Auth Setup ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DASH_DIR = os.path.dirname(CURRENT_DIR)                 
ROOT_DIR = os.path.dirname(DASH_DIR)                    
TEMP_DIR = os.path.join(ROOT_DIR, "temp")
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")
GEE_DIR = os.path.join(ROOT_DIR, "GEE")
OUTPUT_DIR = os.path.join(ROOT_DIR, "Outputs")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
os.makedirs(CONFIG_DIR, exist_ok=True)

# Load GEE Credentials (Same as Scheduler)
project_id, service_account_path = load_gee_creds()

# --- 3. Page Configuration ---
st.set_page_config(layout="wide", page_title="Output Preview")

# Auth Check
if not project_id:
    st.error("**No Credentials Found.** Please go to the **Home** page and log in first.")
    st.stop()

st.markdown(
    """
    <style>
    .reportview-container .main .block-container {
        max-width: 1100px;
        padding-top: 2rem;
    }
    [data-testid="stDataFrame"] {
        width: 1100px !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- 4. Visualization & Data Discovery ---
VIS_BY_LAYER = {
    "z_score": {"min": -2, "max": 2, "palette": "RdYlGn"},
    "potential_water": {"min": 0, "max": 1, "palette": "Blues"},
    "mean_diff": {"min": -5, "max": 5, "palette": "RdBu"},
}

output_folders = glob.glob(os.path.join(OUTPUT_DIR, "Outputs_*"))
_DATE_RE = _re.compile(r"(\d{4}-\d{2}-\d{2})")
dated_folders = []
for f in output_folders:
    suffix = os.path.basename(f).replace("Outputs_", "", 1)
    m = _DATE_RE.search(suffix)
    if not m:
        continue
    try:
        folder_date = datetime.strptime(m.group(1), "%Y-%m-%d")
    except ValueError:
        continue
    # Everything after the matched date (and any leading underscore) is the location
    remainder = suffix[m.end():]
    location = remainder.lstrip("_")
    dated_folders.append((f, folder_date, location))
dated_folders.sort(key=lambda x: (x[1], x[2]), reverse=True)

if not dated_folders:
    st.info("No data found.")
    st.stop()

# --- 5. Sidebar Selection ---
# Determine the most recent date per location name
most_recent_per_location = {}
for f, folder_date, location in dated_folders:
    if location not in most_recent_per_location:
        most_recent_per_location[location] = folder_date

def make_display_label(folder_date, location):
    date_str = folder_date.strftime("%Y-%m-%d")
    if location:
        if most_recent_per_location.get(location) == folder_date:
            return f"{date_str} ({location}, most recent)"
        return f"{date_str} ({location})"
    # No location: fall back to old behaviour
    if folder_date == dated_folders[0][1]:
        return f"{date_str} (most recent)"
    return date_str

date_options = [make_display_label(fd, loc) for _, fd, loc in dated_folders]

selected_display = st.sidebar.selectbox("Date", date_options)
# Recover folder_path from selected index
selected_idx = date_options.index(selected_display)
folder_path, selected_folder_dt, _ = dated_folders[selected_idx]
selected_folder_date = selected_folder_dt.strftime("%Y-%m-%d")
tif_files = glob.glob(os.path.join(folder_path, "*_cog.tif"))

st.title(f"Preview: {selected_folder_date}")
st.caption(f"Connected to GEE Project: `{project_id}`")

# --- 6. Map Generation Logic ---
center = [28.3, 85.6]
fit_bounds = None

if tif_files:
    try:
        with rasterio.open(tif_files[0]) as src:
            wgs_bounds = transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
            center = [(wgs_bounds[1] + wgs_bounds[3]) / 2, (wgs_bounds[0] + wgs_bounds[2]) / 2]
            fit_bounds = [[wgs_bounds[1], wgs_bounds[0]], [wgs_bounds[3], wgs_bounds[2]]]
    except:
        pass

m = folium.Map(location=center, zoom_start=12)
folium.TileLayer("https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}", 
                  attr="Google", name="Satellite").add_to(m)

draw = Draw(
    export=False,
    draw_options={'polyline': False, 'rectangle': True, 'polygon': False, 'circle': False, 'marker': False, 'circlemarker': False}
).add_to(m)
m.add_child(MeasureControl(position='topleft'))

from io import BytesIO

_MERCATOR = RioCRS.from_epsg(3857)

@st.cache_data(show_spinner=False)
def _render_tif(tif_path, vis_min, vis_max, palette, mask_below_zero, mtime):
    """Reproject to Web Mercator, colourise, encode PNG. Cached by (path, mtime)."""
    try:
        with rasterio.open(tif_path) as src:
            dst_transform, dst_w, dst_h = calculate_default_transform(
                src.crs, _MERCATOR, src.width, src.height, *src.bounds)
            raw = src.read(1).astype(np.float32)
            if src.nodata is not None:
                raw[raw == src.nodata] = np.nan
            if mask_below_zero:
                raw[raw < 0] = np.nan  # sub-zero = non-water, treat as nodata
            dst = np.full((dst_h, dst_w), np.nan, dtype=np.float32)
            reproject(
                source=raw, destination=dst,
                src_transform=src.transform, src_crs=src.crs,
                dst_transform=dst_transform, dst_crs=_MERCATOR,
                resampling=Resampling.bilinear,
                src_nodata=np.nan, dst_nodata=np.nan,
            )
        merc_w = dst_transform.c
        merc_n = dst_transform.f
        merc_e = merc_w + dst_transform.a * dst_w
        merc_s = merc_n + dst_transform.e * dst_h
        tb = transform_bounds(_MERCATOR, 'EPSG:4326', merc_w, merc_s, merc_e, merc_n)
        nodata_mask = np.isnan(dst)
        norm = np.clip((dst - vis_min) / (vis_max - vis_min), 0, 1)
        norm[nodata_mask] = 0.0
        cmap = plt.get_cmap(palette)
        rgba = (cmap(norm) * 255).astype(np.uint8)
        rgba[nodata_mask, 3] = 0
        img_io = BytesIO()
        Image.fromarray(rgba, mode='RGBA').save(img_io, format='PNG')
        img_io.seek(0)
        return base64.b64encode(img_io.read()).decode(), tb[1], tb[0], tb[3], tb[2]
    except Exception:
        return None

# Add TIF Layers
if tif_files:
    with st.spinner("Loading map layers, please wait..."):
        for tif in tif_files:
            basename = os.path.basename(tif)
            vis = get_vis_params(basename)
            result = _render_tif(tif, vis['min'], vis['max'], vis['palette'],
                                 'potential_water' in basename, os.path.getmtime(tif))
            if result:
                img_b64, south, west, north, east = result
                folium.raster_layers.ImageOverlay(
                    image=f"data:image/png;base64,{img_b64}",
                    bounds=[[south, west], [north, east]],
                    name=basename, opacity=0.7, interactive=False
                ).add_to(m)
    if fit_bounds:
        m.fit_bounds(fit_bounds)

# Combined colour legend
layers_present = [k for k in VIS_BY_LAYER if any(k in os.path.basename(t) for t in tif_files)]
leg = make_combined_legend(layers_present, VIS_BY_LAYER)
if leg:
    m.get_root().html.add_child(leg)

# Dashed bounding box derived from the first raw backscatter TIF in tracking results
tracking_raw_tifs = sorted(glob.glob(os.path.join(folder_path, "tracking_results", "*VV_raw*.tif")))
if tracking_raw_tifs:
    try:
        with rasterio.open(tracking_raw_tifs[0]) as _src:
            _b = transform_bounds(_src.crs, "EPSG:4326", *_src.bounds)
        _xmin, _ymin, _xmax, _ymax = _b
        folium.Rectangle(
            bounds=[[_ymin, _xmin], [_ymax, _xmax]],
            color="#FF6B00",
            weight=2,
            dash_array="8 6",
            fill=False,
            tooltip="Tracking analysis AOI",
            name="Tracking AOI",
        ).add_to(m)
    except Exception:
        pass

# Handle Clusters GeoJson
geojson_files = glob.glob(os.path.join(folder_path, "detected_clusters*.geojson"))
if geojson_files:
    geojson_files.sort(key=os.path.getmtime, reverse=True)
    with open(geojson_files[0], "r", encoding="utf-8") as fh:
        gj = json.load(fh)
    folium.GeoJson(gj, name="All Clusters",
        style_function=lambda feat: {"color": "red", "weight": 2, "fillColor": "red", "fillOpacity": 0.1},
        tooltip=folium.GeoJsonTooltip(fields=["cluster_id", "area_m2"], aliases=["ID", "Area"])
    ).add_to(m)

Fullscreen(
    position="topright",
    title="Expand map",
    title_cancel="Exit fullscreen",
    force_separate_button=True,
).add_to(m)
folium.LayerControl(collapsed=False).add_to(m)
map_output = st_folium(m, width="100%", height=620, returned_objects=["all_drawings"])

# --- 7. Data Sync & Table ---
cluster_csv_files = glob.glob(os.path.join(folder_path, "cluster_summary*.csv"))
data_rows = []
selected_ids = []
drawn_aoi = None

if cluster_csv_files:
    cluster_csv_files.sort(key=os.path.getmtime, reverse=True)
    with open(cluster_csv_files[0], mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data_rows.append({
                "Cluster_ID": row["Cluster_ID"],
                "Pixel_Count": int(row["Pixel_Count"]),
                "Area_m2": float(row["Area_m2"]),
                "Centroid_Lon": float(row["Centroid_Lon"]),
                "Centroid_Lat": float(row["Centroid_Lat"]),
                "Selected": " " 
            })

    if map_output and map_output.get("all_drawings"):
        for drawing in map_output["all_drawings"]:
            if drawing['geometry']['type'] == 'Polygon':
                coords = drawing['geometry']['coordinates'][0]
                lons, lats = [c[0] for c in coords], [c[1] for c in coords]
                drawn_aoi = [min(lons), min(lats), max(lons), max(lats)]
                for row in data_rows:
                    if (min(lons) <= row["Centroid_Lon"] <= max(lons) and 
                        min(lats) <= row["Centroid_Lat"] <= max(lats)):
                        selected_ids.append(str(row["Cluster_ID"]))
    
    selected_ids = list(set(selected_ids))
    for row in data_rows:
        if str(row["Cluster_ID"]) in selected_ids:
            row["Selected"] = "In Box"
    data_rows.sort(key=lambda x: x["Selected"] == "In Box", reverse=True)

# Display Table
st.write("---")
st.subheader("Detected Clusters Summary")
m_col1, m_col2, m_col3 = st.columns(3)
m_col1.metric("Total Detected", len(data_rows))
if data_rows:
    m_col2.metric("Clusters in Selection", len(selected_ids))

st.dataframe(data_rows, width=1100, height=400, hide_index=True)


# --- 8. Progress Tracking & Execution ---
st.write("### Analysis Progress")
progress_container = st.empty()
progress_container.info("No time tracking analysis running, or running in the background. *(Refreshing stops streaming of messages — an issue to fix.)*")

st.sidebar.header("Cluster tracking over time")
if drawn_aoi:
    st.sidebar.success(f"AOI Defined: {len(selected_ids)} clusters selected.")
    base_date_dt = datetime.strptime(selected_folder_date, "%Y-%m-%d")
    days_back = st.sidebar.slider("Look-back period (days)", 1, 180, 90)
    calc_start = (base_date_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")
    st.sidebar.write(f"**Period:** {calc_start} to {selected_folder_date}")

    if st.sidebar.button("Run Tracking Analysis"):
        try:
            # 1. Write Config (Includes Credentials)
            cfg_p = write_timetrack_config(folder_path, drawn_aoi, calc_start, 
                                           selected_folder_date, selected_ids, 
                                           project_id, service_account_path)
            
            # 2. Path to script
            script_rel_path = os.path.join("GEE", "tracking_headless.py")
            
            # 3. Launch with PIPE
            process = subprocess.Popen(
                [sys.executable, "-u", script_rel_path, cfg_p], 
                cwd=ROOT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            full_log = ""
            with progress_container.container():
                st.info("Initializing GEE tracking analysis...")
                log_box = st.code("Streaming logs...")
                
                for line in iter(process.stdout.readline, ""):
                    full_log += line
                    log_box.code(full_log)
            
            process.stdout.close()
            if process.wait() == 0:
                st.sidebar.success("Analysis Complete!")
            else:
                st.sidebar.error("Analysis failed. Check logs above.")

        except Exception as e:
            st.sidebar.error(f"Error: {e}")
else:
    st.sidebar.warning("Draw an area of interest on the map to define the target area for time tracking analysis.")


# timetracking viewer
render_tracking_viewer(folder_path)
