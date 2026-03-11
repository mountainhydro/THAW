# -*- coding: utf-8 -*-

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
from rasterio.warp import transform_bounds
from streamlit_folium import st_folium
from folium.plugins import MeasureControl, Draw
import matplotlib.pyplot as plt
import base64
from io import BytesIO

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
dated_folders = sorted([
    (f, datetime.strptime(os.path.basename(f).replace("Outputs_", ""), "%Y-%m-%d"))
    for f in output_folders if "Outputs_" in f
], key=lambda x: x[1], reverse=True)

if not dated_folders:
    st.info("No data found.")
    st.stop()

# --- 5. Sidebar Selection ---
date_options = [f[1].strftime("%Y-%m-%d") for f in dated_folders]
if date_options:
    date_options[0] += " (most recent)"

selected_display = st.sidebar.selectbox("Date", date_options)
selected_folder_date = selected_display.replace(" (most recent)", "")
folder_path = next(f[0] for f in dated_folders if f[1].strftime("%Y-%m-%d") == selected_folder_date)
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

# Add TIF Layers
if tif_files:
    for tif in tif_files:
        basename = os.path.basename(tif)
        vis = get_vis_params(basename)
        try:
            with rasterio.open(tif) as src:
                tb = transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
                data = src.read(1).astype(float)
                data[data == src.nodata] = np.nan
                norm_data = np.clip((data - vis['min']) / (vis['max'] - vis['min']), 0, 1)
                cmap = plt.get_cmap(vis['palette'])
                rgba_img = cmap(norm_data, bytes=True)
                img_io = BytesIO()
                plt.imsave(img_io, rgba_img, format='png')
                img_io.seek(0)
                img_b64 = base64.b64encode(img_io.read()).decode()
                folium.raster_layers.ImageOverlay(
                    image=f"data:image/png;base64,{img_b64}",
                    bounds=[[tb[1], tb[0]], [tb[3], tb[2]]],
                    name=basename, opacity=0.7, interactive=False
                ).add_to(m)
        except:
            pass
    if fit_bounds:
        m.fit_bounds(fit_bounds)

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

map_output = st_folium(m, width=1100, height=650, returned_objects=["all_drawings"])

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
    st.sidebar.warning("Draw a rectangle on the map to define the target area.")

folium.LayerControl().add_to(m)