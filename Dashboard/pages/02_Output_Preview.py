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
from datetime import datetime, timedelta
import rasterio
from rasterio.warp import transform_bounds
from streamlit_folium import st_folium
from folium.plugins import MeasureControl, Draw
import matplotlib.pyplot as plt
import base64
from io import BytesIO

# 1. Function Definitions
def get_vis_params(filename):
    for key, vis in VIS_BY_LAYER.items():
        if key in filename:
            return vis
    return {'min': -30, 'max': 0, 'palette': 'gray'}

# 2. Page Configuration
st.set_page_config(layout="wide", page_title="Output Preview")

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

# 3. Directory Setup
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
output_dir = os.path.join(root_dir, "Outputs")

# 4. Visualization Settings
VIS_BY_LAYER = {
    "z_score": {"min": -2, "max": 2, "palette": "RdYlGn"},
    "potential_water": {"min": 0, "max": 1, "palette": "Blues"},
    "mean_diff": {"min": -5, "max": 5, "palette": "RdBu"},
}

# 5. Data Discovery
output_folders = glob.glob(os.path.join(output_dir, "Outputs_*"))
dated_folders = sorted([
    (f, datetime.strptime(os.path.basename(f).replace("Outputs_", ""), "%Y-%m-%d"))
    for f in output_folders if "Outputs_" in f
], key=lambda x: x[1], reverse=True)

if not dated_folders:
    st.info("No data found.")
    st.stop()

# 6. Sidebar Selection
date_options = [f[1].strftime("%Y-%m-%d") for f in dated_folders]
if date_options:
    date_options[0] += " (most recent)"

selected_display = st.sidebar.selectbox("Date", date_options)
selected_folder_date = selected_display.replace(" (most recent)", "")
folder_path = next(f[0] for f in dated_folders if f[1].strftime("%Y-%m-%d") == selected_folder_date)
tif_files = glob.glob(os.path.join(folder_path, "*_cog.tif"))

st.title(f"Preview: {selected_folder_date}")

# 7. Dynamic Center & Bounds
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

# 8. Map Generation
m = folium.Map(location=center, zoom_start=12)
folium.TileLayer("https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}", 
                  attr="Google", name="Satellite").add_to(m)

# Setup Draw Control (Rectangle Only)
draw = Draw(
    export=False,
    draw_options={
        'polyline': False, 
        'rectangle': True, 
        'polygon': False, 
        'circle': False, 
        'marker': False, 
        'circlemarker': False
    }
)
draw.add_to(m)
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
                    name=basename, opacity=0.7, interactive=False # Set to False for cleaner box drawing
                ).add_to(m)
        except:
            pass
    if fit_bounds:
        m.fit_bounds(fit_bounds)

# --- Handling GeoJson ---
geojson_path = ""
geojson_files = glob.glob(os.path.join(folder_path, "detected_clusters*.geojson"))

if geojson_files:
    geojson_files.sort(key=os.path.getmtime, reverse=True)
    geojson_path = geojson_files[0]
    with open(geojson_path, "r", encoding="utf-8") as fh:
        gj = json.load(fh)
    
    folium.GeoJson(
        gj,
        name="All Clusters",
        style_function=lambda feat: {
            "color": "red", "weight": 2, "fillColor": "red", "fillOpacity": 0.1
        },
        tooltip=folium.GeoJsonTooltip(fields=["cluster_id", "area_m2"], aliases=["ID", "Area"])
    ).add_to(m)

# 9. Map Capture
# We only track 'all_drawings' now
map_output = st_folium(
    m, width=1100, height=650, 
    returned_objects=["all_drawings"]
)

# --- SYNC LOGIC ---
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

    # Logic for Box-only selection
    if map_output and map_output.get("all_drawings"):
        for drawing in map_output["all_drawings"]:
            if drawing['geometry']['type'] == 'Polygon':
                coords = drawing['geometry']['coordinates'][0]
                lons, lats = [c[0] for c in coords], [c[1] for c in coords]
                
                # Capture the AOI for the subprocess
                drawn_aoi = [min(lons), min(lats), max(lons), max(lats)]
                
                # Find which clusters are inside this specific box
                for row in data_rows:
                    if (min(lons) <= row["Centroid_Lon"] <= max(lons) and 
                        min(lats) <= row["Centroid_Lat"] <= max(lats)):
                        selected_ids.append(str(row["Cluster_ID"]))
    
    selected_ids = list(set(selected_ids))

    for row in data_rows:
        if str(row["Cluster_ID"]) in selected_ids:
            row["Selected"] = "In Box"

    data_rows.sort(key=lambda x: x["Selected"] == "In Box", reverse=True)

    # 10. Summary & Table
    st.write("---")
    st.subheader("Detected Clusters Summary")
    
    m_col1, m_col2, m_col3 = st.columns(3)
    m_col1.metric("Total Detected", len(data_rows))
    if data_rows:
        m_col2.metric("Clusters in Selection", len(selected_ids))
        m_col3.info("Draw a box on the map to select clusters for analysis.")

    st.dataframe(
        data_rows, width=1100, height=400, hide_index=True,
        column_config={
            "Selected": st.column_config.TextColumn("Selection Status", width="medium"),
            "Cluster_ID": st.column_config.TextColumn("ID"),
            "Area_m2": st.column_config.NumberColumn("Area (m²)", format="%.0f"),
        }
    )

    # --- Sidebar Analysis Trigger (Box-Dependent) ---
    st.sidebar.header("Cluster tracking over time")
    
    if drawn_aoi:
        st.sidebar.success(f"AOI Defined: {len(selected_ids)} clusters inside.")
        
        base_date_dt = datetime.strptime(selected_folder_date, "%Y-%m-%d")
        days_back = st.sidebar.slider(
            "Look-back period (days)", 1, 180, 90
        )
        calc_start = (base_date_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        st.sidebar.write(f"**Analysis Period:** {calc_start} to {selected_folder_date}")

        if st.sidebar.button("Run Tracking Analysis"):
            try:
                # Always pass the drawn AOI box to the subprocess
                cmd = [
                    "python", "analyze_clusters.py",
                    "--start", calc_start,
                    "--end", selected_folder_date,
                    "--out", os.path.join(folder_path, "in_depth_results"),
                    "--aoi", json.dumps(drawn_aoi) # Pass the [minX, minY, maxX, maxY]
                ]
                
                # Still pass IDs in case the script needs them for filtering
                if selected_ids:
                    cmd.extend(["--ids"] + selected_ids)

                subprocess.Popen(cmd, cwd=root_dir)
                st.sidebar.success("Background task started.")
            except Exception as e:
                st.sidebar.error(f"Error: {e}")
    else:
        st.sidebar.warning("Draw a rectangle on the map to define the target area.")

folium.LayerControl().add_to(m)