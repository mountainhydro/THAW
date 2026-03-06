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
import csv
from datetime import datetime
import rasterio
from rasterio.warp import transform_bounds
from streamlit_folium import st_folium
from folium.plugins import MeasureControl
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
    except (rasterio.errors.RasterioIOError, ValueError):
        pass

# 8. Map Generation
m = folium.Map(location=center, zoom_start=12)
folium.TileLayer("https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}", 
                  attr="Google", name="Satellite").add_to(m)
m.add_child(MeasureControl(
    position='topleft',
    primary_length_unit='meters',
    secondary_length_unit='kilometers',
    primary_area_unit='sqmeters',
    secondary_area_unit='hectares'
))
if not tif_files:
    st.warning("No TIF files found in this folder.")
else:
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
                    name=basename,
                    opacity=0.7,
                    interactive=True
                ).add_to(m)
        except Exception as e:
            st.error(f"Error reading {basename}: {e}")

    if fit_bounds:
        m.fit_bounds(fit_bounds)

# Add detected cluster polygons if GeoJSON exists
geojson_files = glob.glob(os.path.join(folder_path, "detected_clusters*.geojson"))
geojson_path = None
if geojson_files:
    # pick the most recent file (by modification time)
    geojson_files.sort(key=os.path.getmtime, reverse=True)
    geojson_path = geojson_files[0]

if geojson_path and os.path.exists(geojson_path):
    try:
        import json
        with open(geojson_path, "r", encoding="utf-8") as fh:
            gj = json.load(fh)
        folium.GeoJson(
            gj,
            name="Suspicious clusters",
            style_function=lambda feat: {"color": "red", "weight": 2, "fillColor": "red", "fillOpacity": 0.1},
            tooltip=folium.GeoJsonTooltip(
                fields=["cluster_id", "area_m2"],
                aliases=["Cluster ID", "Area (m²)"],
                localize=True
            )
        ).add_to(m)
    except Exception as e:
        st.warning(f"Could not load cluster polygons: {e}")

# 9. Output Display
folium.LayerControl().add_to(m)
st_folium(m, width=1100, height=650, returned_objects=[])

# 10. Cluster summary below map
cluster_csv_files = glob.glob(os.path.join(folder_path, "cluster_summary*.csv"))

# pick the most recent file (by modification time)
cluster_csv = None
if cluster_csv_files:
    cluster_csv_files.sort(key=os.path.getmtime, reverse=True)
    cluster_csv = cluster_csv_files[0]

if cluster_csv and os.path.exists(cluster_csv):
    try:
        import csv
        with open(cluster_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # CRITICAL: Convert strings to numbers for proper rounding/sorting
            raw_data = list(reader)
            processed_data = []
            for row in raw_data:
                processed_data.append({
                    "Cluster_ID": row["Cluster_ID"],
                    "Pixel_Count": int(float(row["Pixel_Count"])),
                    "Area_m2": float(row["Area_m2"]),
                    "Centroid_Lon": float(row["Centroid_Lon"]),
                    "Centroid_Lat": float(row["Centroid_Lat"])
                })

        if processed_data:
            st.markdown('<div class="map-matched-container">', unsafe_allow_html=True)
            with st.container():
                st.subheader("Detected Clusters Summary")
                
                # --- METRICS SECTION ---
                areas = [r['Area_m2'] for r in processed_data]
                total_clusters = len(processed_data)
                
                # Format metrics with thousands separator and 0 decimals
                m_col1, m_col2, m_col3 = st.columns(3)
                m_col1.metric("Detected Clusters", total_clusters)
                m_col2.metric("Largest Area", f"{max(areas):,.0f} m²")
                m_col3.metric("Avg. Cluster Area", f"{(sum(areas)/total_clusters):,.0f} m²" if total_clusters > 0 else "0 m²")
                
                st.write("---")
                
                # --- TABLE SECTION ---
                calc_height = min(600, (len(processed_data) + 1) * 38 + 40)
                st.markdown("#### Detailed Cluster Information")
                st.dataframe(
                    processed_data, 
                    width=1100, 
                    height=calc_height,
                    column_config={
                        "Cluster_ID": st.column_config.TextColumn("ID"),
                        "Pixel_Count": st.column_config.NumberColumn("Pixels", format="%d"),
                        "Area_m2": st.column_config.NumberColumn("Area (m²)", format="%.0f"), # Rounded to integer
                        "Centroid_Lon": st.column_config.NumberColumn("Lon", format="%.4f"),
                        "Centroid_Lat": st.column_config.NumberColumn("Lat", format="%.4f"),
                    },
                    hide_index=True
                )
            st.markdown('</div>', unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"Could not load cluster summary: {e}")