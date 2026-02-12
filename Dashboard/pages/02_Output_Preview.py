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
from datetime import datetime
import rasterio
from rasterio.warp import transform_bounds
from streamlit_folium import st_folium
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
    except:
        pass

# 8. Map Generation
m = folium.Map(location=center, zoom_start=12)
folium.TileLayer("https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}", 
                  attr="Google", name="Satellite").add_to(m)

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

# 9. Output Display
folium.LayerControl().add_to(m)
st_folium(m, width=1100, height=650, returned_objects=[])