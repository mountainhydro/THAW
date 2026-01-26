# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 14:12:29 2026
@author: Fugger
"""


import streamlit as st
import os
import glob
from datetime import datetime
from localtileserver import TileClient
import folium
from streamlit_folium import st_folium

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="Output Preview")


# paths
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../Dashboard
output_dir = os.path.join(root_dir, "Outputs")                          # .../THAW/Outputs

# --------------------------------------------------
# Find and sort output folders by date
# --------------------------------------------------
output_folders = glob.glob(os.path.join(output_dir, "Outputs_*"))

dated_folders = []
for f in output_folders:
    name = os.path.basename(f)
    try:
        date = datetime.strptime(name.replace("Outputs_", ""), "%Y-%m-%d")
        dated_folders.append((f, date))
    except ValueError:
        continue

dated_folders.sort(key=lambda x: x[1], reverse=True)

if not dated_folders:
    st.info("No output folders found. Run a task to generate data.")
    st.stop()

# --------------------------------------------------
# Sidebar dropdown (Inspect older outputs)
# --------------------------------------------------
folder_options = {}
for i, (folder, date) in enumerate(dated_folders):
    label = date.strftime("%Y-%m-%d")
    if i == 0:
        label += " (most recent)"
    folder_options[label] = folder

selected_label = st.sidebar.selectbox(
    "Select processing date",
    options=list(folder_options.keys()),
    index=0  # default to most recent
)

selected_folder = folder_options[selected_label]
selected_date = selected_folder.replace("Outputs_", "")

tif_files = glob.glob(os.path.join(selected_folder, "*_cog.tif"))

st.title("Preview of outputs")

if not tif_files:
    st.warning("No _cog.tif files found in the selected output.")
    st.stop()

# --------------------------------------------------
# Build map
# --------------------------------------------------
first_tc = TileClient(tif_files[0])
bounds = first_tc.bounds()

if not bounds:
    st.error("Could not read bounds from the COG file.")
    st.stop()

center = [(bounds[0] + bounds[1]) / 2, (bounds[2] + bounds[3]) / 2]
m = folium.Map(location=center, zoom_start=11)

# Satellite basemap
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    name="SATELLITE",
    attr="© Esri, Maxar, Earthstar Geographics"
).add_to(m)

# Keep tile clients alive
tile_clients = []
for tif in tif_files:
    name = os.path.basename(tif)
    tc = TileClient(tif)
    tile_clients.append(tc)

    folium.TileLayer(
        tiles=tc.get_tile_url(),
        attr="LocalTiles",
        name=name,
        overlay=True,
        control=True,
        opacity=1.0
    ).add_to(m)

folium.LayerControl().add_to(m)

# --------------------------------------------------
# Render map
# --------------------------------------------------
st_folium(m, width=900, height=700)
