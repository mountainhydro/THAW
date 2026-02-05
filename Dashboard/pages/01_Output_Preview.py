# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 14:12:29 2026
@author: Fugger
"""


import streamlit as st
import os
import glob
import numpy as np
import leafmap.foliumap as leafmap
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="Output Preview")


# paths
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # .../Dashboard
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




st.title("Latest Output Preview")

# --- Visualization Logic ---
VIS_BY_LAYER = {
    "z_score": {"min": -2, "max": 2, "palette": "RdYlGn", "nd": None},
    "potential_water": {"min": 0, "max": 1, "palette": "Blues", "nd": None},
    "mean_diff": {"min": -5, "max": 5, "palette": "RdBu", "nd": None},
}

def get_vis_params(filename):
    for key, vis in VIS_BY_LAYER.items():
        if key in filename:
            return vis
    return {'min': -30, 'max': 0, 'palette': 'gray'}

if not tif_files:
    st.warning("No files found.")
else:
    # Initialize Map with attribution to satisfy Folium's requirements
    m = leafmap.Map(
        tiles="OpenStreetMap", 
        attr="Google Satellite",
        google_map="SATELLITE"
    )

    for i, tif in enumerate(tif_files):
        if not os.path.exists(tif):
            continue
            
        basename = os.path.basename(tif)
        vis = get_vis_params(basename)
        
        try:
            # We use zoom_to_layer=True only for the first file 
            # so the map centers once and doesn't bounce around.
            m.add_raster(
                tif,
                layer_name=basename,
                palette=vis['palette'],
                vmin=vis['min'],
                vmax=vis['max'], 
                opacity=0.8,
                nodata=np.nan,
                zoom_to_layer=(i == 0),
                nan_color='rgba(0,0,0,0)'
            )
        except Exception as e:
            st.error(f"Error rendering {basename}: {e}")

    m.add_layer_control()
    m.to_streamlit(height=700)