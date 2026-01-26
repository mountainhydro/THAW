# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 14:12:29 2026
@author: Fugger
"""

import os
import glob
import json
import subprocess
from datetime import datetime

import streamlit as st
from localtileserver import TileClient
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(layout="wide", page_title="Sentinel-1 Water Monitor")


# ============================================================
# PATH SETUP (relative, robust)
# ============================================================
# Current file: .../THAW/Dashboard/pages/02_Scheduler.py
pages_dir = os.path.dirname(os.path.abspath(__file__))      # pages
dash_dir = os.path.dirname(pages_dir)                       # Dashboard
root_dir = os.path.dirname(dash_dir)                        # THAW

output_dir = os.path.join(root_dir, "Outputs")
config_dir = os.path.join(root_dir, "config")

os.makedirs(config_dir, exist_ok=True)


# ============================================================
# SIDEBAR — GEE CREDENTIALS
# ============================================================
st.sidebar.title("Scheduler")

project_id = st.sidebar.text_input("GEE Project ID")

service_account_path = st.sidebar.text_input(
    "Service account JSON path",
    placeholder=r"C:\Users\...\service-account.json"
)


# ============================================================
# SIDEBAR — SCHEDULER INPUTS
# ============================================================
if "frequency_changed" not in st.session_state:
    st.session_state.frequency_changed = False

def mark_frequency_changed():
    st.session_state.frequency_changed = True

frequency = st.sidebar.selectbox(
    "Run Frequency",
    ["Daily", "Weekly", "Monthly"],
    key="frequency_select",
    on_change=mark_frequency_changed
)

weekday = None
month_day = None

if frequency == "Weekly":
    weekday = st.sidebar.selectbox(
        "Weekday",
        ["Monday", "Tuesday", "Wednesday", "Thursday",
         "Friday", "Saturday", "Sunday"]
    )

if frequency == "Monthly":
    month_day = st.sidebar.number_input(
        "Day of month",
        min_value=1,
        max_value=31,
        value=1
    )

time_options = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]
time_of_day = st.sidebar.selectbox("Time of day", time_options)


# ============================================================
# CRON CONVERSION
# ============================================================
hour, minute = time_of_day.split(":")

if frequency == "Daily":
    cron = f"{int(minute)} {int(hour)} * * *"
    schedule_summary = f"Daily at {time_of_day}"

elif frequency == "Weekly":
    weekday_map = {
        "Monday": 1, "Tuesday": 2, "Wednesday": 3,
        "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 0
    }
    cron = f"{int(minute)} {int(hour)} * * {weekday_map[weekday]}"
    schedule_summary = f"Weekly on {weekday} at {time_of_day}"

else:  # Monthly
    cron = f"{int(minute)} {int(hour)} {month_day} * *"
    schedule_summary = f"Monthly on day {month_day} at {time_of_day}"

st.sidebar.markdown("### Schedule Summary")
st.sidebar.info(schedule_summary)


# ============================================================
# FIND LATEST OUTPUT FOLDER (Outputs_YYYY-MM-DD)
# ============================================================
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

latest_folder = dated_folders[0][0] if dated_folders else None
tif_files = glob.glob(os.path.join(latest_folder, "*_cog.tif")) if latest_folder else []


# ============================================================
# MAP SETUP
# ============================================================
if tif_files:
    first_tc = TileClient(tif_files[0])
    bounds = first_tc.bounds()
    center = [
        (bounds[0] + bounds[1]) / 2,
        (bounds[2] + bounds[3]) / 2
    ] if bounds else [28.3, 85.6]
else:
    center = [28.3, 85.6]

m = folium.Map(location=center, zoom_start=10)

folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    name="Satellite",
    attr="© Esri, Maxar, Earthstar Geographics"
).add_to(m)

Draw(
    export=True,
    draw_options={
        "polygon": True,
        "rectangle": True,
        "polyline": False,
        "circle": False,
        "circlemarker": False,
        "marker": False
    },
    edit_options={"edit": True, "remove": True}
).add_to(m)

folium.LayerControl().add_to(m)

draw_data = st_folium(m, width=900, height=700)


# ============================================================
# AOI EXTRACTION
# ============================================================
aoi_geojson = None

if draw_data and draw_data.get("all_drawings"):
    aoi_geojson = draw_data["all_drawings"][0]["geometry"]

    coords = aoi_geojson.get("coordinates", [[]])[0]
    flat_coords = []
    for lon, lat in coords:
        flat_coords.append(f"{lon:.5f}")
        flat_coords.append(f"{lat:.5f}")

    aoi_message = "AOI selected: " + ", ".join(flat_coords)
    st.sidebar.info(aoi_message)


# ============================================================
# SIDEBAR VALIDATION MESSAGES
# ============================================================
missing = []

if not project_id:
    missing.append("Please select a GEE project")

if not service_account_path:
    missing.append("Please provide a service account JSON path")

if not aoi_geojson:
    missing.append("Please draw an AOI on the map")

if missing:
    st.sidebar.markdown("### Required inputs")
    for msg in missing:
        st.sidebar.warning(msg)

if not st.session_state.frequency_changed:
    st.sidebar.warning("Please adjust Schedule frequency or confirm (Daily).")

if not missing and st.session_state.frequency_changed:
    st.sidebar.success("All required inputs provided.")


# ============================================================
# SCHEDULE BUTTON (DISABLED UNTIL READY)
# ============================================================
all_inputs_ready = (
    project_id and
    service_account_path and
    aoi_geojson and
    st.session_state.frequency_changed
)

if st.sidebar.button("🗓️ Schedule job", disabled=not all_inputs_ready):

    # Save AOI
    aoi_path = os.path.join(config_dir, "aoi.geojson")
    with open(aoi_path, "w") as f:
        json.dump({
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "geometry": aoi_geojson}]
        }, f, indent=2)

    # Save job config
    config = {
        "project_id": project_id,
        "service_account": service_account_path,
        "frequency": frequency,
        "weekday": weekday,
        "month_day": month_day,
        "time_of_day": time_of_day,
        "cron": cron,
        "aoi_path": aoi_path
    }

    config_path = os.path.join(config_dir, "job_config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    st.sidebar.success("Job scheduled! Running now...")

    subprocess.Popen(
        ["python", "main_gee_script.py", config_path],
        cwd=root_dir
    )

    st.sidebar.info("Processing started in background.")
