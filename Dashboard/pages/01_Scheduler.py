# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 14:12:29 2026
@author: Fugger
"""

import os
import glob
import json
import subprocess
import threading
import sys
import time
from datetime import datetime, date as dt_date
import streamlit as st
from localtileserver import TileClient
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
from streamlit.runtime.scriptrunner import add_script_run_ctx


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(layout="wide", page_title="Sentinel-1 Water Monitor")


# ============================================================
# PATH SETUP (relative, robust)
# ============================================================
pages_dir = os.path.dirname(os.path.abspath(__file__))      # pages
dash_dir = os.path.dirname(pages_dir)                       # Dashboard
root_dir = os.path.dirname(dash_dir)                        # THAW
gee_dir = f"{root_dir}\GEE"

output_dir = os.path.join(root_dir, "Outputs")
config_dir = os.path.join(root_dir, "config")
os.makedirs(config_dir, exist_ok=True)


# ============================================================
# SIDEBAR — GEE CREDENTIALS
# ============================================================
st.sidebar.title("Job configuration")
project_id = st.sidebar.text_input("GEE Project ID")
service_account_path = st.sidebar.text_input(
    "Service account JSON path",
    placeholder=r"C:\Users\...\service-account.json"
)


# ============================================================
# FIND LATEST OUTPUT FOLDER (Outputs_YYYY-MM-DD)
# ============================================================
output_folders = glob.glob(os.path.join(output_dir, "Outputs_*"))
dated_folders = []

for f in output_folders:
    name = os.path.basename(f)
    try:
        folder_date = datetime.strptime(name.replace("Outputs_", ""), "%Y-%m-%d")
        dated_folders.append((f, folder_date))
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
    center = [(bounds[0] + bounds[1]) / 2, (bounds[2] + bounds[3]) / 2] if bounds else [28.3, 85.6]
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
    st.sidebar.info("AOI selected: " + ", ".join(flat_coords))

aoi_path = os.path.join(config_dir, "aoi.geojson")
with open(aoi_path, "w") as f:
    json.dump({
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "geometry": aoi_geojson}]
    }, f, indent=2)

# ============================================================
# SELECT RUN DATE (DEFAULT TODAY)
# ============================================================
run_date = st.sidebar.date_input(
    "Run date",
    value=dt_date.today(),
    max_value=dt_date.today()
)
if run_date == dt_date.today():
    st.sidebar.caption("📅 Today selected")


# ============================================================
# SIDEBAR VALIDATION MESSAGES for "Run Now"
# ============================================================
missing_now = []
if not project_id:
    missing_now.append("Please select a GEE project")
if not service_account_path:
    missing_now.append("Please provide a service account JSON path")
if not aoi_geojson:
    missing_now.append("Please draw an AOI on the map")
if missing_now:
    st.sidebar.markdown("### Required inputs")
    for msg in missing_now:
        st.sidebar.warning(msg)


# ============================================================
# RUN NOW BUTTON
# ============================================================
all_inputs_ready_now = (
    project_id and
    service_account_path and
    os.path.exists(service_account_path) and
    aoi_geojson is not None
)

run_now_clicked = st.sidebar.button(
    "▶ Run job now",
    disabled=not all_inputs_ready_now
)


# ============================================================
# WRITE CONFIG AND LAUNCH HEADLESS SCRIPT
# ============================================================
def write_now_config():
    cfg = {
        "run_date": run_date.isoformat(),
        "aoi_geojson": aoi_path,  # <-- path to file
        "project_id": project_id,
        "service_account_path": service_account_path,
        "output_root": output_dir
    }
    
    config_path = os.path.join(config_dir, "now_config.json")
    with open(config_path, "w") as f:
        json.dump(cfg, f, indent=2)
    return config_path

def run_headless(config_path, output_container):
    # Ensure gee_dir is correctly defined in your script scope
    script_path = os.path.join(gee_dir, "lakedetection_headless.py")
    
    # -u is critical: it tells the python subprocess to not buffer the output
    cmd = [sys.executable, "-u", script_path, config_path]
    
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1  # Line-buffered
    )

    full_log = ""
    
    # Continuously read the output
    while True:
        line = process.stdout.readline()
        
        # If no more output and process is done, break
        if not line and process.poll() is not None:
            break
            
        if line:
            full_log += line
            # Overwrite the empty container with the code block
            # This provides the 'live streaming' effect
            output_container.code(full_log)
        
    process.wait()
    
    if process.returncode == 0:
        output_container.success("Job finished successfully! Please check Output Preview page")
    else:
        # If it failed, the full_log will already contain the error traceback
        output_container.error(f"Job failed with return code {process.returncode}")

# --- Trigger Logic ---
if run_now_clicked:
    config_path = write_now_config()
    
    # 1. Create a placeholder in the UI for the logs
    status_container = st.empty()
    status_container.info("Initializing Subprocess...")

    # 2. Define the command (the -u flag is the 'Live' switch)
    script_path = os.path.join(gee_dir, "lakedetection_headless.py")
    cmd = [sys.executable, "-u", script_path, config_path]

    # 3. Start the process
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, # Redirect errors to the same pipe
        text=True,
        bufsize=1,                # Line-buffered
        universal_newlines=True
    )

    full_log = ""

    # 4. The "Live Listener" loop
    # This loop keeps the Dashboard busy until the GEE script finishes
    for line in iter(process.stdout.readline, ""):
        full_log += line
        # Every time a new line comes in, overwrite the code block
        status_container.code(full_log)

    process.stdout.close()
    return_code = process.wait()

    # 5. Final Status Update
    if return_code == 0:
        st.success("Processing Complete!")
    else:
        st.error(f"Process failed with return code {return_code}")

# ============================================================
# SIDEBAR — SCHEDULER INPUTS
# ============================================================
import subprocess

st.sidebar.title("Scheduler")
if "frequency_changed" not in st.session_state:
    st.session_state.frequency_changed = False

def mark_frequency_changed():
    st.session_state.frequency_changed = True

# Frequency selection
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
        ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    )
if frequency == "Monthly":
    month_day = st.sidebar.number_input(
        "Day of month",
        min_value=1,
        max_value=31,
        value=1
    )
time_options = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0,15,30,45)]
time_of_day = st.sidebar.selectbox("Time of day", time_options)

# ============================================================
# VALIDATION
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
    st.sidebar.warning("Please adjust schedule frequency or confirm (Daily).")
else:
    st.sidebar.success("All required inputs provided.")

# ============================================================
# SCHEDULE JOB BUTTON
# ============================================================
all_inputs_ready = (
    project_id and
    service_account_path and
    aoi_geojson and
    st.session_state.frequency_changed
)

if st.sidebar.button("📅 Schedule job", disabled=not all_inputs_ready):
    # --- Save AOI ---
    aoi_path = os.path.join(config_dir, "aoi.geojson")
    with open(aoi_path, "w") as f:
        json.dump({"type":"FeatureCollection","features":[{"type":"Feature","geometry":aoi_geojson}]}, f, indent=2)

    # --- Save job config ---
    job_cfg = {
        "run_date": "today",  # <-- today
        "aoi_geojson": aoi_path,                         # path to AOI
        "project_id": project_id,
        "service_account_path": service_account_path,
        "output_root": output_dir
        }
    config_path = os.path.join(config_dir, "job_config.json")
    with open(config_path, "w") as f:
        json.dump(job_cfg, f, indent=2)

    # --- Windows Scheduler Setup ---
    python_exe = sys.executable
    script_path = os.path.join(root_dir, "GEE", "lakedetection_headless.py")

    # Define task name
    task_name_map = {"Daily": "LakeDetection_Daily", "Weekly": "LakeDetection_Weekly", "Monthly": "LakeDetection_Monthly"}
    task_name = task_name_map[frequency]

    # Build the schtasks command
    if frequency == "Weekly":
        weekday_map = {
            "Monday": "MON","Tuesday": "TUE","Wednesday": "WED",
            "Thursday": "THU","Friday": "FRI","Saturday": "SAT","Sunday": "SUN"
        }
        sch_weekday = weekday_map.get(weekday, "MON")
        sch_cmd = f'schtasks /Create /SC WEEKLY /D {sch_weekday} /TN "{task_name}" /TR "{python_exe} {script_path} {config_path}" /ST {time_of_day} /F'
    elif frequency == "Daily":
        sch_cmd = f'schtasks /Create /SC DAILY /TN "{task_name}" /TR "{python_exe} {script_path} {config_path}" /ST {time_of_day} /F'
    else:  # Monthly
        sch_cmd = f'schtasks /Create /SC MONTHLY /D {month_day} /TN "{task_name}" /TR "{python_exe} {script_path} {config_path}" /ST {time_of_day} /F'

    # --- Execute Scheduler Command ---
    try:
        os.system(sch_cmd)
        st.sidebar.success(f"Scheduled job '{task_name}' successfully")
        st.sidebar.info(f"It will run automatically {frequency} on {weekday} at {time_of_day}")
    except Exception as e:
        st.sidebar.error(f"Failed to schedule job: {e}")
        
# -*- coding: utf-8 -*-


import streamlit as st
import subprocess

TASK_PREFIX = "LakeDetection"

# --------------------------------------
# List all LakeDetection tasks
# --------------------------------------
st.subheader("Existing Tasks")

try:
    cmd = 'schtasks /Query /FO LIST'
    output = subprocess.check_output(cmd, shell=True, text=True)
except subprocess.CalledProcessError:
    st.error("Failed to query scheduled tasks.")
    st.stop()

# Parse tasks
tasks = []
current_task = {}
for line in output.splitlines():
    if line.strip() == "":
        if current_task:
            if TASK_PREFIX in current_task.get("TaskName", ""):
                tasks.append(current_task)
            current_task = {}
        continue
    if ":" in line:
        key, val = line.split(":", 1)
        current_task[key.strip()] = val.strip()
# Add last task
if current_task and TASK_PREFIX in current_task.get("TaskName", ""):
    tasks.append(current_task)

if not tasks:
    st.info(f"No scheduled tasks found for '{TASK_PREFIX}'.")
else:
    for t in tasks:
        st.markdown(f"**{t.get('TaskName','')}**")
        st.write(f"Next Run Time: {t.get('Next Run Time','')}")
        st.write(f"Status: {t.get('Status','')}")
        st.write(f"Last Run Time: {t.get('Last Run Time','')}")
        if st.button(f"🗑️ Delete {t.get('TaskName','')}", key=t.get('TaskName','')):
            delete_cmd = f'schtasks /Delete /TN "{t.get("TaskName")}" /F'
            try:
                subprocess.run(delete_cmd, shell=True, check=True)
                st.success(f"Task {t.get('TaskName')} deleted!")
            except subprocess.CalledProcessError:
                st.error(f"Failed to delete {t.get('TaskName')}")
            st.rerun()
