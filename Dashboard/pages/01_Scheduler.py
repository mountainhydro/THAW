# -*- coding: utf-8 -*-

"""
THAW - Streamlit Dashboard Scheduler page

Dr. Stefan Fugger

Created in Feb 2026
"""

import os
import glob
import json
import subprocess
import sys
import rasterio
from rasterio.warp import transform_bounds
from datetime import datetime, date as dt_date
import streamlit as st
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium

# 1. Function Definitions
def load_gee_creds():
    if os.path.exists(CRED_FILE):
        with open(CRED_FILE, "r") as f:
            lines = [line.strip() for line in f.readlines()]
            if len(lines) >= 2:
                return lines[0], lines[1]
    return None, None

def mark_frequency_changed():
    st.session_state.frequency_changed = True

def write_job_config(is_manual=True):
    aoi_p = os.path.join(CONFIG_DIR, "aoi.geojson")
    with open(aoi_p, "w") as f:
        json.dump({"type":"FeatureCollection","features":[{"type":"Feature","geometry":aoi_geojson}]}, f)

    cfg = {
        "run_date": run_date.isoformat() if is_manual else "today",
        "aoi_geojson": aoi_p,
        "project_id": project_id,
        "service_account_path": service_account_path,
        "output_root": OUTPUT_DIR
    }
    
    cfg_file = "now_config.json" if is_manual else "sch_config.json"
    cfg_path = os.path.join(CONFIG_DIR, cfg_file)
    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2)
    return cfg_path

# 2. Directory Setup
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DASH_DIR = os.path.dirname(CURRENT_DIR)                 
ROOT_DIR = os.path.dirname(DASH_DIR)                    
TEMP_DIR = os.path.join(ROOT_DIR, "temp")
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")
GEE_DIR = os.path.join(ROOT_DIR, "GEE")
OUTPUT_DIR = os.path.join(ROOT_DIR, "Outputs")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
os.makedirs(CONFIG_DIR, exist_ok=True)

# 3. Initialization and Auth Check
project_id, service_account_path = load_gee_creds()

st.set_page_config(layout="wide", page_title="Job Scheduler")

if not project_id:
    st.error("**No Credentials Found.** Please go to the **Home** page and log in first.")
    st.stop()

# 4. Data Discovery
output_folders = glob.glob(os.path.join(OUTPUT_DIR, "Outputs_*"))
dated_folders = []
for f in output_folders:
    try:
        folder_date = datetime.strptime(os.path.basename(f).replace("Outputs_", ""), "%Y-%m-%d")
        dated_folders.append((f, folder_date))
    except: continue

dated_folders.sort(key=lambda x: x[1], reverse=True)
latest_folder = dated_folders[0][0] if dated_folders else None
tif_files = glob.glob(os.path.join(latest_folder, "*_cog.tif")) if latest_folder else []

# 5. UI Header and Map Setup
st.title("THAW Task Manager and Scheduler")
st.success(f"Connected to Project: `{project_id}`")

center = [28.3, 85.6]
fit_bounds = None
if tif_files:
    try:
        with rasterio.open(tif_files[0]) as src:
            wgs_bounds = transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
            center = [(wgs_bounds[1] + wgs_bounds[3]) / 2, (wgs_bounds[0] + wgs_bounds[2]) / 2]
            fit_bounds = [[wgs_bounds[1], wgs_bounds[0]], [wgs_bounds[3], wgs_bounds[2]]]
    except: pass

m = folium.Map(location=center, zoom_start=10)
folium.TileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", 
                  name="Satellite", attr="Esri").add_to(m)

if fit_bounds:
    m.fit_bounds(fit_bounds)

Draw(export=True, draw_options={"polyline":False, "circle":False, "marker":False}).add_to(m)
draw_data = st_folium(m, width=900, height=550)

aoi_geojson = None
if draw_data and draw_data.get("all_drawings"):
    aoi_geojson = draw_data["all_drawings"][0]["geometry"]
    coords = aoi_geojson.get("coordinates", [[]])[0]
    flat_coords = [f"{lon:.5f}, {lat:.5f}" for lon, lat in coords[:3]]
    st.sidebar.info("AOI selected: " + " | ".join(flat_coords) + "...")

# 6. Sidebar Manual Run
st.sidebar.header("▶ Manual Run")
run_date = st.sidebar.date_input("Processing Date", value=dt_date.today(), max_value=dt_date.today())

if not aoi_geojson:
    st.sidebar.warning("Please draw an AOI on the map to run a manual job.")

run_now_clicked = st.sidebar.button("Run job now", disabled=not aoi_geojson)
st.sidebar.markdown("---")

# 7. Sidebar Scheduling
st.sidebar.header("📅 Scheduled Task")
if "frequency_changed" not in st.session_state:
    st.session_state.frequency_changed = False

frequency = st.sidebar.selectbox(
    "Run Frequency", ["Daily", "Weekly", "Monthly"],
    key="frequency_select", on_change=mark_frequency_changed
)

weekday = None
month_day = None
if frequency == "Weekly":
    weekday = st.sidebar.selectbox("Weekday", ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"])
if frequency == "Monthly":
    month_day = st.sidebar.number_input("Day of month", 1, 31, 1)

time_options = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0,15,30,45)]
time_of_day = st.sidebar.selectbox("Time of day", time_options)

missing_sch = []
if not aoi_geojson:
    missing_sch.append("Please draw an AOI on the map")
if not st.session_state.frequency_changed:
    missing_sch.append("Please adjust/confirm frequency settings")

if missing_sch:
    st.sidebar.markdown("### Required for Scheduling")
    for msg in missing_sch:
        st.sidebar.warning(msg)
else:
    st.sidebar.success("All required inputs provided.")

schedule_clicked = st.sidebar.button("Schedule job", disabled=len(missing_sch) > 0)

# 8. Execution Manual Job
if run_now_clicked:
    cfg_p = write_job_config(is_manual=True)
    status_container = st.empty()
    script_p = os.path.join(GEE_DIR, "lakedetection_headless.py")
    
    process = subprocess.Popen([sys.executable, "-u", script_p, cfg_p], 
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    full_log = ""
    for line in iter(process.stdout.readline, ""):
        full_log += line
        status_container.code(full_log)
    
    if process.wait() == 0: st.success("Manual run complete!")
    else: st.error("Manual run failed.")

# 9. Execution Task Scheduling
if schedule_clicked:
    cfg_p = write_job_config(is_manual=False)
    script_p = os.path.join(GEE_DIR, "lakedetection_headless.py")
    task_name = f"LakeDetection_{frequency}"
    python_exe = sys.executable
    
    day_map = {"Monday":"MON","Tuesday":"TUE","Wednesday":"WED","Thursday":"THU","Friday":"FRI","Saturday":"SAT","Sunday":"SUN"}

    if frequency == "Weekly":
        sch_cmd = f'schtasks /Create /SC WEEKLY /D {day_map[weekday]} /TN "{task_name}" /TR "{python_exe} {script_p} {cfg_p}" /ST {time_of_day} /F'
    elif frequency == "Daily":
        sch_cmd = f'schtasks /Create /SC DAILY /TN "{task_name}" /TR "{python_exe} {script_p} {cfg_p}" /ST {time_of_day} /F'
    else:
        sch_cmd = f'schtasks /Create /SC MONTHLY /D {month_day} /TN "{task_name}" /TR "{python_exe} {script_p} {cfg_p}" /ST {time_of_day} /F'

    if os.system(sch_cmd) == 0:
        powershell_fix = (
            f'powershell -Command "$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable; '
            f'Set-ScheduledTask -TaskName \\"{task_name}\\" -Settings $settings"'
        )
        
        if os.system(powershell_fix) == 0:
            st.sidebar.success(f"Scheduled '{task_name}' successfully!")
        else:
            st.sidebar.warning("Task created, but 'Start When Available' setting failed. Check admin rights.")
    else:
        st.sidebar.error("Failed to schedule task.")

# 10. Active Tasks Display
st.divider()
st.subheader("📋 Active Scheduled Tasks")
st.caption("Note: Tasks missed while the computer is off will run after the system is turned back on.")

try:
    output = subprocess.check_output(
        'schtasks /Query /FO CSV /V', 
        shell=True, 
        text=True, 
        encoding='cp1252', 
        errors='replace'
    )
    
    import csv
    from io import StringIO
    
    raw_data = list(csv.reader(StringIO(output)))
    
    if len(raw_data) < 2:
        st.info("No scheduled tasks found.")
    else:
        lake_tasks = [row for row in raw_data[1:] if "LakeDetection" in row[1]]

        if not lake_tasks:
            st.info("No active GEE scheduled tasks found.")

        for t in lake_tasks:
            full_task_name = t[1]
            clean_name = full_task_name.replace('\\', '')
            
            next_run = t[2]
            status = t[3]
            last_run_raw = t[5]
            result_code = t[6].strip()

            if "1999" in last_run_raw:
                last_run_display = "Never Run"
                result_display = "⚪ Pending first run"
            else:
                last_run_display = last_run_raw
                if result_code == '0':
                    result_display = "✅ Success (0)"
                else:
                    result_display = f"❌ Error ({result_code})"

            with st.expander(f"📌 {clean_name}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Next Run:** {next_run}")
                    st.write(f"**Last Run:** {last_run_display}")
                
                with col2:
                    st.write(f"**Status:** {status}")
                    st.write(f"**Last Result:** {result_display}")
                    

                if st.button(f"🗑️ Delete {clean_name}", key=f"del_{clean_name}"):
                    subprocess.run(f'schtasks /Delete /TN "{full_task_name}" /F', shell=True)
                    st.rerun()
                
except Exception as e:
    st.error(f"Could not retrieve task list: {e}")