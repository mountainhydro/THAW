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
import pandas as pd  # Added for GLOF CSV processing
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

def sanitize_name(name: str) -> str:
    """Strip characters that are invalid in folder names and collapse spaces."""
    safe = "".join(c for c in name if c.isalnum() or c in (" ", "-", "_")).strip()
    return safe.replace(" ", "_")

def write_job_config(is_manual=True, task_name=""):
    aoi_filename = "now_aoi.geojson" if is_manual else "sch_aoi.geojson"
    aoi_p = os.path.join(CONFIG_DIR, aoi_filename)

    with open(aoi_p, "w") as f:
        json.dump(
            {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": aoi_geojson}
                ],
            },
            f,
        )

    safe_name = sanitize_name(task_name)

    cfg = {
        "run_date": run_date.isoformat() if is_manual else "today",
        "aoi_geojson": aoi_p,
        "project_id": project_id,
        "drive_token_path": DRIVE_TOKEN_FILE,
        "output_root": OUTPUT_DIR,
        "task_name": safe_name,
    }

    cfg_file = "now_config.json" if is_manual else "sch_config.json"
    cfg_path = os.path.join(CONFIG_DIR, cfg_file)

    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2)

    return cfg_path

def calculate_bbox_area_km2(geometry):
    """
    Calculates approximate bounding box area (km2)
    from WGS84 polygon coordinates.
    """
    import math
    
    coords = geometry.get("coordinates", [[]])[0]
    lons = [pt[0] for pt in coords]
    lats = [pt[1] for pt in coords]
    
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    
    # Approximate conversions
    lat_km = 111.32
    lon_km = 111.32 * math.cos(math.radians((min_lat + max_lat) / 2))
    
    width = (max_lon - min_lon) * lon_km
    height = (max_lat - min_lat) * lat_km
    
    return abs(width * height)


# 2. Directory Setup
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DASH_DIR = os.path.dirname(CURRENT_DIR)                 
ROOT_DIR = os.path.dirname(DASH_DIR)                    
TEMP_DIR = os.path.join(ROOT_DIR, "temp")
DOCS_DIR = os.path.join(ROOT_DIR, "docs")  # Defined for GLOF CSV
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")
DRIVE_TOKEN_FILE = os.path.join(TEMP_DIR, "drive_token.json")
GEE_DIR = os.path.join(ROOT_DIR, "GEE")
OUTPUT_DIR = os.path.join(ROOT_DIR, "Outputs")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
os.makedirs(CONFIG_DIR, exist_ok=True)

# 3. Initialization and Auth Check
project_id, service_account_path = load_gee_creds()

st.set_page_config(layout="wide", page_title="Job Scheduler")

if not project_id:
    st.error("**No Credentials Found.** Please go to the Home page and log in first.")
    st.stop()

# 4. Data Discovery
output_folders = glob.glob(os.path.join(OUTPUT_DIR, "Outputs_*"))
dated_folders = []
for f in output_folders:
    try:
        suffix = os.path.basename(f).replace("Outputs_", "", 1)
        folder_date = datetime.strptime(suffix[:10], "%Y-%m-%d")
        dated_folders.append((f, folder_date))
    except ValueError:
        continue

dated_folders.sort(key=lambda x: x[1], reverse=True)

# 5. UI Header and Map Setup
st.title("THAW Task Manager and Scheduler")
st.success(f"Connected to Project: {project_id}")

# Use HMA center as default and removed automatic tif-based zooming
center = [36.0, 86] 
fit_bounds = None 

# Zoom-to-location manual controls
with st.expander("Zoom to location", expanded=False):
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        zoom_lat = st.number_input("Latitude", value=float(center[0]), min_value=-90.0, max_value=90.0, format="%.5f")
    with col2:
        zoom_lon = st.number_input("Longitude", value=float(center[1]), min_value=-180.0, max_value=180.0, format="%.5f")
    with col3:
        st.write("")
        st.write("")
        zoom_clicked = st.button("Go", use_container_width=True)

if zoom_clicked:
    center = [zoom_lat, zoom_lon]
    # Small window around manual input
    fit_bounds = [[zoom_lat - 0.5, zoom_lon - 0.5], [zoom_lat + 0.5, zoom_lon + 0.5]]

# Map initialized zoomed out (zoom_start=5) with no tiles to control base layer order
m = folium.Map(location=center, zoom_start=5, tiles=None) 

# Base Layer 1: Satellite (Default)
folium.TileLayer(
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", 
    name="Satellite", 
    attr="Esri",
    overlay=False
).add_to(m)

# Base Layer 2: OpenStreetMap
folium.TileLayer('openstreetmap', name="OpenStreetMap", overlay=False).add_to(m)

# Overlay Layer: Past GLOF Events
glof_file = os.path.join(DOCS_DIR, "GLOFevents2015-.csv")
if os.path.exists(glof_file):
    try:
        # Fixed encoding to cp1252 to handle special characters/non-breaking spaces
        df_events = pd.read_csv(glof_file, encoding='cp1252')
        glof_group = folium.FeatureGroup(name="Past GLOF Events")
        
        for _, row in df_events.iterrows():
            if pd.notna(row['Lat_lake']) and pd.notna(row['Lon_lake']):
                # Construct Label: Lake Name (YYYY-MM-DD)
                name = str(row['Lake_name']) if pd.notna(row['Lake_name']) else "Unknown"
                y = str(int(row['Year_exact'])) if pd.notna(row['Year_exact']) else "XXXX"
                mv = str(int(row['Month'])).zfill(2) if pd.notna(row['Month']) else "XX"
                dv = str(int(row['Day'])).zfill(2) if pd.notna(row['Day']) else "XX"
                
                label = f"{name} ({y}-{mv}-{dv})"
                
                folium.CircleMarker(
                    location=[row['Lat_lake'], row['Lon_lake']],
                    radius=5,
                    color="red",
                    weight=2,
                    fill=True,
                    fill_color="red",
                    fill_opacity=0.6,
                    tooltip=label,
                    popup=label
                ).add_to(glof_group)
        
        glof_group.add_to(m)
    except Exception as e:
        st.warning(f"Could not load GLOF markers: {e}")

# Layer Selector
folium.LayerControl(position='topright', collapsed=False).add_to(m)

if fit_bounds:
    m.fit_bounds(fit_bounds)

Draw(export=True, draw_options={"polyline":False, "circle":False, "marker":False}).add_to(m)
draw_data = st_folium(m, width=None, height=550)

aoi_geojson = None

MAX_AOI_AREA_KM2 = 60000
if draw_data and draw_data.get("all_drawings"):
    aoi_geojson = draw_data["all_drawings"][0]["geometry"]
    aoi_area = calculate_bbox_area_km2(aoi_geojson)

    if aoi_area > MAX_AOI_AREA_KM2:
        st.sidebar.error(
            f"AOI too large ({aoi_area:,.0f} km2). "
            f"Maximum allowed area is {MAX_AOI_AREA_KM2:,} km2."
        )
        aoi_geojson = None
    else:
        coords = aoi_geojson.get("coordinates", [[]])[0]
        flat_coords = [f"{lon:.5f}, {lat:.5f}" for lon, lat in coords[:3]]
        st.sidebar.info(
            f"AOI selected ({aoi_area:,.0f} km2): "
            + " | ".join(flat_coords) + "..."
        )

# Shared Task Name 
st.sidebar.markdown("---")
st.sidebar.markdown("### Task Name")
task_name_raw = st.sidebar.text_input(
    "Name",
    placeholder="e.g. Himalaya_Survey",
    help="Used in the output folder name: Outputs_YYYY-MM-DD_Name",
    label_visibility="collapsed",
)
task_name_safe = sanitize_name(task_name_raw)
has_name = bool(task_name_safe)

if task_name_raw and not has_name:
    st.sidebar.warning("Name contains only invalid characters. Use letters, numbers, hyphens or underscores.")
elif has_name:
    run_date_preview = dt_date.today().isoformat()
    st.sidebar.caption(f"Output folder: Outputs_{run_date_preview}_{task_name_safe}")
st.sidebar.markdown("---")

# 6. Sidebar Manual Run
st.sidebar.header("Manual Run")
run_date = st.sidebar.date_input("Processing Date", value=dt_date.today(), max_value=dt_date.today())

missing_now = []
if not aoi_geojson:
    missing_now.append("Draw an AOI on the map")
if not has_name:
    missing_now.append("Enter a Task Name above")

if missing_now:
    st.sidebar.markdown("**Required for analysis:**")
    for msg in missing_now:
        st.sidebar.warning(f"Note: {msg}")

run_now_clicked = st.sidebar.button("Run job now", disabled=bool(missing_now))
st.sidebar.markdown("---")

# 7. Sidebar Scheduling
st.sidebar.header("Scheduled Task")
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
    missing_sch.append("Draw an AOI on the map")
if not has_name:
    missing_sch.append("Enter a Task Name above")
if not st.session_state.frequency_changed:
    missing_sch.append("Adjust/confirm frequency settings")

if missing_sch:
    st.sidebar.markdown("**Required for Scheduling:**")
    for msg in missing_sch:
        st.sidebar.warning(f"Note: {msg}")
else:
    st.sidebar.success("All required inputs provided.")

schedule_clicked = st.sidebar.button("Schedule job", disabled=bool(missing_sch))

# 8. Execution Manual Job
if run_now_clicked:
    cfg_p = write_job_config(is_manual=True, task_name=task_name_safe)
    status_container = st.empty()
    script_p = os.path.join(GEE_DIR, "lakedetection_headless.py")
    
    process = subprocess.Popen([sys.executable, "-u", script_p, cfg_p], 
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    full_log = ""
    for line in iter(process.stdout.readline, ""):
        full_log += line
        status_container.code(full_log)
    
    if process.wait() == 0: 
        st.success("Manual run complete!")
    else: 
        st.error("Manual run failed.")

# 9. Execution Task Scheduling
if schedule_clicked:
    cfg_p = write_job_config(is_manual=False, task_name=task_name_safe)
    script_p = os.path.join(GEE_DIR, "lakedetection_headless.py")
    task_name = f"LakeDetection_{frequency}_{task_name_safe}"
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
st.subheader("Active Scheduled Tasks")
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
                result_display = "Pending first run"
            else:
                last_run_display = last_run_raw
                if result_code == '0':
                    result_display = "Success (0)"
                else:
                    result_display = f"Error ({result_code})"

            with st.expander(f"Task: {clean_name}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Next Run:** {next_run}")
                    st.write(f"**Last Run:** {last_run_display}")
                
                with col2:
                    st.write(f"**Status:** {status}")
                    st.write(f"**Last Result:** {result_display}")
                    

                if st.button(f"Delete {clean_name}", key=f"del_{clean_name}"):
                    subprocess.run(f'schtasks /Delete /TN "{full_task_name}" /F', shell=True)
                    st.rerun()
                
except Exception as e:
    st.error(f"Could not retrieve task list: {e}")