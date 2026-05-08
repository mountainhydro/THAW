# -*- coding: utf-8 -*-
import re as _re
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
import base64
from streamlit_folium import st_folium
from folium.plugins import MeasureControl, Draw, Fullscreen
from folium import MacroElement
from jinja2 import Template
from tracking_viewer import render_tracking_viewer
import matplotlib.pyplot as plt
from PIL import Image
import time as _time

# --- 1. Function Definitions ---

def _is_pid_running(pid):
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            stderr=subprocess.DEVNULL, text=True
        )
        return str(pid) in out
    except Exception:
        return False

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

def generate_tracking_report(tracking_dir, task_date, task_name, folder_path=None):
    """
    Build a fully self-contained HTML report of the tracking analysis.
    Embeds all panel images, lake area chart, and a satellite map with
    z_score overlay as an interactive Folium iframe.
    Returns (html_bytes, filename) or (None, None) if no data found.
    """
    import pandas as pd
    from tracking_viewer import (
        PANEL_CFG, _read_masked, _render_to_pil, _discover_frames
    )
    from PIL import ImageDraw, ImageFont

    frames = _discover_frames(tracking_dir)
    if not frames:
        return None, None

    PANEL_W   = 400
    TOTAL_W   = PANEL_W * 3
    CAPTION_H = 22

    # Render each frame to base64 PNG
    frame_b64 = []
    for frame in frames:
        panels, captions = [], []
        for band, cfg in PANEL_CFG.items():
            try:
                data = _read_masked(frame[band])
                im   = _render_to_pil(data, cfg["cmap"], cfg["vmin"], cfg["vmax"], cfg["nan_fill"])
                ratio = PANEL_W / im.width
                im = im.resize((PANEL_W, max(1, int(im.height * ratio))), Image.LANCZOS)
                panels.append(im)
            except Exception:
                panels.append(Image.new("RGB", (PANEL_W, PANEL_W), (80, 80, 80)))
            captions.append(cfg["label"])

        img_h    = max(p.height for p in panels)
        combined = Image.new("RGB", (TOTAL_W, img_h + CAPTION_H), (255, 255, 255))
        x = 0
        for p in panels:
            combined.paste(p, (x, 0))
            x += p.width

        draw = ImageDraw.Draw(combined)
        try:
            font = ImageFont.truetype("arial.ttf", 12)
        except Exception:
            font = ImageFont.load_default()
        for i, caption in enumerate(captions):
            cx = i * PANEL_W + PANEL_W // 2
            draw.text((cx, img_h + 4), caption, fill=(80, 80, 80), font=font, anchor="mt")

        buf = BytesIO()
        combined.save(buf, format="PNG")
        frame_b64.append(base64.b64encode(buf.getvalue()).decode())

    dates = [f["date"] for f in frames]

    # Render lake area chart
    chart_b64  = ""
    chart_meta = None
    metrics_csv = os.path.join(tracking_dir, "lake_metrics.csv")
    if os.path.isfile(metrics_csv):
        try:
            df = pd.read_csv(metrics_csv)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            fig, ax = plt.subplots(figsize=(9, 4))
            fig.patch.set_facecolor("#ffffff")
            ax.set_facecolor("#ffffff")
            ax.fill_between(df["date"], df["lower_area_km2"], df["upper_area_km2"],
                            color="#4a90d9", alpha=0.25, label="Uncertainty band")
            ax.plot(df["date"], df["mean_area_km2"],
                    color="#4a90d9", linewidth=1.8, label="Mean area")
            ax.scatter(df["date"], df["mean_area_km2"], color="#4a90d9", s=22, zorder=5)
            ax.set_xlabel("Date", fontsize=9)
            ax.set_ylabel("Lake Area (km2)", fontsize=9)
            ax.tick_params(labelsize=8)
            ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y-%m-%d"))
            fig.autofmt_xdate(rotation=30, ha="right")
            ax.legend(fontsize=8, loc="upper left")
            fig.tight_layout()
            _fw = fig.get_figwidth() * fig.dpi
            _fh = fig.get_figheight() * fig.dpi
            _ap = ax.get_position()
            _xlim = ax.get_xlim()
            chart_meta = {
                "left":     int(_ap.x0 * _fw),
                "right":    int(_ap.x1 * _fw),
                "top":      int((1 - _ap.y1) * _fh),
                "bottom":   int((1 - _ap.y0) * _fh),
                "native_w": int(_fw),
                "native_h": int(_fh),
                "xmin":     _xlim[0],
                "xmax":     _xlim[1],
            }
            buf = BytesIO()
            fig.savefig(buf, format="PNG", dpi=100)
            plt.close(fig)
            chart_b64 = base64.b64encode(buf.getvalue()).decode()
        except Exception:
            pass

    # Build cluster table HTML
    cluster_table = ""
    if folder_path:
        csv_files = glob.glob(os.path.join(folder_path, "cluster_summary*.csv"))
        if csv_files:
            csv_files.sort(key=os.path.getmtime, reverse=True)
            try:
                rows = []
                with open(csv_files[0], mode='r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows.append(row)
                if rows:
                    headers = list(rows[0].keys())
                    th = "".join(f"<th>{h}</th>" for h in headers)
                    trs = ""
                    for row in rows:
                        trs += "<tr>" + "".join(f"<td>{row.get(h,'')}</td>" for h in headers) + "</tr>"
                    cluster_table = f"""<h2>Detected Clusters</h2>
<table>
<thead><tr>{th}</tr></thead>
<tbody>{trs}</tbody>
</table>"""
            except Exception:
                pass

    # Pre-compute slider dates as matplotlib date floats so JS can map them to x-axis pixels
    dates_mpl_js = "null"
    if chart_meta:
        try:
            from matplotlib.dates import date2num as _mpl_date2num
            dates_mpl_js = json.dumps([_mpl_date2num(datetime.strptime(d, "%Y-%m-%d")) for d in dates])
        except Exception:
            pass

    # Build self-contained HTML
    dates_js  = json.dumps(dates)
    frames_js = json.dumps(frame_b64)
    title     = f"THAW Tracking Report - {task_date} {task_name}".strip()
    if chart_b64 and chart_meta:
        chart_section = (
            f'<h2>Lake Area Over Time</h2>'
            f'<div style="position:relative;display:inline-block;width:100%;max-width:1200px;margin-top:10px;">'
            f'<img id="chart-img" src="data:image/png;base64,{chart_b64}" style="display:block;width:100%;">'
            f'<canvas id="chart-canvas" style="position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;"></canvas>'
            f'</div>'
        )
    else:
        chart_section = ""

    # Build satellite + z_score Folium map
    map_iframe = ""
    if folder_path:
        z_files = glob.glob(os.path.join(folder_path, "*z_score*_cog.tif"))
        if not z_files:
            z_files = glob.glob(os.path.join(folder_path, "*z_score*.tif"))
            z_files = [f for f in z_files if not f.endswith("_cog.tif")]
        if z_files:
            try:
                from rasterio.warp import calculate_default_transform, reproject, Resampling as _RS
                from rasterio.crs import CRS as _RioCRS
                _MERC = _RioCRS.from_epsg(3857)
                VIS   = VIS_BY_LAYER["z_score"]
                tif   = z_files[0]
                with rasterio.open(tif) as src:
                    scale = min(2048 / src.width, 2048 / src.height, 1.0)
                    rw = max(1, int(src.width * scale))
                    rh = max(1, int(src.height * scale))
                    raw = src.read(1, out_shape=(rh, rw), resampling=_RS.average).astype(np.float32)
                    from rasterio.transform import from_bounds as _tfm
                    st_  = _tfm(*src.bounds, rw, rh)
                    if src.nodata is not None:
                        raw[raw == src.nodata] = np.nan
                    dt, dw, dh = calculate_default_transform(src.crs, _MERC, rw, rh, *src.bounds)
                    dst = np.full((dh, dw), np.nan, dtype=np.float32)
                    reproject(source=raw, destination=dst,
                              src_transform=st_, src_crs=src.crs,
                              dst_transform=dt, dst_crs=_MERC,
                              resampling=_RS.bilinear, src_nodata=np.nan, dst_nodata=np.nan)
                    wgs = transform_bounds(_MERC, "EPSG:4326",
                                           dt.c, dt.f + dt.e * dh,
                                           dt.c + dt.a * dw, dt.f)
                nodata_mask = np.isnan(dst)
                norm = np.clip((dst - VIS["min"]) / (VIS["max"] - VIS["min"]), 0, 1)
                norm[nodata_mask] = 0.0
                cmap_ = plt.get_cmap(VIS["palette"])
                rgba  = (cmap_(norm) * 255).astype(np.uint8)
                rgba[nodata_mask, 3] = 0
                buf_ = BytesIO()
                Image.fromarray(rgba, mode="RGBA").save(buf_, format="PNG")
                z_b64 = base64.b64encode(buf_.getvalue()).decode()

                south, west, north, east = wgs[1], wgs[0], wgs[3], wgs[2]
                lat_c = (south + north) / 2
                lon_c = (west  + east)  / 2
                fm = folium.Map(location=[lat_c, lon_c], zoom_start=12, tiles=None)
                folium.TileLayer(
                    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                    name="Satellite", attr="Esri"
                ).add_to(fm)
                folium.raster_layers.ImageOverlay(
                    image=f"data:image/png;base64,{z_b64}",
                    bounds=[[south, west], [north, east]],
                    name="Z-Score", opacity=0.7, interactive=False
                ).add_to(fm)
                # Tracking AOI bounding box from first VV_raw TIF
                trk_tifs = sorted(glob.glob(os.path.join(tracking_dir, "*VV_raw*.tif")))
                if trk_tifs:
                    try:
                        with rasterio.open(trk_tifs[0]) as _ts:
                            _b = transform_bounds(_ts.crs, "EPSG:4326", *_ts.bounds)
                        folium.Rectangle(
                            bounds=[[_b[1], _b[0]], [_b[3], _b[2]]],
                            color="#FF6B00", weight=2, dash_array="8 6",
                            fill=False, tooltip="Tracking AOI",
                        ).add_to(fm)
                        fm.fit_bounds([[_b[1], _b[0]], [_b[3], _b[2]]])
                    except Exception:
                        pass
                folium.LayerControl(collapsed=False).add_to(fm)
                map_html = fm.get_root().render()
                map_html_esc = map_html.replace("&", "&amp;").replace('"', "&quot;")
                map_iframe = (
                    f'<h2>Satellite Map with Z-Score</h2>'
                    f'<iframe srcdoc="{map_html_esc}" width="100%" height="520" '
                    f'style="border:1px solid #ddd;border-radius:4px;" '
                    f'allowfullscreen></iframe>'
                )
            except Exception as e:
                map_iframe = f"<p style='color:#888'>Map could not be generated: {e}</p>"
    first_img  = frame_b64[0] if frame_b64 else ""
    first_date = dates[0] if dates else ""
    n_frames   = len(dates)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<style>
  body {{ font-family: Arial, sans-serif; background: #f5f5f5; color: #333; margin: 0; padding: 20px; }}
  h1   {{ font-size: 1.4em; margin-bottom: 4px; }}
  h2   {{ font-size: 1.1em; color: #555; margin: 24px 0 8px; }}
  .subtitle {{ color: #777; font-size: 0.9em; margin-bottom: 20px; }}
  .slider-wrap {{ max-width: 1200px; margin-bottom: 8px; }}
  input[type=range] {{ width: 100%; }}
  #date-label {{ font-size: 0.95em; color: #444; margin: 4px 0 10px; }}
  #frame-img  {{ max-width: 1200px; width: 100%; border: 1px solid #ddd; border-radius: 4px; }}
  #chart-img  {{ max-width: 1200px; width: 100%; border: 1px solid #ddd; border-radius: 4px; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 1200px; font-size: 0.85em; margin-top: 6px; }}
  th {{ background: #4a90d9; color: #fff; padding: 7px 10px; text-align: left; }}
  td {{ padding: 6px 10px; border-bottom: 1px solid #e0e0e0; }}
  tr:nth-child(even) {{ background: #f0f4fb; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="subtitle">Generated by THAW - Sentinel-1 SAR Water Monitor</p>
{map_iframe}
<h2>Tracking Images</h2>
<div class="slider-wrap">
  <input type="range" id="slider" min="0" max="{n_frames - 1}" value="0" oninput="updateFrame(this.value)">
</div>
<div id="date-label">Date: {first_date} (1 of {n_frames})</div>
<img id="frame-img" src="data:image/png;base64,{first_img}">
{chart_section}
<script>
const dates     = {dates_js};
const frames    = {frames_js};
const chartMeta = {json.dumps(chart_meta) if chart_meta else 'null'};
const datesMpl  = {dates_mpl_js};

function drawChartIndicator(i) {{
  if (!chartMeta || !datesMpl) return;
  const canvas = document.getElementById("chart-canvas");
  if (!canvas) return;
  canvas.width  = canvas.offsetWidth;
  canvas.height = canvas.offsetHeight;
  const ctx  = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  const frac = (datesMpl[i] - chartMeta.xmin) / (chartMeta.xmax - chartMeta.xmin);
  if (frac < 0 || frac > 1) return;
  const scaleX = canvas.offsetWidth  / chartMeta.native_w;
  const scaleY = canvas.offsetHeight / chartMeta.native_h;
  const x  = (chartMeta.left + frac * (chartMeta.right - chartMeta.left)) * scaleX;
  const y0 = chartMeta.top    * scaleY;
  const y1 = chartMeta.bottom * scaleY;
  ctx.beginPath();
  ctx.moveTo(x, y0);
  ctx.lineTo(x, y1);
  ctx.strokeStyle = "rgba(220, 80, 0, 0.9)";
  ctx.lineWidth = 2;
  ctx.setLineDash([5, 3]);
  ctx.stroke();
}}

function updateFrame(i) {{
  document.getElementById("frame-img").src = "data:image/png;base64," + frames[i];
  document.getElementById("date-label").textContent = "Date: " + dates[i] + " (" + (parseInt(i)+1) + " of " + frames.length + ")";
  drawChartIndicator(i);
}}

window.addEventListener("load", function() {{ drawChartIndicator(0); }});
</script>
</body>
</html>"""

    safe_name = task_name.replace(" ", "_") if task_name else "report"
    filename  = f"THAW_tracking_{task_date}_{safe_name}.html"
    return html.encode("utf-8"), filename

def make_combined_legend(layers_present, vis_by_layer):
    """Single semi-transparent box with one gradient bar per visible layer."""
    LAYER_META = {
        "z_score":         dict(title="Z-Score",        unit=""),
        "potential_water": dict(title="Potential Water", unit=""),
        "mean_diff":       dict(title="Mean Diff",       unit=" dB"),
    }
    steps = 5
    blocks = ""
    for key, meta in LAYER_META.items():
        if key not in layers_present:
            continue
        vis  = vis_by_layer[key]
        cmap = plt.get_cmap(vis["palette"])
        stops = ", ".join(
            "#{:02x}{:02x}{:02x}".format(
                int(cmap(k/steps)[0]*255),
                int(cmap(k/steps)[1]*255),
                int(cmap(k/steps)[2]*255),
            )
            for k in range(steps + 1)
        )
        blocks += (
            '<div style="margin-bottom:10px;">'
            f'<div style="font-size:12px;font-weight:bold;margin-bottom:3px;color:#222;">{meta["title"]}</div>'
            f'<div style="height:12px;width:160px;background:linear-gradient(to right,{stops});'
            'border:1px solid #aaa;border-radius:2px;"></div>'
            '<div style="display:flex;justify-content:space-between;width:160px;">'
            f'<span style="font-size:10px;color:#222;">{vis["min"]}{meta["unit"]}</span>'
            f'<span style="font-size:10px;color:#222;">{(vis["min"]+vis["max"])/2:.1f}{meta["unit"]}</span>'
            f'<span style="font-size:10px;color:#222;">{vis["max"]}{meta["unit"]}</span>'
            '</div></div>'
        )
    if not blocks:
        return None
    html = (
        '<div style="position:fixed;bottom:40px;left:50px;z-index:9999;'
        'background:rgba(255,255,255,0.92);border:1px solid #bbb;'
        'border-radius:8px;padding:10px 14px;font-family:Arial,sans-serif;'
        'pointer-events:none;min-width:190px;color:#222;">'
        '<div style="font-size:13px;font-weight:bold;margin-bottom:8px;'
        'border-bottom:1px solid #ccc;padding-bottom:4px;color:#222;">Legend</div>'
        + blocks +
        '</div>'
    )
    return folium.Element(html)


def write_timetrack_config(folder_path, aoi, start_date, end_date, selected_ids, proj_id, drive_token_path):
    """
    Saves config using relative paths and GEE auth info to ensure transferability.
    """
    cfg_path = os.path.join(CONFIG_DIR, "timetrack_config.json")
    
    # Convert the absolute folder_path to a path relative to ROOT_DIR
    rel_output_path = os.path.relpath(folder_path, ROOT_DIR)

    # Extract task_name from folder name: "Outputs_YYYY-MM-DD_TaskName" → "TaskName"
    folder_base = os.path.basename(folder_path)
    parts = folder_base.split("_", 2)
    task_name = parts[2] if len(parts) > 2 else "tracking"

    config_data = {
        "aoi_bbox": aoi,
        "start_date": start_date,
        "end_date": end_date,
        "cluster_ids": selected_ids,
        "rel_output_dir": rel_output_path,
        "task_name": task_name,
        "project_id": proj_id,
        "drive_token_path": drive_token_path,
        "processed_at": datetime.now().isoformat()
    }
    
    with open(cfg_path, "w") as f:
        json.dump(config_data, f, indent=4)
    
    return cfg_path

def _discover_tracking_runs(folder_path):
    """Return [(label, path), …] in display order: legacy first (empty label), then Tracking 1, 2, …"""
    import re as _re_runs
    runs = []
    legacy = os.path.join(folder_path, "tracking_results")
    if os.path.isdir(legacy):
        runs.append(("Tracking", legacy))
    numbered = []
    try:
        for entry in os.scandir(folder_path):
            if not entry.is_dir():
                continue
            m = _re_runs.fullmatch(r"tracking_results_(\d+)", entry.name)
            if m:
                numbered.append((int(m.group(1)), entry.path))
    except OSError:
        pass
    for n, path in sorted(numbered, key=lambda x: x[0]):
        runs.append((f"Tracking {n}", path))
    return runs

def _get_run_status(tracking_dir):
    """Return 'idle' | 'running' | 'success' | 'failed' for one tracking dir."""
    log_files = sorted(glob.glob(os.path.join(tracking_dir, "tracking_log_*.txt")))
    if not log_files:
        return "idle"
    with open(log_files[-1], encoding="utf-8", errors="replace") as _f:
        content = _f.read()
    if "PIPELINE_SUCCESS" in content:
        return "success"
    if "PIPELINE_ERROR" in content:
        return "failed"
    return "running"

# --- 2. Directory & Auth Setup ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DASH_DIR = os.path.dirname(CURRENT_DIR)                 
ROOT_DIR = os.path.dirname(DASH_DIR)                    
TEMP_DIR = os.path.join(ROOT_DIR, "temp")
CRED_FILE = os.path.join(TEMP_DIR, "gee_credentials.txt")
DRIVE_TOKEN_FILE = os.path.join(TEMP_DIR, "drive_token.json")
GEE_DIR = os.path.join(ROOT_DIR, "GEE")
OUTPUT_DIR = os.path.join(ROOT_DIR, "Outputs")
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
os.makedirs(CONFIG_DIR, exist_ok=True)

# Load GEE Credentials (Same as Scheduler)
project_id, _ = load_gee_creds()

# --- 3. Page Configuration ---
st.set_page_config(layout="wide", page_title="Output Preview")

# Auth Check
if not project_id:
    st.error("**No Credentials Found.** Please go to the **Home** page and log in first.")
    st.stop()

st.markdown(
    """
    <style>
    [data-stale='true'] { opacity: 1 !important; transition: none !important; }
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
_DATE_RE = _re.compile(r"(\d{4}-\d{2}-\d{2})")
dated_folders = []
for f in output_folders:
    suffix = os.path.basename(f).replace("Outputs_", "", 1)
    m = _DATE_RE.search(suffix)
    if not m:
        continue
    try:
        folder_date = datetime.strptime(m.group(1), "%Y-%m-%d")
    except ValueError:
        continue
    # Everything after the matched date (and any leading underscore) is the location
    remainder = suffix[m.end():]
    location = remainder.lstrip("_")
    dated_folders.append((f, folder_date, location))
dated_folders.sort(key=lambda x: (x[1], x[2]), reverse=True)

if not dated_folders:
    st.info("No data found.")
    st.stop()

# --- 5. Sidebar Selection ---
# Determine the most recent date per location name
most_recent_per_location = {}
for f, folder_date, location in dated_folders:
    if location not in most_recent_per_location:
        most_recent_per_location[location] = folder_date

def make_display_label(folder_date, location):
    date_str = folder_date.strftime("%Y-%m-%d")
    if location:
        if most_recent_per_location.get(location) == folder_date:
            return f"{date_str} ({location}, most recent)"
        return f"{date_str} ({location})"
    # No location: fall back to old behaviour
    if folder_date == dated_folders[0][1]:
        return f"{date_str} (most recent)"
    return date_str

date_options = [make_display_label(fd, loc) for _, fd, loc in dated_folders]

selected_display = st.sidebar.selectbox("Date", date_options)
# Recover folder_path from selected index
selected_idx = date_options.index(selected_display)
folder_path, selected_folder_dt, _ = dated_folders[selected_idx]
selected_folder_date = selected_folder_dt.strftime("%Y-%m-%d")
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

# Discover all tracking runs before building the map (needed for bounding boxes)
_all_runs = _discover_tracking_runs(folder_path)

m = folium.Map(location=center, zoom_start=12)
folium.TileLayer("https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
                  attr="Google", name="Satellite").add_to(m)

draw = Draw(
    export=False,
    draw_options={'polyline': False, 'rectangle': True, 'polygon': False, 'circle': False, 'marker': False, 'circlemarker': False}
).add_to(m)

class _LimitOneDrawing(MacroElement):
    _template = Template("""
        {% macro script(this, kwargs) %}
        {{ this._parent.get_name() }}.on('draw:drawstart', function() {
            {{ this._parent.get_name() }}.eachLayer(function(fg) {
                if (!(fg instanceof L.FeatureGroup)) return;
                var toRemove = [];
                fg.eachLayer(function(l) {
                    if (l instanceof L.Polygon) toRemove.push(l);
                });
                toRemove.forEach(function(l) { fg.removeLayer(l); });
            });
        });
        {% endmacro %}
    """)

_LimitOneDrawing().add_to(m)
m.add_child(MeasureControl(position='topleft'))



from io import BytesIO
from rasterio.warp import calculate_default_transform, reproject, Resampling as _Resampling
from rasterio.crs import CRS as _RioCRS

_MERCATOR = _RioCRS.from_epsg(3857)
_COG_MAX_PX = 4096  # cap longest dimension — uses COG overview, preserves detail

@st.cache_data(show_spinner=False)
def _render_tif(tif_path, vis_min, vis_max, palette, mask_below_zero, mtime):
    """Reproject to Web Mercator, colourise, encode PNG. Cached by (path, mtime).
    Reads at up to 4096px on the longest dimension using COG internal overviews."""
    try:
        with rasterio.open(tif_path) as src:
            scale = min(_COG_MAX_PX / src.width, _COG_MAX_PX / src.height, 1.0)
            read_w = max(1, int(src.width  * scale))
            read_h = max(1, int(src.height * scale))

            raw = src.read(
                1,
                out_shape=(read_h, read_w),
                resampling=_Resampling.average,
            ).astype(np.float32)

            from rasterio.transform import from_bounds as _tfm_from_bounds
            scaled_transform = _tfm_from_bounds(*src.bounds, read_w, read_h)

            if src.nodata is not None:
                raw[raw == src.nodata] = np.nan
            if mask_below_zero:
                raw[raw < 0] = np.nan

            dst_transform, dst_w, dst_h = calculate_default_transform(
                src.crs, _MERCATOR, read_w, read_h, *src.bounds)
            dst = np.full((dst_h, dst_w), np.nan, dtype=np.float32)
            reproject(
                source=raw, destination=dst,
                src_transform=scaled_transform, src_crs=src.crs,
                dst_transform=dst_transform, dst_crs=_MERCATOR,
                resampling=_Resampling.bilinear,
                src_nodata=np.nan, dst_nodata=np.nan,
            )

        merc_w = dst_transform.c
        merc_n = dst_transform.f
        merc_e = merc_w + dst_transform.a * dst_w
        merc_s = merc_n + dst_transform.e * dst_h
        tb = transform_bounds(_MERCATOR, 'EPSG:4326', merc_w, merc_s, merc_e, merc_n)
        nodata_mask = np.isnan(dst)
        norm = np.clip((dst - vis_min) / (vis_max - vis_min), 0, 1)
        norm[nodata_mask] = 0.0
        cmap = plt.get_cmap(palette)
        rgba = (cmap(norm) * 255).astype(np.uint8)
        rgba[nodata_mask, 3] = 0
        img_io = BytesIO()
        Image.fromarray(rgba, mode='RGBA').save(img_io, format='PNG')
        img_io.seek(0)
        return base64.b64encode(img_io.read()).decode(), tb[1], tb[0], tb[3], tb[2]
    except Exception:
        return None

# Add TIF Layers
if tif_files:
    with st.spinner("Loading map layers..."):
        for tif in tif_files:
            basename = os.path.basename(tif)
            vis = get_vis_params(basename)
            # Use readable label from VIS_BY_LAYER instead of raw filename
            layer_label = next((k.replace("_", " ").title() for k in VIS_BY_LAYER if k in basename), basename)
            result = _render_tif(tif, vis['min'], vis['max'], vis['palette'],
                                 'potential_water' in basename, os.path.getmtime(tif))
            if result:
                img_b64, south, west, north, east = result
                folium.raster_layers.ImageOverlay(
                    image=f"data:image/png;base64,{img_b64}",
                    bounds=[[south, west], [north, east]],
                    name=layer_label, opacity=0.7, interactive=False
                ).add_to(m)
    if fit_bounds:
        m.fit_bounds(fit_bounds)

# Combined colour legend
layers_present = [k for k in VIS_BY_LAYER if any(k in os.path.basename(t) for t in tif_files)]
leg = make_combined_legend(layers_present, VIS_BY_LAYER)
if leg:
    m.get_root().html.add_child(leg)

# One dashed bounding box per tracking run, individually named in the LayerControl
for _bb_label, _bb_dir in _all_runs:
    _bb_tifs = sorted(glob.glob(os.path.join(_bb_dir, "*VV_raw*.tif")))
    if not _bb_tifs:
        continue
    try:
        with rasterio.open(_bb_tifs[0]) as _src:
            _b = transform_bounds(_src.crs, "EPSG:4326", *_src.bounds)
        _xmin, _ymin, _xmax, _ymax = _b
        folium.Rectangle(
            bounds=[[_ymin, _xmin], [_ymax, _xmax]],
            color="#FF6B00", weight=2, dash_array="8 6",
            fill=False,
            tooltip=f"Tracking AOI — {_bb_label}",
            name=f"Tracking AOI — {_bb_label}",
        ).add_to(m)
    except Exception:
        pass

# Handle Clusters GeoJson
geojson_files = glob.glob(os.path.join(folder_path, "detected_clusters*.geojson"))
if geojson_files:
    geojson_files.sort(key=os.path.getmtime, reverse=True)
    with open(geojson_files[0], "r", encoding="utf-8") as fh:
        gj = json.load(fh)

    # Inject centroid lat/lon into properties from geometry if not already present
    for feat in gj.get("features", []):
        props = feat.get("properties") or {}
        if "centroid_lat" not in props or "centroid_lon" not in props:
            geom = feat.get("geometry", {})
            coords = geom.get("coordinates", [])
            try:
                if geom.get("type") == "Polygon" and coords:
                    pts = coords[0]
                    lon = sum(p[0] for p in pts) / len(pts)
                    lat = sum(p[1] for p in pts) / len(pts)
                elif geom.get("type") == "Point" and coords:
                    lon, lat = coords[0], coords[1]
                else:
                    lon, lat = None, None
                if lon is not None:
                    props["centroid_lat"] = round(lat, 3)
                    props["centroid_lon"] = round(lon, 3)
                    feat["properties"] = props
            except Exception:
                pass

    _sample_props = next(
        (f["properties"] for f in gj.get("features", []) if f.get("properties")), {}
    )
    _field_map = {
        "cluster_id":   "ID",
        "area_m2":      "Area (m2)",
        "centroid_lat": "Lat",
        "centroid_lon": "Lon",
    }
    _tooltip_fields  = [f for f in _field_map if f in _sample_props]
    _tooltip_aliases = [_field_map[f] for f in _tooltip_fields]
    _tooltip = folium.GeoJsonTooltip(fields=_tooltip_fields, aliases=_tooltip_aliases) if _tooltip_fields else None
    folium.GeoJson(gj, name="All Clusters",
        style_function=lambda feat: {"color": "red", "weight": 2, "fillColor": "red", "fillOpacity": 0.1},
        tooltip=_tooltip
    ).add_to(m)

Fullscreen(
    position="topright",
    title="Expand map",
    title_cancel="Exit fullscreen",
    force_separate_button=True,
).add_to(m)
folium.LayerControl(collapsed=False).add_to(m)
map_output = st_folium(m, width="100%", height=620, returned_objects=["all_drawings"], key=f"map_{folder_path}")

if st.session_state.get("tracking_just_launched"):
    _dirs_at_launch = st.session_state.get("tracking_dirs_at_launch", set())
    _new_runs = [(l, d) for l, d in _all_runs if d not in _dirs_at_launch]
    if not _new_runs:
        st.info("Timetracking started, please wait...")
    else:
        _new_status = _get_run_status(_new_runs[-1][1])
        if _new_status == "running":
            st.info("Timetracking running, please scroll down.")
        elif _new_status in ("success", "failed"):
            st.session_state.pop("tracking_just_launched", None)
            st.session_state.pop("tracking_launched_for", None)
            st.session_state.pop("tracking_dirs_at_launch", None)
        else:
            st.info("Timetracking started, please wait...")

# Extract drawn AOI from map regardless of whether clusters exist
drawn_aoi = None
if map_output and map_output.get("all_drawings"):
    last = map_output["all_drawings"][-1]
    if last['geometry']['type'] == 'Polygon':
        coords = last['geometry']['coordinates'][0]
        lons, lats = [c[0] for c in coords], [c[1] for c in coords]
        drawn_aoi = [min(lons), min(lats), max(lons), max(lats)]

# --- 7. Data Sync & Table ---
cluster_csv_files = glob.glob(os.path.join(folder_path, "cluster_summary*.csv"))
data_rows = []
selected_ids = []

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

    if drawn_aoi:
        min_lon, min_lat, max_lon, max_lat = drawn_aoi
        for row in data_rows:
            if (min_lon <= row["Centroid_Lon"] <= max_lon and
                    min_lat <= row["Centroid_Lat"] <= max_lat):
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

# Clear "just launched" flag when the user switches to a different folder
if st.session_state.get("tracking_launched_for") != folder_path:
    st.session_state.pop("tracking_just_launched", None)
    st.session_state.pop("tracking_launched_for", None)
    st.session_state.pop("tracking_dirs_at_launch", None)

# Derive overall tracking status from all runs
tracking_status = "idle"
for _lbl, _tdir in _all_runs:
    _s = _get_run_status(_tdir)
    if _s == "running":
        tracking_status = "running"
        break
    if _s in ("success", "failed") and tracking_status == "idle":
        tracking_status = _s

st.sidebar.header("Cluster tracking over time")
base_date_dt = datetime.strptime(selected_folder_date, "%Y-%m-%d")
days_back = st.sidebar.slider("Look-back period (days)", 1, 180, 90)
calc_start = (base_date_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")
calc_end   = (base_date_dt + timedelta(days=12)).strftime("%Y-%m-%d")
st.sidebar.write(f"**Period:** {calc_start} to {calc_end}")

if drawn_aoi:
    st.sidebar.success(f"AOI Defined: {len(selected_ids)} clusters selected.")
    if tracking_status == "running":
        st.sidebar.caption("A tracking analysis is already running.")
    if st.sidebar.button("Run Tracking Analysis", disabled=(tracking_status == "running")):
        try:
            cfg_p = write_timetrack_config(folder_path, drawn_aoi, calc_start,
                                           calc_end, selected_ids,
                                           project_id, DRIVE_TOKEN_FILE)
            script_rel_path = os.path.join("GEE", "tracking_headless.py")
            subprocess.Popen(
                [sys.executable, "-u", script_rel_path, cfg_p],
                cwd=ROOT_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            tracking_status = "running"
            st.session_state["tracking_just_launched"] = True
            st.session_state["tracking_launched_for"] = folder_path
            st.session_state["tracking_dirs_at_launch"] = {d for _, d in _all_runs}
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Error: {e}")
else:
    st.sidebar.info("Draw an area of interest on the map to select clusters for tracking.")

# --- 9. Multi-Run Display ---
st.write("### Analysis Progress")

if tracking_status == "idle" and st.session_state.get("tracking_just_launched"):
    st.info("Tracking analysis starting, please wait...")
    _time.sleep(2)
    st.rerun()

if not _all_runs:
    st.info("No tracking analysis run yet for this folder.")

_, _location = dated_folders[selected_idx][1], dated_folders[selected_idx][2]

for _run_label, _run_dir in _all_runs:
    _run_status = _get_run_status(_run_dir)
    _log_files  = sorted(glob.glob(os.path.join(_run_dir, "tracking_log_*.txt")))
    _key        = _run_label or os.path.basename(_run_dir)  # stable non-empty widget key

    if _run_label:
        st.write(f"#### {_run_label}")

    if _run_status == "idle":
        if st.session_state.get("tracking_just_launched") and _run_dir == _all_runs[-1][1]:
            st.info("Tracking analysis starting, please wait...")
            _time.sleep(2)
            st.rerun()
        else:
            st.info("Run directory exists but no log found yet.")
        continue

    _status_prefix = (
        "[Running]" if _run_status == "running" else
        "[Done]"    if _run_status == "success"  else
        "[Failed]"
    )
    _expander_label = f"{_status_prefix} {_run_label}".strip()

    if _run_status == "success":
        st.success(f"Tracking analysis complete! Files saved in: {_run_dir}")

    with st.expander(_expander_label, expanded=(_run_status == "running")):
        if _log_files:
            with open(_log_files[-1], encoding="utf-8", errors="replace") as _lf:
                st.code(_lf.read())
        else:
            st.info("Starting tracking analysis, please wait...")
        if _run_status == "running":
            _pid_file = os.path.join(_run_dir, "pipeline.pid")
            if os.path.exists(_pid_file):
                try:
                    _pid = int(open(_pid_file).read().strip())
                    if st.button("Cancel", key=f"cancel_tracking_{_key}"):
                        if _is_pid_running(_pid):
                            subprocess.call(["taskkill", "/F", "/PID", str(_pid)])
                        try:
                            os.remove(_pid_file)
                        except Exception:
                            pass
                        if _log_files:
                            with open(_log_files[-1], "a", encoding="utf-8") as _lf2:
                                _lf2.write("\nPIPELINE_ERROR: Cancelled by user.\n")
                        st.rerun()
                except Exception:
                    pass

    if _run_status == "running":
        _time.sleep(3)
        st.rerun()

    if _run_status != "running":
        _viewer_title = _run_label.replace("Tracking", "Tracking Results", 1)
        render_tracking_viewer(_run_dir, title=_viewer_title)

        if os.path.isdir(_run_dir):
            st.write("---")
            if st.button("Export Tracking Report (.html)", key=f"export_{_key}"):
                with st.spinner("Generating report..."):
                    _task_name = f"{_location}_{_run_label.replace(' ', '_')}" if _run_label else _location
                    _html_bytes, _filename = generate_tracking_report(
                        _run_dir,
                        task_date=selected_folder_date,
                        task_name=_task_name,
                        folder_path=folder_path,
                    )
                if _html_bytes:
                    st.download_button(
                        label="Download Report",
                        data=_html_bytes,
                        file_name=_filename,
                        mime="text/html",
                        key=f"dl_{_key}",
                    )
                else:
                    st.warning("No tracking results found to export.")

if st.session_state.get("tracking_just_launched"):
    _time.sleep(3)
    st.rerun()
