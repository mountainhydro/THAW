# -*- coding: utf-8 -*-

"""
THAW - Main processing pipeline for lake detection
Launched as a sub-process from the THAW dashboard

GEE Processing code: Dr. Evan Miles
Tool/Operationalizing: Dr. Stefan Fugger

Created on Feb 2 2026
"""

import os
import sys
import math
import ee
import datetime
import time
import io
import json
import glob
import numpy as np
import rasterio

# Path resolution for local imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
# Local imports
from gee_auth import initialize_ee, build_drive_service
from drive_io import Logger, export_and_download, resume_download, convert_to_cog, delete_drive_files, CancelledError
from gee_core import get_radar_mask, apply_radar_mask_to_collection, get_historical_collection
from reporting import cluster_processing


# ============================================================
# CHECKPOINT HELPERS
# ============================================================

CHECKPOINT_FILE = "pipeline_checkpoint.json"

def retry(fn, label, max_attempts=5, base_wait=30):
    """
    Call fn(), retrying up to max_attempts times on failure.
    Wait time doubles each attempt: 30s, 60s, 120s, 240s, 480s.
    CancelledError (user manually cancelled GEE tasks) is never retried.
    Raises the last exception if all attempts are exhausted.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except CancelledError:
            print(f"{label} cancelled by user — not retrying.", flush=True)
            raise
        except Exception as e:
            if attempt == max_attempts:
                print(f"{label} failed after {max_attempts} attempts: {e}", flush=True)
                raise
            wait = base_wait * (2 ** (attempt - 1))
            print(f"{label} failed (attempt {attempt}/{max_attempts}): {e}", flush=True)
            print(f"Retrying in {wait}s...", flush=True)
            time.sleep(wait)

def write_checkpoint(local_dir, **kwargs):
    """Write or update the checkpoint file in local_dir."""
    path = os.path.join(local_dir, CHECKPOINT_FILE)
    existing = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = json.load(f)
    existing.update(kwargs)
    with open(path, "w") as f:
        json.dump(existing, f, indent=2)

def read_checkpoint(local_dir):
    """Return checkpoint dict or None if not found."""
    path = os.path.join(local_dir, CHECKPOINT_FILE)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

def clear_checkpoint(local_dir):
    """Remove checkpoint once pipeline completes successfully."""
    path = os.path.join(local_dir, CHECKPOINT_FILE)
    if os.path.exists(path):
        os.remove(path)




# ============================================================
# MAIN PROCESSING PIPELINE
# ============================================================
def run_pipeline(config_path):
    with open(config_path, "r") as f:
        cfg = json.load(f)

    initialize_ee(cfg["drive_token_path"], cfg.get("project_id"))

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    if cfg["run_date"] == "today":
        ref_date = datetime.datetime.now()
    else:
        ref_date = datetime.datetime.strptime(cfg["run_date"], "%Y-%m-%d")
    doy = ref_date.timetuple().tm_yday
    
    date_str = ref_date.strftime("%Y-%m-%d")
    task_name = cfg.get("task_name", "")
    name_suffix = f"_{task_name}" if task_name else ""
    local_dir = os.path.join(cfg["output_root"], f'Outputs_{date_str}{name_suffix}')
    os.makedirs(local_dir, exist_ok=True)

    # logging of console outputs
    log_file = os.path.join(local_dir, f"pipeline_log_{timestamp}.txt")
    sys.stdout = Logger(log_file)
    sys.stderr = sys.stdout

    # ── Resume detection ────────────────────────────────────────────────────
    ckpt = read_checkpoint(local_dir)
    if ckpt and "download" in ckpt.get("steps_complete", []):
        print(f"Resuming incomplete pipeline from checkpoint in {local_dir}", flush=True)
        done = ckpt.get("steps_complete", [])

        local_path = local_dir

        if "cog" not in done:
            print("Step 2/3: Converting to COG...", flush=True)
            retry(lambda: convert_to_cog(local_path), label="COG conversion", max_attempts=3, base_wait=10)
            done.append("cog")
            write_checkpoint(local_dir, steps_complete=done)

        if "cluster" not in done:
            print("Step 3/3: Running cluster analysis...", flush=True)
            saved_ts = ckpt.get("timestamp", timestamp)
            z_score_files = glob.glob(os.path.join(local_path, "z_score*.tif"))
            z_score_files = [f for f in z_score_files if not f.endswith("_cog.tif")]
            if z_score_files:
                retry(lambda: cluster_processing(z_score_files[0], saved_ts), label="Clustering", max_attempts=3, base_wait=10)
            done.append("cluster")
            write_checkpoint(local_dir, steps_complete=done)

        clear_checkpoint(local_dir)
        return "Processing complete (resumed)."
    # ────────────────────────────────────────────────────────────────────────

    # Setup AOI and Terrain
    with open(cfg["aoi_geojson"]) as f:
        aoi_data = json.load(f)
    aoi = ee.Geometry.Polygon(aoi_data["features"][0]["geometry"]["coordinates"])

    srtm = ee.Image("USGS/SRTMGL1_003")
    elev = srtm.select('elevation')
    slope = ee.Terrain.slope(elev.focal_median(4))
    terrain_mask = elev.gt(3000).And(slope.focal_min(8).lt(6)).clip(aoi)

    daysBack = 90
    start = ref_date-datetime.timedelta(days=daysBack)
    doy = ref_date.timetuple().tm_yday
    windowSize = 12

    # Load image collection
    s1 = ee.ImageCollection('COPERNICUS/S1_GRD') \
        .filterBounds(aoi) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .select(['VV', 'angle'])

    # Split ASC and DESC
    s1_asc = s1 \
        .filterDate(start, ref_date) \
        .filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING')) \
        .sort('system:time_start', False)
    s1_asc = apply_radar_mask_to_collection(s1_asc, elev)

    s1_desc = s1 \
        .filterDate(start, ref_date) \
        .filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')) \
        .sort('system:time_start', False)
    s1_desc =  apply_radar_mask_to_collection(s1_desc, elev)

    # Reduce to the recent days, and the earlier days
    recent_asc = s1_asc.filterDate(ref_date-datetime.timedelta(days=13),ref_date)
    recent_desc = s1_desc.filterDate(ref_date-datetime.timedelta(days=13),ref_date)
    earlier_asc = s1_asc.filterDate(ref_date-datetime.timedelta(days=25), ref_date-datetime.timedelta(days=13))
    earlier_desc = s1_desc.filterDate(ref_date-datetime.timedelta(days=25), ref_date-datetime.timedelta(days=13))

    # Mosaic per orbit direction — fall back to most recent available image
    # if the fixed date window returns nothing (sparse coverage AOIs)
    def safe_mosaic(recent_col, full_col, label):
        n = recent_col.size().getInfo()
        if n == 0:
            print(f"Warning: no {label} images in recent window, using most recent available.", flush=True)
            return full_col.sort('system:time_start', False).limit(1).mosaic()
        return recent_col.mosaic()

    latest_asc  = safe_mosaic(recent_asc,  s1_asc,  "ASC recent")
    latest_desc = safe_mosaic(recent_desc, s1_desc, "DESC recent")
    prev_asc    = safe_mosaic(earlier_asc,  s1_asc.sort('system:time_start', False).limit(4).sort('system:time_start', True).limit(1),  "ASC earlier")
    prev_desc   = safe_mosaic(earlier_desc, s1_desc.sort('system:time_start', False).limit(4).sort('system:time_start', True).limit(1), "DESC earlier")

    # Get images from the years before within a timewindow around the doy
    hist_asc = get_historical_collection(s1, 'ASCENDING', doy, windowSize, 10, ref_date)
    hist_desc = get_historical_collection(s1, 'DESCENDING', doy, windowSize, 10, ref_date)

    # get mean and stdv from historical ASC and DESC images
    hist_asc_stats = hist_asc.reduce(
        ee.Reducer.mean().combine(
        reducer2 = ee.Reducer.stdDev(), \
        sharedInputs = True))
    hist_desc_stats = hist_desc.reduce(
        ee.Reducer.mean().combine(
            reducer2 = ee.Reducer.stdDev(), \
        sharedInputs = True))

    #hist_mean = hist_asc_stats.select('VV_mean').add(hist_desc_stats.select('VV_mean')).divide(2)

    # Compute differences, mean, z-score
    mean_img = latest_asc.add(latest_desc).divide(2)
    #mean_prev = prev_asc.add(prev_desc).divide(2)
    diff_asc = latest_asc.subtract(prev_asc)
    diff_desc = latest_desc.subtract(prev_desc)
    mean_diff = diff_asc.add(diff_desc).divide(2).focal_mean(5)

    # apply terrain and mask
    masked_mean = mean_img.updateMask(terrain_mask).focal_mean(5)
    #masked_mean_prev = mean_prev.updateMask(terrain_mask)

    
    # flagging
    # water/land transition between -14(very likely land -> likelyhood water = 0) and -18(very likely water -> likelyhood water = 1)
    potential_water = masked_mean.select('VV').subtract(-14).divide(-4)
    focal_mean = potential_water.focal_mean(3) # spatial clustering: focal mean of potential water
    masked_diff = mean_diff.updateMask(focal_mean)

    latest_asc_anomaly = latest_asc.select('VV') \
        .subtract(hist_asc_stats.select('VV_mean')) \
        .rename('asc_anomaly')
    latest_desc_anomaly = latest_desc.select('VV') \
        .subtract(hist_desc_stats.select('VV_mean')) \
        .rename('asc_anomaly')

    zscore_asc = latest_asc_anomaly \
        .divide(hist_asc_stats.select('VV_stdDev')) \
        .rename('asc_zscore')
    zscore_desc = latest_desc_anomaly \
        .divide(hist_desc_stats.select('VV_stdDev')) \
        .rename('asc_zscore')
    zscore_mean = zscore_asc.add(zscore_desc).divide(2).focal_mean(3).updateMask(focal_mean)

    
# ============================================================
# EXPORT AND DOWNLOAD
# ============================================================
    token_path   = cfg["drive_token_path"]
    export_names = ["potential_water", "z_score"]
    exports = {
        "potential_water": potential_water,
        "z_score": zscore_mean,
    }

    # Write initial checkpoint so dashboard can detect this run
    write_checkpoint(local_dir,
        timestamp=timestamp,
        task_name=task_name,
        ref_date=date_str,
        export_names=export_names,
        steps_complete=[],
    )

    ckpt = read_checkpoint(local_dir)
    done = ckpt.get("steps_complete", [])

    if "download" not in done:
        print("Step 1/3: Exporting and downloading from GEE...", flush=True)
        local_path = retry(
            lambda: export_and_download(
                exports, ref_date, aoi, token_path,
                cfg["output_root"], timestamp, task_name
            ),
            label="Download",
        )
        write_checkpoint(local_dir, steps_complete=done + ["download"])
        done.append("download")
    else:
        print("Step 1/3: Download already complete, skipping.", flush=True)
        local_path = local_dir


# ============================================================
# COG CONVERSION
# ============================================================
    if "cog" not in done:
        print("Step 2/3: Converting to COG...", flush=True)
        retry(
            lambda: convert_to_cog(local_path),
            label="COG conversion",
            max_attempts=3,
            base_wait=10,
        )
        done.append("cog")
        write_checkpoint(local_dir, steps_complete=done)
    else:
        print("Step 2/3: COG conversion already complete, skipping.", flush=True)


# ============================================================
# CLUSTERING
# ============================================================
    if "cluster" not in done:
        print("Step 3/3: Running cluster analysis...", flush=True)
        z_score_files = glob.glob(os.path.join(local_path, "z_score*.tif"))
        z_score_files = [f for f in z_score_files if not f.endswith("_cog.tif")]
        if z_score_files:
            z_score_tif = z_score_files[0]
            retry(
                lambda: cluster_processing(z_score_tif, timestamp),
                label="Clustering",
                max_attempts=3,
                base_wait=10,
            )
        done.append("cluster")
        write_checkpoint(local_dir, steps_complete=done)
    else:
        print("Step 3/3: Clustering already complete, skipping.", flush=True)

    clear_checkpoint(local_dir)
    return "Processing complete."

# ============================================================
# SCRIPT ENTRY
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("ERROR: No config path provided!")
        sys.exit(1)

    config_path = sys.argv[1]  # <-- consistent with Streamlit
    try:
        print("Starting pipeline...", flush=True)
        msg = run_pipeline(config_path)
        print(f"PIPELINE_SUCCESS: {msg}", flush=True)
    except Exception as e:
        print(f"PIPELINE_ERROR: {str(e)}", flush=True)
        sys.exit(1)
