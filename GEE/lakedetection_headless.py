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
from drive_io import Logger, export_and_download, convert_to_cog, delete_drive_files
from gee_core import get_radar_mask, apply_radar_mask_to_collection, get_historical_collection
from reporting import cluster_processing




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

    # Mosaic per orbit direction
    latest_asc = recent_asc.mosaic()
    latest_desc = recent_desc.mosaic()
    prev_asc = earlier_asc.mosaic()
    prev_desc = earlier_desc.mosaic()

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
    token_path = cfg["drive_token_path"]
    exports = {
        "potential_water": potential_water,
        "z_score": zscore_mean# ,
        # "mean_diff": masked_diff.toFloat()
    }

    local_path = export_and_download(exports, ref_date, aoi, token_path, cfg["output_root"], timestamp, task_name)
    convert_to_cog(local_path)


# ============================================================
# CLUSTERING
# ============================================================    
    z_score_files = glob.glob(os.path.join(local_path, "z_score*.tif"))
    z_score_files = [f for f in z_score_files if not f.endswith("_cog.tif")]
    if z_score_files:
        z_score_tif = z_score_files[0]
        
        # Run local clustering - PASSING THE TIMESTAMP HERE
        poly_file, summary_file = cluster_processing(z_score_tif, timestamp)
            
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
