# -*- coding: utf-8 -*-

"""
THAW - Headless tracking analysis script
Launched as a sub-process from the THAW dashboard (Output_and_Tracking page).

GEE Processing code: Dr. Evan Miles
Tool/Operationalizing: Dr. Stefan Fugger
Created on Feb 2 2026
"""

import ee
import os
import sys
import json
import datetime
from pathlib import Path

# Path resolution for local imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
    
# Local imports
from gee_core import (
    preprocess_s1_collection,
    compute_temporal_spatial_mean,
    apply_temporal_spatial_smoothing_by_orbit,
    likelihood_score,
)
from thinning import (
    load_aoi,
    load_glacier_mask,
    load_dem,
    get_glacier_thinning_correction,
)
from reporting import generate_lake_metrics_report
from drive_io import Logger, export_images_via_drive
from gee_auth import initialize_ee, build_drive_service


# ============================================================
# MAIN PROCESSING PIPELINE
# ============================================================
def run_tracking_pipeline(config_path):

    with open(config_path, "r") as f:
        cfg = json.load(f)

    aoi_input   = cfg.get("aoi_bbox")
    start_date  = cfg.get("start_date")
    end_date    = cfg.get("end_date")
    project_id  = cfg.get("project_id")
    rel_out_dir = cfg.get("rel_output_dir")

    # ROOT_DIR is the cwd when launched by the dashboard.
    # Explicitly chdir to it so relative paths inside inputs.py
    # (e.g. thinning_cache) resolve against ROOT_DIR.
    ROOT_DIR = os.getcwd()
    os.chdir(ROOT_DIR)

    final_out_dir = Path(ROOT_DIR) / rel_out_dir / "tracking_results"
    final_out_dir.mkdir(parents=True, exist_ok=True)
    final_out_dir_str = str(final_out_dir)

    # Pre-create thinning cache at ROOT_DIR/temp/thinning_cache so
    # get_glacier_thinning_correction can write tile downloads there
    thinning_cache_dir = Path(ROOT_DIR) / "temp" / "thinning_cache"
    thinning_cache_dir.mkdir(parents=True, exist_ok=True)

    # logging of console outputs
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    log_file = final_out_dir / f"tracking_log_{timestamp}.txt"
    sys.stdout = Logger(str(log_file))
    sys.stderr = sys.stdout

    print("--- THAW Tracking Analysis Started ---", flush=True)
    print(f"Time Range:   {start_date} to {end_date}", flush=True)
    print(f"AOI BBox:     {aoi_input}", flush=True)
    print(f"Output dir:   {final_out_dir_str}", flush=True)
    print("---------------------------------------", flush=True)

    initialize_ee(cfg.get("drive_token_path"), project_id)
    drive_service = build_drive_service(cfg.get("drive_token_path"))
    print(f"GEE initialised (project: {project_id})", flush=True)


# ============================================================
# SPATIAL AND TERRAIN INPUTS
# ============================================================
    aoi          = load_aoi(aoi_input)
    glacier_geom = load_glacier_mask(aoi, buffer_m=100, output_dir=final_out_dir_str)
    dem, slope_rad, aspect, terrain_mask = load_dem(aoi)

    # Headless mode always clips to the glacier geometry
    print("Clipping analysis to buffered glacier geometry...", flush=True)
    refined_aoi = glacier_geom

    try:
        target_year = (int(start_date[:4]) + int(end_date[:4])) // 2
    except (ValueError, TypeError):
        target_year = datetime.datetime.now().year

    # Cast to plain float to prevent numpy float32 JSON-serialisation error
    thinning_correction = float(get_glacier_thinning_correction(
        refined_aoi, target_year, dem,
        cache_dir=str(thinning_cache_dir),
        output_dir=final_out_dir_str,
    ))
    print(f"Glacier thinning correction (m): {thinning_correction:.3f}", flush=True)



# ============================================================
# S1 PREPROCESSING AND SCORING
# ============================================================
    print("Preprocessing Sentinel-1 collection...", flush=True)
    s1_preprocessed = preprocess_s1_collection(
        refined_aoi, start_date, end_date,
        slope_rad, dem, aspect, glacier_geom,
        terrain_mask, thinning_correction,
    )

    img_count = s1_preprocessed.size().getInfo()
    print(f"Found and preprocessed {img_count} Sentinel-1 images.", flush=True)

    if img_count == 0:
        print("WARNING: No imagery found for the specified period. Terminating.", flush=True)
        return "No imagery found."

    s1_smoothed = apply_temporal_spatial_smoothing_by_orbit(
        s1_preprocessed,
        smoothing_fn=compute_temporal_spatial_mean,
        smoothed_band_name="VV_smoothed",
    )

    s1_scored = s1_smoothed.map(likelihood_score)


# ============================================================
# EXPORT AND DOWNLOAD
# ============================================================
    bands = ["VV_raw", "VV_corrected", "lake_likelihood"]

    print("Launching GEE Drive export tasks...", flush=True)
    export_images_via_drive(
        s1_scored,
        aoi,
        token_path=cfg.get("drive_token_path"),
        bands_to_export=bands,
        output_dir=final_out_dir_str,
        prefix="tracking_s1",
    )


# ============================================================
# REPORTING
# ============================================================
    print("Generating lake metrics report...", flush=True)
    generate_lake_metrics_report(output_dir=final_out_dir_str)

    print("SUCCESS: Tracking analysis complete.", flush=True)
    return "Tracking analysis complete."


# ============================================================
# SCRIPT ENTRY
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("CRITICAL: No config path provided.", flush=True)
        sys.exit(1)

    config_path = sys.argv[1]
    try:
        result = run_tracking_pipeline(config_path)
        print(f"PIPELINE_SUCCESS: {result}", flush=True)
    except Exception as e:
        print(f"PIPELINE_ERROR: {str(e)}", flush=True)
        sys.exit(1)
