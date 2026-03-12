# -*- coding: utf-8 -*-
"""
THAW - Headless Tracking Analysis Script
Launched by the THAW Dashboard (Output_Preview page).
Reads a JSON config from argv[1]; all inputs/outputs resolved from there.

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger
Created Feb 2026
"""

import ee
import os
import sys
import json
import datetime
from pathlib import Path

# --- 1. Path Resolution ---
# When launched by the dashboard via subprocess, cwd is ROOT_DIR.
# Insert the GEE module directory so local imports resolve correctly.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from preprocessing import (
    preprocess_s1_collection,
    compute_temporal_spatial_mean,
    apply_temporal_spatial_smoothing_by_orbit,
)
from inputs import (
    load_aoi,
    load_glacier_mask,
    load_dem,
    get_glacier_thinning_correction,
)
from water_detection import (
    likelihood_score,
    generate_lake_metrics_report,
    export_images_via_drive,
)


# --- 2. Logger ---
class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()


# --- 3. Main Pipeline ---
def run_tracking_pipeline(config_path):

    # 3a. Load config written by the dashboard
    try:
        with open(config_path, "r") as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"CRITICAL: Could not read config at {config_path}: {e}", flush=True)
        sys.exit(1)

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
    # inputs.get_glacier_thinning_correction can write tile downloads there
    thinning_cache_dir = Path(ROOT_DIR) / "temp" / "thinning_cache"
    thinning_cache_dir.mkdir(parents=True, exist_ok=True)

    # 3b. Start logging (stdout still streams to the dashboard via PIPE)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    log_file = final_out_dir / f"tracking_log_{timestamp}.txt"
    sys.stdout = Logger(str(log_file))
    sys.stderr = sys.stdout

    print("--- THAW Tracking Analysis Started ---", flush=True)
    print(f"Time Range:   {start_date} to {end_date}", flush=True)
    print(f"AOI BBox:     {aoi_input}", flush=True)
    print(f"Output dir:   {final_out_dir_str}", flush=True)
    print(f"---------------------------------------", flush=True)

    # --- 3c. Initialise GEE using project_id from config ---
    try:
        ee.Initialize(project=project_id)
        print(f"GEE initialised (project: {project_id})", flush=True)
    except Exception as e:
        print(f"CRITICAL: GEE initialisation failed: {e}", flush=True)
        sys.exit(1)

    # --- 3d. Build Google Drive service (mirrors lakedetection_headless) ---
    service_account_path = cfg.get("service_account_path")
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build as build_gdrive
        SCOPES = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_file(
            service_account_path, scopes=SCOPES)
        drive_service = build_gdrive(
            'drive', 'v3',
            credentials=credentials,
            cache_discovery=False,
            static_discovery=False,
        )
        print("Google Drive service initialised.", flush=True)
    except Exception as e:
        print(f"CRITICAL: Could not build Drive service: {e}", flush=True)
        sys.exit(1)

    # --- 4. Spatial & Terrain Inputs ---
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
        cache_dir=str(thinning_cache_dir),   # ROOT_DIR/temp/thinning_cache
        output_dir=final_out_dir_str,        # tracking_results/
    ))
    print(f"Glacier thinning correction (m): {thinning_correction:.3f}", flush=True)

    # --- 5. Processing Pipeline ---
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

    # --- 6. Export via Drive (one task per image per band) ---
    # Filenames: tracking_s1_{band}_{img_id}.tif — matches main.py convention
    bands = ["VV_raw", "VV_corrected", "lake_likelihood"]

    print("Launching GEE Drive export tasks...", flush=True)
    export_images_via_drive(
        s1_scored,
        aoi,
        drive_service=drive_service,
        bands_to_export=bands,
        output_dir=final_out_dir_str,
        prefix="tracking_s1",
    )

    # --- 7. Metrics Report & GIF ---
    # All outputs (CSV, plot, GIF) are directed to final_out_dir via output_dir arg
    print("Generating lake metrics report...", flush=True)
    generate_lake_metrics_report(
        s1_scored,
        aoi,
        output_dir=final_out_dir_str,
    )

    print("SUCCESS: Tracking analysis complete.", flush=True)
    return "Tracking analysis complete."


# --- 8. Entry Point ---
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
