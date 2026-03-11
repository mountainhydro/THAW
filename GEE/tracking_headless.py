# -*- coding: utf-8 -*-
"""
THAW - Headless Tracking Analysis Script
Integrated with the THAW dashboard for cluster time-series analysis

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger
Created in Feb 2026
"""

import ee
import os
import sys
import json
import io
import time
import datetime
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# --- 1. Path & Transferability Fix ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.append(SCRIPT_DIR)

from preprocessing import (
    preprocess_s1_collection,
    compute_temporal_spatial_mean,
    apply_temporal_spatial_smoothing_by_orbit
)
from inputs import (
    load_aoi,
    load_glacier_mask,
    load_dem,
    get_glacier_thinning_correction
)
from water_detection import (
    likelihood_score, 
    generate_lake_metrics_report, 
    export_images_to_drive # Now using Drive version
)

# --- 2. Support Functions ---
class Logger(object):
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

def build_drive_service(service_account_path):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials, 
                 cache_discovery=False, static_discovery=False)

# --- 3. Main Processing Pipeline ---
def run_tracking_pipeline(config_path):
    with open(config_path, "r") as f:
        cfg = json.load(f)

    # Pre-flight Directory Check
    Path("temp/thinning_cache").mkdir(parents=True, exist_ok=True)    
    Path("outputs").mkdir(parents=True, exist_ok=True) 

    ee.Initialize(project=cfg.get("project_id"))

    # Setup Paths
    GEE_DIR = SCRIPT_DIR
    ROOT_DIR = os.path.dirname(GEE_DIR)
    rel_out_dir = cfg.get("rel_output_dir")
    final_out_dir = Path(ROOT_DIR) / rel_out_dir / "tracking_results"
    final_out_dir.mkdir(parents=True, exist_ok=True)

    # Setup Logging
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    log_file = os.path.join(final_out_dir, f"tracking_log_{timestamp}.txt")
    sys.stdout = Logger(log_file)
    sys.stderr = sys.stdout

    # Inputs
    aoi_input = cfg.get("aoi_bbox")
    start_date = cfg.get("start_date")
    end_date = cfg.get("end_date")

    print(f"--- THAW Tracking Analysis Started ---", flush=True)
    
    # 4. Spatial & Terrain Inputs
    aoi = load_aoi(aoi_input)
    glacier_geom = load_glacier_mask(aoi, buffer_m=100)
    dem, slope_rad, aspect, terrain_mask = load_dem(aoi)
    refined_aoi = glacier_geom

    # Thinning Correction
    try:
        target_year = (int(start_date[:4]) + int(end_date[:4])) // 2
    except:
        target_year = datetime.datetime.now().year

    thinning_correction = get_glacier_thinning_correction(refined_aoi, target_year, dem)

    # 5. Processing Pipeline
    s1_preprocessed = preprocess_s1_collection(
        refined_aoi, start_date, end_date, 
        slope_rad, dem, aspect, glacier_geom, 
        terrain_mask, thinning_correction 
    )
    
    s1_smoothed = apply_temporal_spatial_smoothing_by_orbit(
        s1_preprocessed,
        smoothing_fn=compute_temporal_spatial_mean,
        smoothed_band_name='VV_smoothed'
    )   
    
    s1_scored = s1_smoothed.map(likelihood_score)

    # 6. Export via Drive (Mirroring Integrated Script)
    print("Triggering GEE Drive exports...", flush=True)
    # This calls the updated function in water_detection.py
    task_list = export_images_to_drive(
        s1_scored, 
        aoi, 
        bands_to_export=['VV_raw', 'VV_corrected', 'lake_likelihood'], 
        prefix="tracking"
    )

    drive_service = build_drive_service(cfg["service_account_path"])
    
    print("Waiting for GEE tasks and downloading from Drive...", flush=True)
    completed = 0
    while completed < len(task_list):
        for item in task_list:
            if item.get('done'): continue
            
            status = item['task'].status()
            if status['state'] == 'COMPLETED':
                fname = f"{item['prefix']}.tif"
                res = drive_service.files().list(q=f"name='{fname}' and trashed=false").execute()
                files = res.get('files', [])
                if files:
                    request = drive_service.files().get_media(fileId=files[0]['id'])
                    with io.FileIO(os.path.join(final_out_dir, fname), 'wb') as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        d = False
                        while not d: _, d = downloader.next_chunk()
                    print(f"Downloaded: {fname}", flush=True)
                    item['done'] = True
                    completed += 1
            elif status['state'] in ['FAILED', 'CANCELLED']:
                print(f"Task {item['name']} failed: {status.get('error_message')}", flush=True)
                item['done'] = True
                completed += 1
        
        if completed < len(task_list):
            time.sleep(30)
    
    # 7. Generate Metrics (now using local files)
    print("Generating lake metrics report...", flush=True)
    generate_lake_metrics_report(s1_scored, aoi)
    
    return "Tracking analysis complete."

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    config_path = sys.argv[1]
    try:
        run_tracking_pipeline(config_path)
    except Exception as e:
        print(f"PIPELINE_ERROR: {str(e)}", flush=True)
        sys.exit(1)