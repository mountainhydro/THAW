# -*- coding: utf-8 -*-
"""
Headless GEE Cluster Analyzer
Integrated with THAW Dashboard Time-Tracking
Created in Feb 2026
"""
import ee
import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Local project imports
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
    export_images_to_local
)

def main():
    # --- 1. Path Resolution (Transferability) ---
    GEE_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR = os.path.dirname(GEE_DIR)

    # --- 2. Ingest Configuration ---
    if len(sys.argv) < 2:
        print("CRITICAL: No configuration file path provided.", flush=True)
        sys.exit(1)

    config_path = sys.argv[1]
    
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        print(f"CRITICAL: Error reading config file at {config_path}: {e}", flush=True)
        sys.exit(1)

    aoi_input = config.get("aoi_bbox")
    start_date = config.get("start_date")
    end_date = config.get("end_date")
    rel_out_dir = config.get("rel_output_dir")
    selected_ids = config.get("cluster_ids", [])

    final_out_dir = Path(ROOT_DIR) / rel_out_dir / "tracking_results"
    final_out_dir.mkdir(parents=True, exist_ok=True)

    print(f"--- THAW Headless Analysis Started ---", flush=True)
    print(f"Time Range: {start_date} to {end_date}", flush=True)
    print(f"AOI BBox:   {aoi_input}", flush=True)
    print(f"Outputting to: {final_out_dir}", flush=True)
    print(f"---------------------------------------", flush=True)

    # --- 3. Initialize Earth Engine ---
    try:
        ee.Initialize()
    except Exception as e:
        print(f"CRITICAL: Earth Engine initialization failed: {e}", flush=True)
        sys.exit(1)

    # --- 4. Spatial & Terrain Inputs ---
    aoi = load_aoi(aoi_input)
    glacier_geom = load_glacier_mask(aoi, buffer_m=100)
    dem, slope_rad, aspect, terrain_mask = load_dem(aoi)

    print("Clipping analysis to buffered glacier geometry...", flush=True)
    refined_aoi = glacier_geom

    try:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        target_year = (start_year + end_year) // 2
    except (ValueError, TypeError):
        target_year = datetime.now().year

    thinning_correction = get_glacier_thinning_correction(refined_aoi, target_year, dem)
    print(f"Glacier thinning correction (m): {thinning_correction:.3f}", flush=True)

    # --- 5. Processing Pipeline ---
    print("Preprocessing Sentinel-1 collection...", flush=True)
    s1_preprocessed = preprocess_s1_collection(
        refined_aoi, start_date, end_date, 
        slope_rad, dem, aspect, glacier_geom, 
        terrain_mask, thinning_correction 
    )
    
    img_count = s1_preprocessed.size().getInfo()
    print(f"✅ Found and preprocessed {img_count} Sentinel-1 images.", flush=True)

    if img_count == 0:
        print("WARNING: No imagery found for the specified period. Terminating.", flush=True)
        return

    s1_smoothed = apply_temporal_spatial_smoothing_by_orbit(
        s1_preprocessed,
        smoothing_fn=compute_temporal_spatial_mean,
        smoothed_band_name='VV_smoothed'
    )   
    
    s1_scored = s1_smoothed.map(likelihood_score)
    
    # --- 6. Export Results ---
    bands = ['VV_raw', 'VV_corrected', 'lake_likelihood']
    
    print(f"Exporting GeoTIFFs to local directory...", flush=True)
    export_images_to_local(
        s1_scored, 
        aoi, 
        bands_to_export=bands, 
        output_dir=str(final_out_dir), 
        prefix="tracking_s1"
    )
    
    print("Generating lake metrics report and visualizations...", flush=True)
    generate_lake_metrics_report(s1_scored, aoi)
    
    print(f"✅ SUCCESS: Analysis complete for clusters {selected_ids if selected_ids else 'all'}.", flush=True)

if __name__ == "__main__":
    main()