# -*- coding: utf-8 -*-
"""
Sentinel-1 Water Detection - Headless Integrated Script
Created on Feb 2 2026
"""

import math
import ee
import datetime
import time
import os
import io
import json
import subprocess
import glob
import sys
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# ============================================================
# 0. CORE FUNCTIONS
# ============================================================


def build_drive_service(service_account_path):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials)

def get_radar_mask(image, dem):
    theta_i = image.select('angle')
    phi_i = ee.Terrain.aspect(theta_i).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=image.geometry(),
        scale=1000,
        maxPixels=1e9).get('aspect')
    phi_i = ee.Number(phi_i)

    alpha_s = ee.Terrain.slope(dem).select('slope')
    phi_s = ee.Terrain.aspect(dem).select('aspect')
    phi_r = ee.Image.constant(phi_i).subtract(phi_s)

    deg2rad, rad2deg = math.pi/180, 180/math.pi
    theta_i_rad, alpha_s_rad = theta_i.multiply(deg2rad), alpha_s.multiply(deg2rad)
    phi_r_rad = phi_r.multiply(deg2rad)
    ninetyRad = ee.Image.constant(math.pi/2)

    alpha_r_rad = alpha_s_rad.tan().multiply(phi_r_rad.cos()).atan()
    alpha_az = alpha_s_rad.tan().multiply(phi_r_rad.sin()).atan()
    theta_lia_rad = alpha_az.cos().multiply(theta_i_rad.subtract(alpha_r_rad).cos()).acos()
    theta_lia = theta_lia_rad.multiply(rad2deg)

    sigma0 = ee.Image.constant(10).pow(image.select('VV').divide(10))
    gamma0 = sigma0.divide(theta_i_rad.cos())
    gamma0_volume = gamma0.divide(ninetyRad.subtract(theta_i_rad).add(alpha_r_rad).tan()
                                  .divide(ninetyRad.subtract(theta_i_rad).tan()).abs())
    gamma0_volume_db = ee.Image.constant(10).multiply(gamma0_volume.log10())

    alpha_r = alpha_r_rad.multiply(rad2deg)
    layover, shadow = alpha_r.gt(theta_i), theta_lia.gt(85)
    mask = layover.Not().And(shadow.Not()).And(gamma0_volume_db.gt(-35)).focalMedian(3)
    return mask.rename('valid_mask')

def apply_radar_mask_to_collection(collection, dem):
    def wrap(image):
        mask = get_radar_mask(image, dem)
        maskedVV = image.select('VV').updateMask(mask).rename('VV_masked')
        return image.addBands(maskedVV).addBands(mask).copyProperties(image, image.propertyNames())
    return collection.map(wrap)

def get_historical_collection(s1, orbit_pass, doy, window, yearsBack, reference_date):
    yearList = range(reference_date.year - yearsBack, reference_date.year)
    seasonal_list = []
    for y in yearList:
        target = datetime.datetime(y, 1, 1) + datetime.timedelta(days=doy - 1)
        start, end = target - datetime.timedelta(days=window), target + datetime.timedelta(days=window)
        imgs = s1.filterDate(start, end).filter(ee.Filter.eq('orbitProperties_pass', orbit_pass)).toList(100)
        seasonal_list.append(imgs)
    return ee.ImageCollection(ee.List(seasonal_list).flatten())

def export_and_download(images_to_export, reference_date, aoi, drive_service, output_root):
    date_str = reference_date.strftime("%Y-%m-%d")
    local_dir = os.path.join(output_root, f'Outputs_{date_str}')
    os.makedirs(local_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    task_list = []

    for name, img in images_to_export.items():
        file_prefix = f"{name}_{timestamp}"
        task = ee.batch.Export.image.toDrive(
            image=img, description=file_prefix, folder="GEE_Exports",
            fileNamePrefix=file_prefix, region=aoi, scale=10, maxPixels=1e12
        )
        task.start()
        task_list.append({'name': name, 'prefix': file_prefix, 'task': task})
        print(f"Started GEE Task: {name}", flush=True)

    print("Waiting for GEE completion...", flush=True)
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
                    with io.FileIO(os.path.join(local_dir, fname), 'wb') as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        d = False
                        while not d: _, d = downloader.next_chunk()
                    print(f"Downloaded: {fname}", flush = True)
                    item['done'] = True
                    completed += 1
            elif status['state'] in ['FAILED', 'CANCELLED']:
                print(f"Task {item['name']} failed: {status.get('error_message')}", flush = True)
                item['done'] = True
                completed += 1
        if completed < len(task_list): time.sleep(30)
    return local_dir

def convert_to_cog(folder):
    for tif in glob.glob(os.path.join(folder, "*.tif")):
        if "_cog.tif" in tif: continue
        cog = tif.replace(".tif", "_cog.tif")
        subprocess.run(["gdal_translate", tif, cog, "-of", "COG", "-co", "COMPRESS=LZW"],capture_output=False)
        #print(f"COG created: {cog}")

# ============================================================
# 1. MAIN PROCESSING PIPELINE
# ============================================================

def run_pipeline(config_path):
    with open(config_path, "r") as f:
        cfg = json.load(f)

    # Initialize GEE
    ee.Initialize(project=cfg.get("project_id"))
    print("GEE initialized OK", flush= True)

    # Setup AOI and Terrain
    with open(cfg["aoi_geojson"]) as f:
        aoi_data = json.load(f)
    aoi = ee.Geometry.Polygon(aoi_data["features"][0]["geometry"]["coordinates"])
    
    srtm = ee.Image("USGS/SRTMGL1_003")
    elev = srtm.select('elevation')
    slope = ee.Terrain.slope(elev.focal_median(4))
    terrain_mask = elev.gt(3000).And(slope.focal_min(8).lt(6)).clip(aoi)

    # Dates
    ref_date = datetime.datetime.strptime(cfg["run_date"], "%Y-%m-%d")
    doy = ref_date.timetuple().tm_yday
    
    # Load and Mask S1
    s1 = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(aoi).filter(ee.Filter.eq('instrumentMode', 'IW')).select(['VV', 'angle'])
    
    s1_asc = apply_radar_mask_to_collection(s1.filterDate(ref_date - datetime.timedelta(days=90), ref_date).filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING')), elev)
    s1_desc = apply_radar_mask_to_collection(s1.filterDate(ref_date - datetime.timedelta(days=90), ref_date).filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')), elev)

    # Mosaics
    latest_asc = s1_asc.filterDate(ref_date - datetime.timedelta(days=13), ref_date).mosaic()
    latest_desc = s1_desc.filterDate(ref_date - datetime.timedelta(days=13), ref_date).mosaic()
    
    # Historical Stats
    hist_asc = get_historical_collection(s1, 'ASCENDING', doy, 12, 10, ref_date)
    hist_desc = get_historical_collection(s1, 'DESCENDING', doy, 12, 10, ref_date)
    h_asc_stats = hist_asc.reduce(ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True))
    h_desc_stats = hist_desc.reduce(ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True))

    # Computation
    mean_img = latest_asc.add(latest_desc).divide(2).updateMask(terrain_mask)
    z_asc = latest_asc.select('VV').subtract(h_asc_stats.select('VV_mean')).divide(h_asc_stats.select('VV_stdDev'))
    z_desc = latest_desc.select('VV').subtract(h_desc_stats.select('VV_mean')).divide(h_desc_stats.select('VV_stdDev'))
    z_score = z_asc.add(z_desc).divide(2).focal_mean(3).updateMask(terrain_mask)
    
    potential_water = mean_img.select('VV').subtract(-14).divide(-4).updateMask(terrain_mask)

    # Export
    drive_service = build_drive_service(cfg["service_account_path"])
    exports = {"potential_water": potential_water, "z_score": z_score}
    
    local_path = export_and_download(exports, ref_date, aoi, drive_service, cfg["output_root"])
    convert_to_cog(local_path)
    return "Processing successful"

# ============================================================
# 2. SCRIPT ENTRY
# ============================================================

if __name__ == "__main__":
    conf = "C:/Users/fugger/Documents/Lake_detection/THAW/config/now_config.json"
    
    try:
        # Start the work
        print("Starting pipeline...", flush=True)
        msg = run_pipeline(conf)
        
        # Success message
        print(f"PIPELINE_SUCCESS: {msg}", flush=True)
        
    except Exception as e:
        # Error message
        print(f"PIPELINE_ERROR: {str(e)}", flush=True)
        sys.exit(1) # Exit with error code