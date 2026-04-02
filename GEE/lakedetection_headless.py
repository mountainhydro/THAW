# -*- coding: utf-8 -*-

"""
THAW - Headless Integrated Script
Meant to run in combination with the THAW dashboard

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger

Created on Feb 2 2026
"""

import math
import ee
import datetime
import time
import os
import io
import json
import glob
import sys
import numpy as np
import rasterio
from rasterio.features import shapes
from rasterio.warp import transform_geom
from sklearn.cluster import DBSCAN
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

# ============================================================
# CORE FUNCTIONS
# ============================================================
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

def build_user_drive_service(token_path):
    """
    Build a Drive client authenticated as the GEE user via saved OAuth token.
    The token is written once by the Dashboard login flow and auto-refreshed on
    every subsequent call.
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"Drive token not found at {token_path}. "
            f"Please log out of the THAW Dashboard and log in again to authorise Drive access."
        )
    creds = Credentials.from_authorized_user_file(
        token_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    return build("drive", "v3", credentials=creds, cache_discovery=False, static_discovery=False)

def delete_drive_files(token_path, file_ids):
    """
    Permanently deletes a list of Google Drive files by file ID.
    Uses the user's OAuth credentials — as file owner, permanent deletion is permitted.
    Errors are logged but do not raise so a cleanup failure never aborts the pipeline.
    """
    try:
        drive = build_user_drive_service(token_path)
    except Exception as e:
        print(f"Warning: could not build Drive service for cleanup: {e}", flush=True)
        return
    for fid in file_ids:
        try:
            drive.files().delete(fileId=fid).execute()
            print(f"Deleted Drive file: {fid}", flush=True)
        except Exception as e:
            print(f"Warning: could not delete Drive file {fid}: {e}", flush=True)

def cluster_processing(tif_path, timestamp, z_thres=-2, min_size_cluster=20, pix=6):
    """
    Performs DBSCAN clustering locally and saves results with a timestamped filename.
    """
    print(f"Starting cluster detection: {os.path.basename(tif_path)}", flush=True)
    
    with rasterio.open(tif_path) as src:
        data = src.read(1).astype(float)
        transform = src.transform
        res_x, res_y = src.res
        src_crs = src.crs

    # Mask positive values and find candidates
    data = np.where(data <= 0, data, np.nan)
    candidate = data <= z_thres
    ys, xs = np.nonzero(candidate)
    
    if len(ys) == 0:
        print("No suspicious patterns found.")
        return None, None

    coords = np.column_stack([ys, xs])
    db = DBSCAN(eps=pix, min_samples=min_size_cluster).fit(coords)
    labels = db.labels_
    
    labels_raster = np.full(candidate.shape, -1, dtype=int)
    labels_raster[ys, xs] = labels.astype(int)

    features = []
    summary_data = "Cluster_ID,Pixel_Count,Area_m2,Centroid_Lon,Centroid_Lat\n"

    for geom, val in shapes(labels_raster, mask=(labels_raster != -1), transform=transform):
        lbl = int(val)

        # Reproject from raster native CRS (e.g. UTM) to WGS84 so Folium renders correctly
        geom_wgs84 = transform_geom(src_crs, "EPSG:4326", geom)

        coords_list = geom_wgs84['coordinates'][0]
        lons = [p[0] for p in coords_list]
        lats = [p[1] for p in coords_list]
        center_lon = sum(lons) / len(lons)
        center_lat = sum(lats) / len(lats)

        pix_count = int((labels_raster == lbl).sum())

        # Use native projected pixel size (metres) if CRS is projected, else degree approximation
        if src_crs.is_projected:
            pixel_area_m2 = abs(res_x) * abs(res_y)
        else:
            m_per_deg_lat = 111320
            m_per_deg_lon = 111320 * math.cos(math.radians(center_lat))
            pixel_area_m2 = abs(res_x * m_per_deg_lon) * abs(res_y * m_per_deg_lat)
        total_area_m2 = pix_count * pixel_area_m2

        feature = {
            "type": "Feature",
            "properties": {
                "cluster_id": lbl,
                "pixel_count": pix_count,
                "area_m2": round(total_area_m2, 0)
            },
            "geometry": geom_wgs84
        }
        features.append(feature)
        summary_data += f"{lbl},{pix_count},{round(total_area_m2, 0)},{center_lon:.6f},{center_lat:.6f}\n"

    # Define paths with the shared timestamp
    output_dir = os.path.dirname(tif_path)
    poly_path = os.path.join(output_dir, f"detected_clusters_{timestamp}.geojson")
    summary_path = os.path.join(output_dir, f"cluster_summary_{timestamp}.csv")

    # Save Files
    with open(poly_path, 'w') as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)



    with open(summary_path, 'w') as f:
        f.write(summary_data)

    print("Clustering complete.", flush=True)
    return poly_path, summary_path

def export_and_download(images_to_export, reference_date, aoi, token_path, output_root, timestamp, task_name=""):
    date_str = reference_date.strftime("%Y-%m-%d")
    name_suffix = f"_{task_name}" if task_name else ""
    local_dir = os.path.join(output_root, f'Outputs_{date_str}{name_suffix}')
    os.makedirs(local_dir, exist_ok=True)

    drive_service = build_user_drive_service(token_path)

    task_list = []

    for name, img in images_to_export.items():
        file_prefix = f"{name}_{timestamp}"
        task = ee.batch.Export.image.toDrive(
            image=img, description=file_prefix, folder="GEE_Exports",
            fileNamePrefix=file_prefix, region=aoi, scale=10, maxPixels=1e12
        )
        task.start()
        task_list.append({'name': name, 'prefix': file_prefix, 'task': task, 'drive_file_id': None})
        print(f"Started GEE Task: {name}", flush=True)

    print("Waiting for GEE exports...", flush=True)
    completed = 0
    while completed < len(task_list):
        for item in task_list:
            if item.get('done'):
                continue
            status = item['task'].status()
            if status['state'] == 'COMPLETED':
                fname = f"{item['prefix']}.tif"
                res = drive_service.files().list(q=f"name='{fname}' and trashed=false", fields="files(id)").execute()
                files = res.get('files', [])
                if files:
                    file_id = files[0]['id']
                    request = drive_service.files().get_media(fileId=file_id)
                    with io.FileIO(os.path.join(local_dir, fname), 'wb') as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        d = False
                        while not d:
                            _, d = downloader.next_chunk()
                    print(f"Downloaded: {fname}", flush=True)
                    item['drive_file_id'] = file_id  # store for cleanup
                    item['done'] = True
                    completed += 1
            elif status['state'] in ['FAILED', 'CANCELLED']:
                print(f"Task {item['name']} failed: {status.get('error_message')}", flush=True)
                item['done'] = True
                completed += 1
        if completed < len(task_list):
            time.sleep(30)

    # Delete all successfully downloaded files from Drive
    drive_ids_to_delete = [item['drive_file_id'] for item in task_list if item.get('drive_file_id')]
    if drive_ids_to_delete:
        print(f"Cleaning up {len(drive_ids_to_delete)} file(s) from Google Drive...", flush=True)
        delete_drive_files(token_path, drive_ids_to_delete)

    return local_dir

def convert_to_cog(folder):
    dst_profile = cog_profiles.get("deflate")

    for tif in glob.glob(os.path.join(folder, "*.tif")):
        # Skip if it's already a COG or not a standard TIF
        if tif.endswith("_cog.tif"): 
            continue
        
        output_cog = tif.replace(".tif", "_cog.tif")
        
        print(f"Converting to COG: {os.path.basename(tif)}...", flush=True)
        try:
            cog_translate(
                tif, 
                output_cog, 
                dst_profile, 
                in_memory=False, 
                quiet=True
            )
        except Exception as e:
            print(f"Failed to convert {tif}: {e}", flush=True)
            
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

# ============================================================
# MAIN PROCESSING PIPELINE
# ============================================================
def run_pipeline(config_path):
    with open(config_path, "r") as f:
        cfg = json.load(f)

    token_path = cfg["drive_token_path"]
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    creds = Credentials.from_authorized_user_file(token_path)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    ee.Initialize(credentials=creds, project=cfg.get("project_id"))

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
    masked_diff = mean_diff.updateMask(terrain_mask)
    
    # flagging
    # water/land transition between -14(very likely land -> likelyhood water = 0) and -18(very likely water -> likelyhood water = 1)
    potential_water = masked_mean.select('VV').subtract(-14).divide(-4)
    focal_mean = potential_water.focal_mean(3) # spatial clustering: focal mean of potential water

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
# Export and download
# ============================================================
    token_path = cfg["drive_token_path"]
    exports = {
        "potential_water": potential_water,
        "z_score": zscore_mean,
        "mean_diff": masked_diff.toFloat()
    }

    local_path = export_and_download(exports, ref_date, aoi, token_path, cfg["output_root"], timestamp, task_name)
    convert_to_cog(local_path)


# ============================================================
# Clustering
# ============================================================    
    # Identify the downloaded z-score file — use the ORIGINAL (non-COG) for clustering
    # so that shapes() uses the unmodified geotransform. cog_translate can silently
    # adjust the origin to align with its tile grid, introducing a sub-pixel shift.
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
