#!/usr/bin/env python
# coding: utf-8

# In[148]:


import math
import ee
import geemap
import datetime
import time
import os
import io
import webbrowser
import json
import subprocess
import glob
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account


# In[150]:


##################################
## 0. FUNCTIONS
##################################
def initialize_gee():
    if os.environ.get("CI") == "true":
        
        SCOPES = [
            'https://www.googleapis.com/auth/earthengine',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform'
        ]
        # 1. Use GEE token to authenticate
# =============================================================================
#         gee_credentials = json.loads(os.environ["GEE_API_KEY"])
#         credentials = Credentials(
#            None, 
#             refresh_token=gee_credentials.get("refresh_token"),
#             client_id=gee_credentials.get("client_id"),
#             client_secret=gee_credentials.get("client_secret"), 
#             token_uri="https://oauth2.googleapis.com/token",
#             scopes=SCOPES
#         )
# =============================================================================        
        
        # Use service account to authenticate
        from google.oauth2 import service_account
        key_data =  json.loads(os.environ["DRIVE_SERVICE_ACCOUNT"])
        credentials = service_account.Credentials.from_service_account_info(key_data, scopes=SCOPES)

        #Initialize
        ee.Initialize(credentials)
        print("GEE initialized in GitHub Actions")

    else:
        # Local machine (interactive credentials already stored)
        ee.Authenticate()
        ee.Initialize(project = "axial-sight-474313-f4")
        print("GEE initialized locally")


def get_radar_mask(image, dem):
    ## geometry, calculating local incident angle
    theta_i = image.select('angle')
    phi_i = ee.Terrain.aspect(theta_i).reduceRegion( \
        reducer = ee.Reducer.mean(),
        geometry = image.geometry(),
        scale = 1000,
        maxPixels = 1e9).get('aspect');
    phi_i = ee.Number(phi_i)

    alpha_s = ee.Terrain.slope(dem).select('slope')
    phi_s = ee.Terrain.aspect(dem).select('aspect')
    phi_r = ee.Image.constant(phi_i).subtract(phi_s)

    deg2rad = math.pi/180
    rad2deg = 180/math.pi
    theta_i_rad = theta_i.multiply(deg2rad)
    alpha_s_rad = alpha_s.multiply(deg2rad)
    phi_r_rad = phi_r.multiply(deg2rad)
    ninetyRad = ee.Image.constant(math.pi/2)

    alpha_r_rad = alpha_s_rad.tan().multiply(phi_r_rad.cos()).atan()
    alpha_az = alpha_s_rad.tan().multiply(phi_r_rad.sin()).atan()

    theta_lia_rad = alpha_az.cos().multiply(theta_i_rad.subtract(alpha_r_rad).cos()).acos()
    theta_lia = theta_lia_rad.multiply(rad2deg)

    ## radiometric corrections
    sigma0 = ee.Image.constant(10).pow(image.select('VV').divide(10))
    gamma0 = sigma0.divide(theta_i_rad.cos());
    gamma0_volume = gamma0.divide(ninetyRad.subtract(theta_i_rad).add(alpha_r_rad).tan() \
      .divide(ninetyRad.subtract(theta_i_rad).tan()).abs())
    gamma0_volume_db = ee.Image.constant(10).multiply(gamma0_volume.log10())

    alpha_r = alpha_r_rad.multiply(rad2deg)
    layover = alpha_r.gt(theta_i)
    shadow = theta_lia.gt(85)

    mask = layover.Not() \
        .And(shadow.Not()) \
        .And(gamma0_volume_db.gt(-35)) \
        .focalMedian(3)
    return mask.rename('valid_mask')


## applying mask to radar data
def apply_radar_mask_to_collection(collection, dem):
    def wrap(image):
        mask = get_radar_mask(image, dem)
        maskedVV = image.select('VV').updateMask(mask).rename('VV_masked')
        return (image.addBands(maskedVV)
            .addBands(mask)
            .copyProperties(image, image.propertyNames()))
    return collection.map(wrap)


## Get historic ASC or DESC within 12 days for the past 10 years
def get_historical_collection(s1, orbit_pass, doy, window, yearsBack):
    yearList = range(today.year - yearsBack, today.year)

    seasonal_list = []
    for y in yearList:
        target = datetime.datetime(y, 1, 1) + datetime.timedelta(days=doy - 1)
        start = target - datetime.timedelta(days=window)
        end = target + datetime.timedelta(days=window)
        start_gee = start.strftime('%Y-%m-%d') # Date translation for GEE
        end_gee = end.strftime('%Y-%m-%d') # Date translation for GEE

        imgs = s1.filterDate(start,end) \
            .filter(ee.Filter.eq('orbitProperties_pass', orbit_pass)) \
            .toList(100)
        seasonal_list.append(imgs)
    return ee.ImageCollection(ee.List(seasonal_list).flatten())

## Export final images to local machine via GoogleDrive (needed for filesizes)
def export_to_local_via_drive(images_to_export, today, aoi,drive_service):
    date_only = str(today.date())
    local_folder_path = os.path.join('.', f'Outputs_{date_only}')

    if not os.path.exists(local_folder_path):
        os.makedirs(local_folder_path)

    # 1. START GEE TASKS
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    drive_folder = "GEE_Exports" # Use the folder you shared with the Service Account
    task_list = []

    for name, img in images_to_export.items():
        file_prefix = f"{name}_{timestamp}"
        task = ee.batch.Export.image.toDrive(
            image=img,
            description=file_prefix,
            folder=drive_folder,
            fileNamePrefix=file_prefix,
            region=aoi,
            scale=10,
            maxPixels=1e12
        )
        task.start()
        task_list.append({'name': name, 'prefix': file_prefix, 'task': task})
        print(f"🚀 GEE Task started: {name}")


    # 2. MONITOR & DOWNLOAD
    print("\nWaiting for GEE to finish... then API will download.")
    completed_count = 0
    while completed_count < len(task_list):
        for item in task_list:
            if item.get('downloaded', False): continue

            status = item['task'].status()
            state = status['state']

            if state == 'COMPLETED':
                file_name = f"{item['prefix']}.tif"

                # Search for file ID
                query = f"name = '{file_name}' and trashed = false"
                results = drive_service.files().list(q=query, fields="files(id)").execute()
                files = results.get('files', [])

                if files:
                    file_id = files[0]['id']
                    dest = os.path.join(local_folder_path, file_name)

                    # Official API Download
                    request = drive_service.files().get_media(fileId=file_id)
                    with io.FileIO(dest, 'wb') as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()

                    print(f"✅ Downloaded: {file_name}")
                    item['downloaded'] = True
                    completed_count += 1
                else:
                    print(f"⏳ {file_name} finished in GEE, waiting for Drive to index...")

            elif state in ['FAILED', 'CANCELLED']:
                print(f"❌ {item['name']} failed.")
                item['downloaded'] = True
                completed_count += 1

        if completed_count < len(task_list):
            time.sleep(30)

    print(f"✨ All files are in {local_folder_path}")
    return local_folder_path
    
def convert_to_cog(folder):
    """
    Converts all GeoTIFFs in the folder to Web-Optimized GeoTIFFs (COG).
    """
    tifs = glob.glob(os.path.join(folder, "*.tif"))

    for tif in tifs:
        cog_path = tif.replace(".tif", "_cog.tif")

        # Skip if already exists
        if os.path.exists(cog_path):
            continue

        # Run gdal_translate to create COG
        subprocess.run([
            "gdal_translate",
            tif,
            cog_path,
            "-of", "COG",
            "-co", "COMPRESS=LZW",
            "-co", "BLOCKSIZE=512"
        ])

        print(f"🟢 Converted to COG: {cog_path}")




# In[151]:
#use function to initialize GEE based on current environment
initialize_gee()
##################################
## 1. DEFINE AOI and elevation mask
##################################

srtm = ee.Image("USGS/SRTMGL1_003")
elev = srtm.select('elevation')
slope = ee.Terrain.slope(elev.focal_median(4))
#aoi = ee.Geometry.Rectangle([80,27,89,31]) # Central Himalaya
aoi = ee.Geometry.Rectangle([85.35,28.19,85.87,28.50]) 

elevationMask = elev.gt(3000)
slopeMask = slope.focal_min(8).lt(6)
terrain_mask = elevationMask.And(slopeMask).clip(aoi)


# In[152]:


##################################
## 2. GET S1 IMAGE COLLECTIONS
##################################

## get today's date
#today = datetime.datetime.now()
## get event date
#today = datetime.datetime(2025,5,15); # Purepu
#today = datetime.datetime(2025,6,15); # Purepu
today = datetime.datetime(2025,7,5); # Purepu
today

###### Parameters #######
daysBack = 90
start = today-datetime.timedelta(days=daysBack)
doy = today.timetuple().tm_yday
windowSize = 12
#########################


## load image collection
s1 = ee.ImageCollection('COPERNICUS/S1_GRD') \
    .filterBounds(aoi) \
    .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
    .filter(ee.Filter.eq('instrumentMode', 'IW')) \
    .select(['VV', 'angle'])
#check size
print("All:", s1.size().getInfo())

# Split ASC and DESC
s1_asc = s1 \
    .filterDate(start, today) \
    .filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING')) \
    .sort('system:time_start', False)
s1_asc = apply_radar_mask_to_collection(s1_asc, elev);

s1_desc = s1 \
    .filterDate(start, today) \
    .filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')) \
    .sort('system:time_start', False)
s1_desc =  apply_radar_mask_to_collection(s1_desc, elev);

# Reduce to the recent days, and the days before
recent_asc = s1_asc.filterDate(today-datetime.timedelta(days=13),today);
recent_desc = s1_desc.filterDate(today-datetime.timedelta(days=13),today);
earlier_asc = s1_asc.filterDate(today-datetime.timedelta(days=25), today-datetime.timedelta(days=13))
earlier_desc = s1_desc.filterDate(today-datetime.timedelta(days=25), today-datetime.timedelta(days=13))
# Check sizes
print("Recent ascending:", recent_asc.filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING')).size().getInfo())
print("Recent descending:", recent_desc.filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')).size().getInfo())
print("Earlier ascending:", earlier_asc.filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING')).size().getInfo())
print("Earlier descending:", earlier_desc.filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')).size().getInfo())


# Mosaic per orbit direction
latest_asc = recent_asc.mosaic()
latest_desc = recent_desc.mosaic()
prev_asc = earlier_asc.mosaic()
prev_desc = earlier_desc.mosaic()

# Get images from the years before within a timewindow around the doy
hist_asc = get_historical_collection(s1, 'ASCENDING', doy, windowSize, 10)
hist_desc = get_historical_collection(s1, 'DESCENDING', doy, windowSize, 10)
# Check sizes
print("Historical Ascending:", hist_asc.size().getInfo())
print("Historical Descending:", hist_desc.size().getInfo())

# get mean and stdv from historical ASC and DESC images
hist_asc_stats = hist_asc.reduce(
    ee.Reducer.mean().combine(
    reducer2 = ee.Reducer.stdDev(), \
    sharedInputs = True))
hist_desc_stats = hist_desc.reduce(
    ee.Reducer.mean().combine(
        reducer2 = ee.Reducer.stdDev(), \
    sharedInputs = True))

hist_mean = hist_asc_stats.select('VV_mean').add(hist_desc_stats.select('VV_mean')).divide(2)


# In[153]:


########################################
## 4. COMPUTE DIFFERENCES, MEAN, Z-SCORE
########################################
mean_img = latest_asc.add(latest_desc).divide(2)
mean_prev = prev_asc.add(prev_desc).divide(2)
diff_asc = latest_asc.subtract(prev_asc)
diff_desc = latest_desc.subtract(prev_desc)
mean_diff = diff_asc.add(diff_desc).divide(2).focal_mean(5)

# apply terrain and mask
masked_mean = mean_img.updateMask(terrain_mask).focal_mean(5)
masked_mean_prev = mean_prev.updateMask(terrain_mask)
masked_diff = mean_diff.updateMask(terrain_mask)

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
zscore_mean = zscore_asc.add(zscore_desc).divide(2).focal_mean(3) 


# In[154]:


########################################
## 5. FLAGGING
########################################
# water/land transition between -14(very likely land -> likelyhood water = 0) and -18(very likely water -> likelyhood water = 1)
potential_water = masked_mean.select('VV').subtract(-14).divide(-4)
focal_mean = potential_water.focal_mean(3) # spatial clustering: focal mean of potential water


# In[156]:


########################################
## 5. VISUALIZATION
########################################

vis_params = {
  'min': -30,
  'max': 0,
    'palette': ['black','white']}
p1 = geemap.get_palette_colors('Spectral', n_class=7)
p2 = geemap.get_palette_colors('inferno', n_class=7)
p3 = geemap.get_palette_colors('RdBu', n_class=7)

Map = geemap.Map(basemap='SATELLITE')
Map.addLayer(masked_mean.select('VV'),vis_params, 'Masked Mean Current VV')
Map.addLayer(masked_mean_prev.select('VV'), vis_params, 'Masked Mean Prev VV')
Map.addLayer(hist_mean.select('VV_mean').updateMask(terrain_mask), vis_params, 'Masked Historic VV')
Map.addLayer(potential_water.updateMask(focal_mean), {'min': 0, 'max': 1, 'palette': ['blue']}, 'Current Water Likelihood')
Map.addLayer(zscore_mean.updateMask(focal_mean), {'min': -2, 'max':2, 'palette': p1}, 'z-score for current VV')
Map.addLayer(masked_diff.select('VV_masked'), {'min': -5, 'max': 5, 'palette': p3}, 'Recent decrease in VV')
Map.centerObject(aoi,12)
Map

map_path = os.path.abspath('my_map.html')
Map.save(map_path)
webbrowser.open('file://' + map_path)


# In[155]:


########################################
## 6. EXPORTING
########################################


drive_service = 0
## only needed when downloading to local
# =============================================================================
# # --- AUTHENTICATION ---
# SERVICE_ACCOUNT_FILE = 'serviceaccount-484310-bf7987dba1b2.json' # Point to your downloaded file
# SCOPES = ['https://www.googleapis.com/auth/drive']
# 
# creds = service_account.Credentials.from_service_account_file(
#         SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# drive_service = build('drive', 'v3', credentials=creds)
# =============================================================================

# =============================================================================
# images_to_export = {
#     'potential_water': potential_water,
#     'z_score': zscore_mean
# }
# =============================================================================

#export_to_local_via_drive(images_to_export, today, aoi, drive_service)
local_folder_path = "C:/Users/fugger/Documents/Lake_detection/THAW/Outputs_2025-07-05/"
convert_to_cog(local_folder_path)

# In[27]:


# mean > 14 AND decreasing backscatter
#decrease_flag = masked_mean.select('VV').lte(-14) \
#    .And(masked_diff.select('VV').lt(-1))
#
# determine unusual water, >= 1.5 stdevs different and backscatter in the water range
#unusual_water_asc = zscore_asc.lte(-1.5).focal_mean(3) \
#   .multiply(focal_mean.min(1))
#unusual_water_desc = zscore_desc.lte(-1.5).focal_mean(3) \
#    .multiply(focal_mean.min(1))
#
#unusual_water = unusual_water_asc.add(unusual_water_desc).updateMask(terrain_mask)
#
#AOI = potential_water.select('VV').min(1) \
#    .add(proximity) \
#    .add(zscore_mean.multiply(zscoreMean.lt(-1)).divide(-2)) \
#    .add(decrease_flag)

