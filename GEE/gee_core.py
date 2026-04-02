# -*- coding: utf-8 -*-
"""
THAW - Core GEE processing module

All Google Earth Engine image processing functions used by the lakedetection
and tracking pipelines. Replaces preprocessing.py and consolidates functions
previously in lakedetection_headless.py and water_detection.py.

Sections
--------
1. Geometry utilities
2. Radar masking          (lakedetection pipeline)
3. Historical collection  (lakedetection pipeline)
4. S1 preprocessing       (tracking pipeline)
5. Temporal smoothing     (tracking pipeline)
6. Water likelihood       (tracking pipeline + shared)
"""

import ee
import math
import datetime


# ============================================================
# 1. GEOMETRY UTILITIES
# ============================================================

def ensure_ee_geometry(geom):
    """
    Accepts a Shapely geometry, EE Geometry, or GeoJSON dict,
    and returns a valid ee.Geometry object.
    """
    import geojson
    import shapely

    if isinstance(geom, ee.Geometry):
        return geom
    elif isinstance(geom, shapely.geometry.base.BaseGeometry):
        geojson_dict = shapely.geometry.mapping(geom)
        return ee.Geometry(geojson_dict)
    elif isinstance(geom, dict) and 'type' in geom and 'coordinates' in geom:
        return ee.Geometry(geom)
    elif isinstance(geom, geojson.Geometry):
        return ee.Geometry(geom)
    else:
        raise TypeError("Unsupported geometry format. Provide an ee.Geometry, shapely geometry, or GeoJSON dict.")


# ============================================================
# 2. RADAR MASKING — LAKEDETECTION PIPELINE
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


# ============================================================
# 3. HISTORICAL COLLECTION — LAKEDETECTION PIPELINE
# ============================================================

def get_historical_collection(s1, orbit_pass, doy, window, yearsBack, reference_date):
    yearList = range(reference_date.year - yearsBack, reference_date.year)
    seasonal_list = []
    for y in yearList:
        target = datetime.datetime(y, 1, 1) + datetime.timedelta(days=doy - 1)
        start, end = target - datetime.timedelta(days=window), target + datetime.timedelta(days=window)
        imgs = s1.filterDate(start, end).filter(ee.Filter.eq('orbitProperties_pass', orbit_pass)).toList(100)
        seasonal_list.append(imgs)
    return ee.ImageCollection(ee.List(seasonal_list).flatten())


# ============================================================
# 4. S1 PREPROCESSING — TRACKING PIPELINE
# ============================================================

def preprocess_s1_collection(aoi, start_date, end_date, slope_rad, dem, aspect, glacier_geom, terrain_mask, thinning_correction=0.0):
    thinning_correction = float(thinning_correction)
    aoi_ee = aoi
    pi = ee.Number(math.pi)
    radar_azimuth = ee.Number(225).multiply(pi.divide(180))

    def process_image(image, dem):

        # Radar geometry
        theta_i = image.select('angle')
        phi_i = ee.Terrain.aspect(theta_i)\
            .reduceRegion(ee.Reducer.mean(), image.geometry(), 1000)\
            .get('aspect')
        phi_i = ee.Number(phi_i)

        # Terrain geometry
        alpha_s = ee.Terrain.slope(dem).select('slope')
        phi_s = ee.Terrain.aspect(dem).select('aspect')

        # Relative geometry
        phi_r = ee.Image.constant(phi_i).subtract(phi_s)

        # Convert angles to radians
        deg_to_rad = math.pi / 180
        theta_i_rad = theta_i.multiply(deg_to_rad)
        alpha_s_rad = alpha_s.multiply(deg_to_rad)
        phi_r_rad = phi_r.multiply(deg_to_rad)
        ninety_rad = ee.Image.constant(90 * deg_to_rad)

        # Slope steepness in range
        alpha_r = (alpha_s_rad.tan().multiply(phi_r_rad.cos())).atan()

        # Slope steepness in azimuth
        alpha_az = (alpha_s_rad.tan().multiply(phi_r_rad.sin())).atan()

        # Local incidence angle
        theta_lia = (alpha_az.cos().multiply((theta_i_rad.subtract(alpha_r)).cos())).acos()
        theta_lia_deg = theta_lia.multiply(180 / math.pi)

        # Gamma0
        sigma0 = ee.Image.constant(10).pow(image.select('VV_raw').divide(10))
        gamma0 = sigma0.divide(theta_i_rad.cos())
        gamma0_db = ee.Image.constant(10).multiply(gamma0.log10())

        # Volume model correction
        nominator = (ninety_rad.subtract(theta_i_rad).add(alpha_r)).tan()
        denominator = (ninety_rad.subtract(theta_i_rad)).tan()
        vol_model = nominator.divide(denominator).abs()

        gamma0_volume = gamma0.divide(vol_model)
        gamma0_volume_db = ee.Image.constant(10).multiply(gamma0_volume.log10())

        # Layover and Shadow masks
        alpha_r_deg = alpha_r.multiply(180 / math.pi)
        layover = alpha_r_deg.gt(theta_i)
        shadow = theta_lia_deg.gt(85)

        # Combine shadow and layover into a valid-data mask
        combined_mask = layover.Not().And(shadow.Not()).And(gamma0_volume_db.gt(-35)).focalMedian(3).And(terrain_mask)

        return image.addBands(gamma0_volume_db.updateMask(combined_mask).rename('VV_corrected')).addBands(combined_mask.rename('combined_mask'))

    def shift_image_laterally(image, thinning_correction, pixel_size_m=10):
        pi = ee.Number(math.pi)
        radar_azimuth_rad = get_radar_azimuth(image)
        thinning_correction = ee.Number(thinning_correction)
        theta_i = ee.Number(image.get('mean_angle')).multiply(ee.Number(math.pi).divide(180))
        offset = (theta_i.tan()).multiply(thinning_correction)
        dx = offset.multiply(radar_azimuth_rad.cos())
        dy = offset.multiply(radar_azimuth_rad.sin())
        return image.translate(dx, dy, 'meters')

    def shift_corrected_laterally(image, thinning_correction, pixel_size_m=10):
        pi = ee.Number(math.pi)
        theta_i = ee.Number(image.get('mean_angle')).multiply(pi.divide(180))
        thinning_correction = ee.Number(thinning_correction)
        radar_azimuth_rad = get_radar_azimuth(image)

        offset = thinning_correction.multiply(theta_i.tan())
        dx = offset.multiply(radar_azimuth_rad.cos())
        dy = offset.multiply(radar_azimuth_rad.sin())

        corrected_band = image.select('VV_corrected')
        shifted_band = corrected_band.translate(dx, dy, 'meters',
                                                proj=corrected_band.projection())
        image_no_corrected = image.select(image.bandNames().remove('VV_corrected'))
        result_image = image_no_corrected.addBands(shifted_band)
        return result_image

    def annotate_with_mean_angle(aoi):
        def annotate(image):
            mean_angle = image.select('angle')\
                .reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=aoi,
                    scale=30,
                    maxPixels=1e9
                ).get('angle')
            return image.set('mean_angle', mean_angle)
        return annotate

    def apply_mask(image):
        return image.updateMask(image.select('combined_mask'))

    def get_radar_azimuth(image):
        orbit_pass = image.get('orbitProperties_pass')
        pi = ee.Number(math.pi)
        radar_azimuth = ee.Algorithms.If(
            ee.String(orbit_pass).compareTo('ASCENDING').eq(0),
            pi.divide(2),
            pi.multiply(1.5)
        )
        return ee.Number(radar_azimuth)

    def preprocess(image):
        image = image.select(['VV', 'angle']).rename(['VV_raw', 'angle'])
        image = process_image(image, dem)
        image = shift_corrected_laterally(image, thinning_correction, pixel_size_m=10)
        return image

    s1 = ee.ImageCollection('COPERNICUS/S1_GRD') \
        .filterBounds(aoi_ee) \
        .filterDate(start_date, end_date) \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')) \
        .map(annotate_with_mean_angle(aoi_ee)) \
        .map(preprocess)

    return s1


# ============================================================
# 5. TEMPORAL SMOOTHING — TRACKING PIPELINE
# ============================================================

def compute_temporal_spatial_mean(img, collection, spatial_radius=1):
    # Get image acquisition time
    img_date = ee.Date(img.get('system:time_start'))

    # Define time window for temporal smoothing (e.g., +-1 day)
    prev_date = img_date.advance(-1, 'day')
    next_date = img_date.advance(1, 'day')

    # Filter collection to images in +-1 day window
    temporal_neighbors = collection.filterDate(prev_date, next_date)

    # Stack the VV_corrected bands of the temporal neighbors into one multi-band image
    vv_images = temporal_neighbors.select('VV_corrected').toList(100)
    vv_stack = ee.ImageCollection(vv_images).toBands()

    # Average over the temporal dimension (bands)
    num_imgs = vv_images.size()
    mean_temporal = vv_stack.reduce(ee.Reducer.mean())

    # Spatial smoothing with focal mean over a radius (in pixels)
    kernel = ee.Kernel.square(spatial_radius)
    mean_spatial_temporal = mean_temporal.reduceNeighborhood(ee.Reducer.mean(), kernel)

    return mean_spatial_temporal.rename('VV_smoothed')


def apply_temporal_spatial_smoothing_by_orbit(collection, smoothing_fn, smoothed_band_name='VV_smoothed'):
    """
    Applies temporal-spatial smoothing to Sentinel-1 image collection,
    processing ascending and descending orbits separately.

    Parameters:
        collection (ee.ImageCollection): Preprocessed Sentinel-1 collection.
        smoothing_fn (Callable): A function that takes (img, subcollection) and returns a smoothed image.
        smoothed_band_name (str): Name of the band to be added from smoothing.

    Returns:
        ee.ImageCollection: Collection with smoothed band added per image.
    """
    ascending = collection.filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING'))
    descending = collection.filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING'))

    ascending_smoothed = ascending.map(
        lambda img: img.addBands(smoothing_fn(img, ascending).rename(smoothed_band_name))
    )
    descending_smoothed = descending.map(
        lambda img: img.addBands(smoothing_fn(img, descending).rename(smoothed_band_name))
    )

    return ascending_smoothed.merge(descending_smoothed)


# ============================================================
# 6. WATER LIKELIHOOD
# ============================================================

def likelihood_score(img):
    vv = img.select('VV_smoothed')
    likelihood = vv.expression(
        'clamp((thresh_high - val) / (thresh_high - thresh_low), 0, 1)',
        {
            'val': vv,
            'thresh_high': -13,
            'thresh_low': -17,
        }
    ).rename('lake_likelihood')
    return img.addBands(likelihood)


def simple_threshold(image, threshold=-14):
    vv = image.select('VV_dB')
    water = vv.lt(threshold).rename('water_mask')
    return image.addBands(water)
