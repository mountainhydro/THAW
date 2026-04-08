# -*- coding: utf-8 -*-
"""
THAW - Core GEE processing module

All Google Earth Engine image processing functions used by the lakedetection
and tracking pipelines.

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger

Created on Feb 2 2026

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
    Convert various geometry formats to an ee.Geometry object.

    Parameters
    ----------
    geom : ee.Geometry, shapely geometry, or dict
        Accepts an ee.Geometry, Shapely geometry, or GeoJSON dict.

    Returns
    -------
    ee.Geometry
    """
    import geojson
    import shapely

    if isinstance(geom, ee.Geometry):
        return geom
    elif isinstance(geom, shapely.geometry.base.BaseGeometry):
        return ee.Geometry(shapely.geometry.mapping(geom))
    elif isinstance(geom, dict) and 'type' in geom and 'coordinates' in geom:
        return ee.Geometry(geom)
    elif isinstance(geom, geojson.Geometry):
        return ee.Geometry(geom)
    else:
        raise TypeError(
            "Unsupported geometry format. Provide an ee.Geometry, shapely geometry, or GeoJSON dict."
        )


def get_radar_azimuth(image):
    """
    Return the radar azimuth angle in radians derived from the orbit pass direction.

    ASCENDING  → π/2   (flying northward, looking east)
    DESCENDING → 3π/2  (flying southward, looking west)

    Parameters
    ----------
    image : ee.Image
        Must carry the 'orbitProperties_pass' property.

    Returns
    -------
    ee.Number
    """
    pi = ee.Number(math.pi)
    orbit_pass = image.get('orbitProperties_pass')
    radar_azimuth = ee.Algorithms.If(
        ee.String(orbit_pass).compareTo('ASCENDING').eq(0),
        pi.divide(2),
        pi.multiply(1.5)
    )
    return ee.Number(radar_azimuth)


def annotate_with_mean_angle(aoi, scale=30):
    """
    Return a mapping function that stores the spatially-averaged incidence
    angle as the 'mean_angle' image property.

    Required before applying any thinning-correction shift.

    Parameters
    ----------
    aoi : ee.Geometry
        Area over which the angle band is averaged.
    scale : int, optional
        Spatial scale in metres for the reduction (default 30).

    Returns
    -------
    Callable
        A function suitable for use with ee.ImageCollection.map().
    """
    def annotate(image):
        mean_angle = image.select('angle') \
            .reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi,
                scale=scale,
                maxPixels=1e9
            ).get('angle')
        return image.set('mean_angle', mean_angle)
    return annotate




# ============================================================
# 2. RADAR MASKING — LAKEDETECTION PIPELINE
# ============================================================

def get_radar_mask(image, dem):
    """
    Compute a valid-data mask based on radar geometry and terrain.

    Masks out layover, shadow, and low-backscatter artefacts using the
    gamma-naught volume correction. Returns a binary mask image.

    Parameters
    ----------
    image : ee.Image
        Sentinel-1 GRD image with 'VV' and 'angle' bands.
    dem : ee.Image
        Digital elevation model used for terrain geometry.

    Returns
    -------
    ee.Image
        Single-band binary mask named 'valid_mask'.
    """
    theta_i = image.select('angle')
    phi_i = ee.Terrain.aspect(theta_i).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=image.geometry(),
        scale=1000,
        maxPixels=1e9
    ).get('aspect')
    phi_i = ee.Number(phi_i)

    alpha_s = ee.Terrain.slope(dem).select('slope')
    phi_s = ee.Terrain.aspect(dem).select('aspect')
    phi_r = ee.Image.constant(phi_i).subtract(phi_s)

    deg2rad, rad2deg = math.pi / 180, 180 / math.pi
    theta_i_rad = theta_i.multiply(deg2rad)
    alpha_s_rad = alpha_s.multiply(deg2rad)
    phi_r_rad = phi_r.multiply(deg2rad)
    ninetyRad = ee.Image.constant(math.pi / 2)

    alpha_r_rad = alpha_s_rad.tan().multiply(phi_r_rad.cos()).atan()
    alpha_az = alpha_s_rad.tan().multiply(phi_r_rad.sin()).atan()
    theta_lia_rad = alpha_az.cos().multiply(theta_i_rad.subtract(alpha_r_rad).cos()).acos()
    theta_lia = theta_lia_rad.multiply(rad2deg)

    sigma0 = ee.Image.constant(10).pow(image.select('VV').divide(10))
    gamma0 = sigma0.divide(theta_i_rad.cos())
    gamma0_volume = gamma0.divide(
        ninetyRad.subtract(theta_i_rad).add(alpha_r_rad).tan()
        .divide(ninetyRad.subtract(theta_i_rad).tan()).abs()
    )
    gamma0_volume_db = ee.Image.constant(10).multiply(gamma0_volume.log10())

    alpha_r = alpha_r_rad.multiply(rad2deg)
    layover = alpha_r.gt(theta_i)
    shadow = theta_lia.gt(85)
    mask = layover.Not().And(shadow.Not()).And(gamma0_volume_db.gt(-35)).focalMedian(3)
    return mask.rename('valid_mask')


def apply_radar_mask_to_collection(collection, dem, thinning_correction=0.0):
    """
    Apply radar geometry mask to every image in a collection.

    If thinning_correction != 0 the masked image is also laterally shifted
    images to carry the 'mean_angle' property; use annotate_with_mean_angle()
    on the collection first.

    Parameters
    ----------
    collection : ee.ImageCollection
        Sentinel-1 collection with 'VV' and 'angle' bands.
    dem : ee.Image
        Digital elevation model for terrain masking.
    thinning_correction : float, optional
        Total surface lowering in metres relative to the DEM epoch (default 0).

    Returns
    -------
    ee.ImageCollection
        Collection with 'VV_masked' and 'valid_mask' bands added.
    """
    thinning_correction = float(thinning_correction)

    def wrap(image):
        mask = get_radar_mask(image, dem)
        maskedVV = image.select('VV').updateMask(mask).rename('VV_masked')
        image = image.addBands(maskedVV).addBands(mask).copyProperties(image, image.propertyNames())
        if thinning_correction != 0.0:
            pi = ee.Number(math.pi)
            theta_i = ee.Number(image.get('mean_angle')).multiply(pi.divide(180))
            tc = ee.Number(float(thinning_correction))
            radar_azimuth_rad = get_radar_azimuth(image)
            offset = theta_i.tan().multiply(tc)
            dx = offset.multiply(radar_azimuth_rad.cos())
            dy = offset.multiply(radar_azimuth_rad.sin())
            corrected_band = image.select('VV_masked')
            shifted_band = corrected_band.translate(dx, dy, 'meters', proj=corrected_band.projection())
            image = image.select(image.bandNames().remove('VV_masked')).addBands(shifted_band)
        return image

    return collection.map(wrap)


# ============================================================
# 3. HISTORICAL COLLECTION — LAKEDETECTION PIPELINE
# ============================================================

def get_historical_collection(s1, orbit_pass, doy, window, yearsBack, reference_date):
    """
    Build a historical Sentinel-1 collection centred on a day-of-year.

    For each year in the lookback period, images within ±window days of the
    target day-of-year are collected and merged into a single collection.

    Parameters
    ----------
    s1 : ee.ImageCollection
        Full Sentinel-1 collection to filter from.
    orbit_pass : str
        'ASCENDING' or 'DESCENDING'.
    doy : int
        Day-of-year of the reference date.
    window : int
        Number of days either side of the target DOY to include.
    yearsBack : int
        Number of years to look back from reference_date.
    reference_date : datetime.datetime
        Reference date defining the target DOY and the lookback end.

    Returns
    -------
    ee.ImageCollection
    """
    yearList = range(reference_date.year - yearsBack, reference_date.year)
    seasonal_list = []
    for y in yearList:
        target = datetime.datetime(y, 1, 1) + datetime.timedelta(days=doy - 1)
        start = target - datetime.timedelta(days=window)
        end = target + datetime.timedelta(days=window)
        imgs = s1.filterDate(start, end) \
            .filter(ee.Filter.eq('orbitProperties_pass', orbit_pass)) \
            .toList(100)
        seasonal_list.append(imgs)
    return ee.ImageCollection(ee.List(seasonal_list).flatten())


# ============================================================
# 4. S1 PREPROCESSING — TRACKING PIPELINE
# ============================================================

def preprocess_s1_collection(
    aoi, start_date, end_date,
    slope_rad, dem, aspect,
    glacier_geom, terrain_mask,
    thinning_correction=0.0
):
    """
    Load, correct, and optionally shift a Sentinel-1 DESCENDING collection.

    Applies gamma-naught volume correction, layover/shadow masking, and
    (if thinning_correction != 0) a lateral shift to compensate for glacier
    surface lowering since the DEM epoch.

    Parameters
    ----------
    aoi : ee.Geometry
        Area of interest for spatial filtering.
    start_date : str
        Start of the date range (ISO format).
    end_date : str
        End of the date range (ISO format).
    slope_rad : ee.Image
        Terrain slope in radians (unused directly; kept for signature compatibility).
    dem : ee.Image
        Digital elevation model.
    aspect : ee.Image
        Terrain aspect (unused directly; kept for signature compatibility).
    glacier_geom : ee.Geometry
        Glacier outline geometry (unused directly; kept for signature compatibility).
    terrain_mask : ee.Image
        Binary mask of valid terrain pixels.
    thinning_correction : float, optional
        Total surface lowering in metres relative to the DEM epoch (default 0).

    Returns
    -------
    ee.ImageCollection
        Preprocessed collection with 'VV_corrected' and 'combined_mask' bands.
    """
    thinning_correction = float(thinning_correction)

    def process_image(image, dem):
        # Radar geometry
        theta_i = image.select('angle')
        phi_i = ee.Terrain.aspect(theta_i) \
            .reduceRegion(ee.Reducer.mean(), image.geometry(), 1000) \
            .get('aspect')
        phi_i = ee.Number(phi_i)

        # Terrain geometry
        alpha_s = ee.Terrain.slope(dem).select('slope')
        phi_s = ee.Terrain.aspect(dem).select('aspect')

        phi_r = ee.Image.constant(phi_i).subtract(phi_s)

        # Convert angles to radians
        deg_to_rad = math.pi / 180
        theta_i_rad = theta_i.multiply(deg_to_rad)
        alpha_s_rad = alpha_s.multiply(deg_to_rad)
        phi_r_rad = phi_r.multiply(deg_to_rad)
        ninety_rad = ee.Image.constant(90 * deg_to_rad)

        # Slope steepness in range and azimuth
        alpha_r = (alpha_s_rad.tan().multiply(phi_r_rad.cos())).atan()
        alpha_az = (alpha_s_rad.tan().multiply(phi_r_rad.sin())).atan()

        # Local incidence angle
        theta_lia = (alpha_az.cos().multiply((theta_i_rad.subtract(alpha_r)).cos())).acos()
        theta_lia_deg = theta_lia.multiply(180 / math.pi)

        # Gamma-naught volume correction
        sigma0 = ee.Image.constant(10).pow(image.select('VV_raw').divide(10))
        gamma0 = sigma0.divide(theta_i_rad.cos())
        nominator = (ninety_rad.subtract(theta_i_rad).add(alpha_r)).tan()
        denominator = (ninety_rad.subtract(theta_i_rad)).tan()
        vol_model = nominator.divide(denominator).abs()
        gamma0_volume_db = ee.Image.constant(10).multiply(gamma0.divide(vol_model).log10())

        # Layover, shadow, and combined mask
        alpha_r_deg = alpha_r.multiply(180 / math.pi)
        layover = alpha_r_deg.gt(theta_i)
        shadow = theta_lia_deg.gt(85)
        combined_mask = (
            layover.Not().And(shadow.Not())
            .And(gamma0_volume_db.gt(-35))
            .focalMedian(3)
            .And(terrain_mask)
        )

        return image \
            .addBands(gamma0_volume_db.updateMask(combined_mask).rename('VV_corrected')) \
            .addBands(combined_mask.rename('combined_mask'))

    def preprocess(image):
        image = image.select(['VV', 'angle']).rename(['VV_raw', 'angle'])
        image = process_image(image, dem)
        if thinning_correction != 0.0:
            pi = ee.Number(math.pi)
            theta_i = ee.Number(image.get('mean_angle')).multiply(pi.divide(180))
            tc = ee.Number(float(thinning_correction))
            radar_azimuth_rad = get_radar_azimuth(image)
            offset = theta_i.tan().multiply(tc)
            dx = offset.multiply(radar_azimuth_rad.cos())
            dy = offset.multiply(radar_azimuth_rad.sin())
            corrected_band = image.select('VV_corrected')
            shifted_band = corrected_band.translate(dx, dy, 'meters', proj=corrected_band.projection())
            image = image.select(image.bandNames().remove('VV_corrected')).addBands(shifted_band)
        return image

    s1 = ee.ImageCollection('COPERNICUS/S1_GRD') \
        .filterBounds(aoi) \
        .filterDate(start_date, end_date) \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')) \
        .filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')) \
        .map(annotate_with_mean_angle(aoi)) \
        .map(preprocess)

    return s1


# ============================================================
# 5. TEMPORAL SMOOTHING — TRACKING PIPELINE
# ============================================================

def compute_temporal_spatial_mean(img, collection, spatial_radius=1):
    """
    Compute a temporally and spatially smoothed VV backscatter value.

    Averages all images in the collection within ±1 day of the input image,
    then applies a focal mean over a square kernel of the given radius.

    Parameters
    ----------
    img : ee.Image
        Reference image defining the target acquisition time.
    collection : ee.ImageCollection
        Collection to draw temporal neighbours from (same orbit pass).
    spatial_radius : int, optional
        Radius in pixels for the spatial focal mean (default 1).

    Returns
    -------
    ee.Image
        Single-band image named 'VV_smoothed'.
    """
    img_date = ee.Date(img.get('system:time_start'))
    prev_date = img_date.advance(-1, 'day')
    next_date = img_date.advance(1, 'day')

    temporal_neighbors = collection.filterDate(prev_date, next_date)
    vv_stack = ee.ImageCollection(
        temporal_neighbors.select('VV_corrected').toList(100)
    ).toBands()

    mean_temporal = vv_stack.reduce(ee.Reducer.mean())
    kernel = ee.Kernel.square(spatial_radius)
    mean_spatial_temporal = mean_temporal.reduceNeighborhood(ee.Reducer.mean(), kernel)

    return mean_spatial_temporal.rename('VV_smoothed')


def apply_temporal_spatial_smoothing_by_orbit(collection, smoothing_fn, smoothed_band_name='VV_smoothed'):
    """
    Apply temporal-spatial smoothing separately per orbit direction.

    Splits the collection into ascending and descending passes, applies the
    smoothing function within each subset (so orbits do not mix), then merges
    the results back into a single collection.

    Parameters
    ----------
    collection : ee.ImageCollection
        Preprocessed Sentinel-1 collection.
    smoothing_fn : Callable
        Function with signature (img, subcollection) → ee.Image.
    smoothed_band_name : str, optional
        Name assigned to the smoothed output band (default 'VV_smoothed').

    Returns
    -------
    ee.ImageCollection
        Collection with the smoothed band added to each image.
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
    """
    Compute a lake likelihood score from the smoothed VV backscatter.

    Applies a linear ramp between two backscatter thresholds:
        -13 dB → likelihood 0 (land)
        -17 dB → likelihood 1 (open water)
    Values outside the range are clamped to [0, 1].

    Parameters
    ----------
    img : ee.Image
        Must contain the 'VV_smoothed' band.

    Returns
    -------
    ee.Image
        Input image with 'lake_likelihood' band added.
    """
    vv = img.select('VV_smoothed')
    likelihood = vv.expression(
        'clamp((thresh_high - val) / (thresh_high - thresh_low), 0, 1)',
        {'val': vv, 'thresh_high': -13, 'thresh_low': -17}
    ).rename('lake_likelihood')
    return img.addBands(likelihood)


def simple_threshold(image, threshold=-14):
    """
    Apply a simple backscatter threshold to classify water pixels.

    Parameters
    ----------
    image : ee.Image
        Must contain the 'VV_dB' band.
    threshold : float, optional
        Backscatter threshold in dB (default -14).

    Returns
    -------
    ee.Image
        Input image with 'water_mask' band added.
    """
    water = image.select('VV_dB').lt(threshold).rename('water_mask')
    return image.addBands(water)
