# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 13:14:49 2025

@author: emiles
"""
import ee
import math
import os

def preprocess_s1_collection(aoi, start_date, end_date, slope_rad, dem, aspect, glacier_geom, terrain_mask, thinning_correction=0.0):
    thinning_correction = float(thinning_correction)
    import math
    import ee
    aoi_ee = aoi
    pi = ee.Number(math.pi)
    radar_azimuth = ee.Number(225).multiply(pi.divide(180))

    def process_image(image, dem):
        import math
    
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
        layover = alpha_r_deg.gt(theta_i)  # Can be flipped based on your masking logic
        shadow = theta_lia_deg.gt(85)      # Adjust to >85 if needed
    
        # Combine shadow and layover into a valid-data mask
        combined_mask = layover.Not().And(shadow.Not()).And(gamma0_volume_db.gt(-35)).focalMedian(3).And(terrain_mask)

        return image.addBands(gamma0_volume_db.updateMask(combined_mask).rename('VV_corrected')).addBands(combined_mask.rename('combined_mask'))



    def shift_image_laterally(image, thinning_correction, pixel_size_m=10):
        pi = ee.Number(math.pi)
        
        radar_azimuth_rad = get_radar_azimuth(image)  # should return ee.Number in radians
        thinning_correction = ee.Number(thinning_correction)
        theta_i = ee.Number(image.get('mean_angle')).multiply(ee.Number(math.pi).divide(180))# convert to radians
        offset = (theta_i.tan()).multiply(thinning_correction)
        dx = offset.multiply(radar_azimuth_rad.cos())
        dy = offset.multiply(radar_azimuth_rad.sin())
        return image.translate(dx, dy, 'meters')
      

    def shift_corrected_laterally(image, thinning_correction, pixel_size_m=10):
        pi = ee.Number(math.pi)
        # Get mean angle from property or calculate as before
        theta_i = ee.Number(image.get('mean_angle')).multiply(pi.divide(180))
        thinning_correction = ee.Number(thinning_correction)
        radar_azimuth_rad = get_radar_azimuth(image)
    
        offset = thinning_correction.multiply(theta_i.tan())
        dx = offset.multiply(radar_azimuth_rad.cos())
        dy = offset.multiply(radar_azimuth_rad.sin())
    
        # Select only the 'VV_corrected' band
        corrected_band = image.select('VV_corrected')
    
        # Shift that band only
        shifted_band = corrected_band.translate(dx, dy, 'meters',
                                                proj=corrected_band.projection())
    
        # Replace original 'VV_corrected' band with shifted one
        # Remove old band and add shifted band
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
        image = image.select(['VV', 'angle']).rename(['VV_raw', 'angle'])  # Rename raw VV
        image = process_image(image, dem)
        image = shift_corrected_laterally(image, thinning_correction, pixel_size_m=10)
        #image = apply_mask(image)
        #image = image.clip(glacier_geom)
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


def compute_temporal_spatial_mean(img, collection, spatial_radius=1):
    import ee
    
    # Get image acquisition time
    img_date = ee.Date(img.get('system:time_start'))
    
    # Define time window for temporal smoothing (e.g., ±1 day)
    prev_date = img_date.advance(-1, 'day')
    next_date = img_date.advance(1, 'day')
    
    # Filter collection to images in ±1 day window
    temporal_neighbors = collection.filterDate(prev_date, next_date)
    
    # Stack the VV_corrected bands of the temporal neighbors into one multi-band image
    vv_images = temporal_neighbors.select('VV_corrected').toList(100)
    vv_stack = ee.ImageCollection(vv_images).toBands()
    
    # Average over the temporal dimension (bands)
    # The bands are named like '0', '1', '2', ...
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
    
    # Split collection by orbit direction
    ascending = collection.filter(ee.Filter.eq('orbitProperties_pass', 'ASCENDING'))
    descending = collection.filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING'))

    # Apply smoothing separately
    ascending_smoothed = ascending.map(
        lambda img: img.addBands(smoothing_fn(img, ascending).rename(smoothed_band_name))
    )
    descending_smoothed = descending.map(
        lambda img: img.addBands(smoothing_fn(img, descending).rename(smoothed_band_name))
    )

    # Merge and return
    return ascending_smoothed.merge(descending_smoothed)
