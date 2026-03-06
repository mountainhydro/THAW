# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 13:15:11 2025

@author: emiles
"""
from datetime import datetime
import matplotlib.dates as mdates

import ee
from shapely.geometry import mapping

def simple_threshold(image, threshold=-14):
    vv = image.select('VV_dB')
    water = vv.lt(threshold).rename('water_mask')
    return image.addBands(water)

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

import pandas as pd
import matplotlib.pyplot as plt

def save_lake_metrics_plot_and_csv(df, output_csv='lake_metrics.csv', output_png='lake_plot.png'):
    """
    Takes feature collection results (as list of dicts or GeoJSON) and saves CSV + plot.
    """
    # df = pd.DataFrame(fc_results)

    # # Convert date strings to datetime
    # df['date'] = pd.to_datetime(df['date'])
    # df = df.sort_values('date')

    # # Save CSV
    # metrics_dict = df.getInfo()

    # # Extract list of features and properties
    # features = metrics_dict['features']
    # records = [f['properties'] for f in features]
    
    # # Create DataFrame
    # metrics_df = pd.DataFrame(records)
    
    # Now you can save it
    df.to_csv(output_csv, index=False)


    date_objs = [datetime.strptime(d, '%Y-%m-%d') for d in df['date']]
    # Plot total water area and uncertainty
    plt.figure(figsize=(10, 6))
    # plt.plot(df['date'], df['prob_gt_0_5'] * 100, label='Likelihood > 0.5', color='blue')
    # plt.plot(df['date'], df['prob_eq_1'] * 100, label='Likelihood = 1.0', color='black', linestyle='--')
    # plt.plot(df['date'], df['prob_gt_0'] * 100, label='Likelihood > 0', color='cyan', linestyle=':')
    plt.fill_between(date_objs, df['upper_area_km2'], df['lower_area_km2'], color='lightblue', alpha=0.5, label='Uncertainty')
    plt.plot(date_objs, df['mean_area_km2'], label='Likelihood > 0.5', color='blue')
    plt.xlabel('Date')
    plt.ylabel('Water Area (km²)')
    plt.title('Lake Area over Time (Estimated from Likelihood)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_png)

    print(f"✅ Saved plot to {output_png} and data to {output_csv}")

def make_gif(image_paths, gif_path, fps=2):
    frames = [Image.open(img_path) for img_path in image_paths]
    duration = int(1000 / fps)  # duration per frame in milliseconds
    frames[0].save(
        gif_path,
        save_all=True,
        append_images=frames[1:],
        duration=duration,
        loop=0
    )
    


def generate_lake_metrics_report(
    lake_likelihood_collection,
    aoi,
    output_dir="outputs",
    gif_output_path="outputs/lake_monitoring.gif",
    raw_band="VV_raw",
    processed_band="VV_smoothed",
    likelihood_band="lake_likelihood",
    csv_filename="outputs/lake_metrics.csv",
    png_filename="outputs/lake_metrics_plot.png"
):
    """
    Generates lake metrics from likelihood image collection and saves CSV, plot, and a GIF visualization.

    Args:
        lake_likelihood_collection (ee.ImageCollection): Collection with raw, processed, and lake likelihood bands.
        aoi (ee.Geometry): Area of interest.
        output_dir (str): Directory to save outputs.
        gif_output_path (str): Path to save the generated GIF.
        raw_band (str): Raw VV band name.
        processed_band (str): Corrected/smoothed VV band name.
        likelihood_band (str): Likelihood score band name.
        csv_filename (str): Output CSV filename.
        png_filename (str): Output plot image filename.
    """
    import os

    os.makedirs(output_dir, exist_ok=True)
    aoi_ee = ensure_ee_geometry(aoi)  # where aoi_geojson is your raw geojson dict

    # 1. Compute lake metrics and time series
    print("📊 Computing lake metrics...")
    metrics_fc = extract_lake_area_timeseries(
        lake_likelihood_collection,
        aoi
    )

    # # 2. Bring to Python
    # lake_metrics_list = metrics_fc.getInfo()['features']
    # lake_metrics_dicts = [f['properties'] for f in lake_metrics_list]
    
    # 3. Save as CSV and plot
    save_lake_metrics_plot_and_csv(metrics_fc, output_csv='outputs/lake_metrics.csv', output_png='outputs/lake_plot.png')
    print("CSV and Plot saved")

    # 4. Generate animated GIF
    # create_lake_monitoring_gif(
    #     image_collection=lake_likelihood_collection,
    #     aoi=aoi,
    #     gif_path=gif_output_path
    # )
    build_lake_monitoring_gif('outputs', metrics_fc['date'].tolist(), metrics_fc['mean_area_km2'].tolist())

import geojson
import shapely

def ensure_ee_geometry(geom):
    """
    Accepts a Shapely geometry, EE Geometry, or GeoJSON dict,
    and returns a valid ee.Geometry object.
    """
    # Case 1: Already an ee.Geometry
    if isinstance(geom, ee.Geometry):
        return geom

    # Case 2: Shapely geometry
    elif isinstance(geom, shapely.geometry.base.BaseGeometry):
        geojson_dict = shapely.geometry.mapping(geom)  # Convert to GeoJSON-like dict
        return ee.Geometry(geojson_dict)

    # Case 3: GeoJSON-like dict (ensure 'type' and 'coordinates' keys exist)
    elif isinstance(geom, dict) and 'type' in geom and 'coordinates' in geom:
        return ee.Geometry(geom)

    # Case 4: geojson.Geometry object (from geojson package)
    elif isinstance(geom, geojson.Geometry):
        return ee.Geometry(geom)

    else:
        raise TypeError("Unsupported geometry format. Provide an ee.Geometry, shapely geometry, or GeoJSON dict.")


def extract_lake_area_timeseries(collection, aoi, out_dir="lake_outputs", thresholds=(0.1, 0.9)):
    """
    Process a Sentinel-1 likelihood image collection to extract lake area metrics and export images.
    
    Parameters:
        collection (ee.ImageCollection): ImageCollection with 'water_likelihood' band.
        aoi (shapely geometry): Area of interest.
        out_dir (str): Directory to save exported images.
        thresholds (tuple): Lower and upper likelihood bounds (default: (0.1, 0.9)).
    
    Returns:
        pd.DataFrame: Table with lake area time series.
    """
    pixel_area_m2 = 10 * 10
    lower_thresh, upper_thresh = thresholds

    # Convert AOI to ee.Geometry if needed
    aoi_ee = ensure_ee_geometry(aoi)

    processed = collection.sort('system:time_start')
    count = processed.size().getInfo()
    if count == 0:
        raise ValueError("No valid Sentinel-1 images found.")

    dates = []
    areas_mean = []
    areas_lower = []
    areas_upper = []

    for i in range(count):
        img = ee.Image(processed.toList(count).get(i))
        date_str = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd').getInfo()
        dates.append(date_str)

        id_str = f's1_{date_str}'
        likelihood = img.select('lake_likelihood')

        try:
            # Mean likelihood-weighted area
            likelihood_sum = likelihood.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=aoi_ee,
                scale=10,
                maxPixels=1e9
            ).get('lake_likelihood').getInfo() or 0
            mean_area_km2 = likelihood_sum * pixel_area_m2 / 1e6

            # Lower bound (confident water)
            lower_mask = likelihood.gt(lower_thresh)
            lower_sum = lower_mask.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=aoi_ee,
                scale=10,
                maxPixels=1e9
            ).get('lake_likelihood').getInfo() or 0
            lower_area_km2 = lower_sum * pixel_area_m2 / 1e6

            # Upper bound (possible water)
            upper_mask = likelihood.gt(upper_thresh)
            upper_sum = upper_mask.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=aoi_ee,
                scale=10,
                maxPixels=1e9
            ).get('lake_likelihood').getInfo() or 0
            upper_area_km2 = upper_sum * pixel_area_m2 / 1e6

        except Exception as e:
            print(f"Failed on image {id_str}: {e}")
            mean_area_km2 = lower_area_km2 = upper_area_km2 = 0

        # Store results
        areas_mean.append(mean_area_km2)
        areas_lower.append(lower_area_km2)
        areas_upper.append(upper_area_km2)

    df = pd.DataFrame({
        "date": dates,
        "mean_area_km2": areas_mean,
        "lower_area_km2": areas_lower,
        "upper_area_km2": areas_upper
    })
    return df


def export_images_to_drive(s1_collection, aoi_ee, bands_to_export=None, prefix="tracking", scale=10):
    """Triggers GEE Drive exports for each image/band. Returns task list."""
    if bands_to_export is None:
        bands_to_export = ['VV_raw', 'VV_corrected', 'lake_likelihood']

    count = s1_collection.size().getInfo()
    s1_list = s1_collection.toList(count)
    tasks = []

    for i in range(count):
        img = ee.Image(s1_list.get(i))
        date_str = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd').getInfo()
        
        for band in bands_to_export:
            file_prefix = f"{prefix}_{band}_{date_str}"
            task = ee.batch.Export.image.toDrive(
                image=img.select(band).clip(aoi_ee),
                description=file_prefix,
                folder="GEE_Exports",
                fileNamePrefix=file_prefix,
                region=aoi_ee.bounds(),
                scale=scale,
                maxPixels=1e12
            )
            task.start()
            tasks.append({'name': f"{date_str}_{band}", 'prefix': file_prefix, 'task': task})
    return tasks
                
import os
import glob
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from PIL import Image, ImageDraw, ImageFont, ImageOps

def build_lake_monitoring_gif(out_dir, dates, areas_km2, 
                              bands=['VV_raw', 'VV_corrected', 'lake_likelihood'],
                              target_size=(1200, 600),
                              gif_filename='outputs/lake_monitoring.gif',
                              duration=600,
                              font_path=None):
    """
    Build a GIF from TIFF files representing raw VV, corrected VV, and lake likelihood.

    Args:
        out_dir (str): Directory where TIFF files are stored.
        dates (list[str]): List of date strings corresponding to images.
        areas_km2 (list[float]): List of lake areas for each date.
        bands (list[str]): List of bands to load from TIFFs. Expected names in files.
            Default: ['VV_raw', 'VV_corrected', 'likelihood']
        target_size (tuple): (width, height) of output GIF frames.
        gif_filename (str): Output GIF filename (full path).
        duration (int): Duration per frame in milliseconds.
        font_path (str or None): Path to a .ttf font file for overlay text. If None, uses default.

    Returns:
        None. Saves GIF to gif_filename.
    """
    # Collect files for each band and date
    band_files = {band: sorted(glob.glob(os.path.join(out_dir, f"*_{band}_*.tif"))) for band in bands}

    # Basic sanity check
    count = len(dates)
    if any(len(files) < count for files in band_files.values()):
        print("⚠️ Warning: Not enough files found for some bands compared to dates.")

    # Load font
    try:
        font = ImageFont.truetype(font_path or "arial.ttf", 24)
    except Exception:
        font = ImageFont.load_default()

    frames = []

    for i in range(count):
        # Check all needed files exist
        if any(i >= len(band_files[band]) for band in bands):
            print(f"⚠️ Missing files for date {dates[i]}, skipping frame.")
            continue

        # Load and normalize raw VV: scale [-20,0] to [0,1], grayscale
        with rasterio.open(band_files['VV_raw'][i]) as src:
            vv_raw = src.read(1)
        vv_raw_norm = np.clip((vv_raw + 20) / 20, 0, 1)
        vv_raw_rgb = (plt.cm.gray(vv_raw_norm)[:, :, :3] * 255).astype(np.uint8)
        im_vv_raw = Image.fromarray(vv_raw_rgb)

        # Load and normalize corrected VV: scale [-20,0] to [0,1], grayscale
        with rasterio.open(band_files['VV_corrected'][i]) as src:
            vv_corr = src.read(1)
        vv_corr_norm = np.clip((vv_corr + 20) / 20, 0, 1)
        vv_corr_rgb = (plt.cm.gray(vv_corr_norm)[:, :, :3] * 255).astype(np.uint8)
        im_vv_corr = Image.fromarray(vv_corr_rgb)

        # Load and normalize likelihood: scale [0,1], viridis colormap
        with rasterio.open(band_files['lake_likelihood'][i]) as src:
            likelihood = src.read(1)
        likelihood_norm = np.clip(likelihood, 0, 1)
        likelihood_rgb = (plt.cm.viridis(likelihood_norm)[:, :, :3] * 255).astype(np.uint8)
        im_likelihood = Image.fromarray(likelihood_rgb)

        # Combine all three side-by-side
        total_width = im_vv_raw.width + im_vv_corr.width + im_likelihood.width
        max_height = max(im_vv_raw.height, im_vv_corr.height, im_likelihood.height)
        combined = Image.new("RGB", (total_width, max_height))
        combined.paste(im_vv_raw, (0, 0))
        combined.paste(im_vv_corr, (im_vv_raw.width, 0))
        combined.paste(im_likelihood, (im_vv_raw.width + im_vv_corr.width, 0))

        # Resize combined frame
        combined = ImageOps.contain(combined, target_size)

        # Draw date and lake area overlay
        draw = ImageDraw.Draw(combined)
        date_text = dates[i]
        area_text = f"Lake area: {areas_km2[i]:.2f} km²"

        # Draw date top-left
        draw.text((10, 10), date_text, font=font, fill='white', stroke_width=2, stroke_fill='black')

        # Draw lake area bottom-left
        bbox = draw.textbbox((0,0), area_text, font=font)
        x = 10
        y = combined.height - (bbox[3] - bbox[1]) - 10
        draw.text((x, y), area_text, font=font, fill='white', stroke_width=2, stroke_fill='black')

        frames.append(combined)

    # Save as animated GIF
    if frames:
        frames[0].save(
            gif_filename,
            save_all=True,
            append_images=frames[1:],
            duration=duration,
            loop=0,
            optimize=True
        )
        print(f"✅ GIF saved to: {gif_filename}")
    else:
        print("⚠️ No frames generated. Check file inputs and dates.")