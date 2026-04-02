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
    df.to_csv(output_csv, index=False)

    date_objs = [datetime.strptime(d, '%Y-%m-%d') for d in df['date']]
    plt.figure(figsize=(10, 6))
    plt.fill_between(date_objs, df['upper_area_km2'], df['lower_area_km2'], color='lightblue', alpha=0.5, label='Uncertainty')
    plt.plot(date_objs, df['mean_area_km2'], label='Likelihood > 0.5', color='blue')
    plt.xlabel('Date')
    plt.ylabel('Water Area (km²)')
    plt.title('Lake Area over Time (Estimated from Likelihood)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_png)

    print(f"Saved plot to {output_png} and data to {output_csv}")

def make_gif(image_paths, gif_path, fps=2):
    frames = [Image.open(img_path) for img_path in image_paths]
    duration = int(1000 / fps)
    frames[0].save(
        gif_path,
        save_all=True,
        append_images=frames[1:],
        duration=duration,
        loop=0
    )
    
import os
import shutil
from PIL import Image, ImageDraw, ImageFont
import ee


def generate_lake_metrics_report(
    lake_likelihood_collection,
    aoi,
    output_dir="outputs",                          # FIX: used consistently below
    gif_output_path=None,                          # FIX: derived from output_dir if None
    raw_band="VV_raw",
    processed_band="VV_smoothed",
    likelihood_band="lake_likelihood",
    csv_filename=None,                             # FIX: derived from output_dir if None
    png_filename=None,                             # FIX: derived from output_dir if None
):
    """
    Generates lake metrics from likelihood image collection and saves CSV, plot, and a GIF visualization.

    Args:
        lake_likelihood_collection (ee.ImageCollection): Collection with raw, processed, and lake likelihood bands.
        aoi (ee.Geometry): Area of interest.
        output_dir (str): Directory to save all outputs. All other path args default to subdirs here.
        gif_output_path (str): Path to save the generated GIF. Defaults to output_dir/lake_monitoring.gif.
        raw_band (str): Raw VV band name.
        processed_band (str): Corrected/smoothed VV band name.
        likelihood_band (str): Likelihood score band name.
        csv_filename (str): Output CSV filename. Defaults to output_dir/lake_metrics.csv.
        png_filename (str): Output plot image filename. Defaults to output_dir/lake_metrics_plot.png.
    """
    # FIX: resolve all output paths relative to output_dir so headless runs
    # don't write into whatever the cwd happens to be
    os.makedirs(output_dir, exist_ok=True)
    if csv_filename is None:
        csv_filename = os.path.join(output_dir, "lake_metrics.csv")
    if png_filename is None:
        png_filename = os.path.join(output_dir, "lake_metrics_plot.png")
    if gif_output_path is None:
        gif_output_path = os.path.join(output_dir, "lake_monitoring.gif")

    aoi_ee = ensure_ee_geometry(aoi)

    # 1. Compute lake metrics and time series
    print("Computing lake metrics...", flush=True)
    metrics_fc = extract_lake_area_timeseries(
        lake_likelihood_collection,
        aoi
    )

    # 2. Save as CSV and plot
    save_lake_metrics_plot_and_csv(
        metrics_fc,
        output_csv=csv_filename,    # FIX: was hardcoded 'outputs/lake_metrics.csv'
        output_png=png_filename,    # FIX: was hardcoded 'outputs\lake_plot.png'
    )
    print("CSV and Plot saved", flush=True)

    # 3. Generate animated GIF
    build_lake_monitoring_gif(
        out_dir=output_dir,
        dates=metrics_fc['date'].tolist(),
        areas_km2=metrics_fc['mean_area_km2'].tolist(),
        gif_filename=gif_output_path,   # FIX: was hardcoded 'outputs\lake_monitoring.gif'
    )

import geojson
import shapely

def ensure_ee_geometry(geom):
    """
    Accepts a Shapely geometry, EE Geometry, or GeoJSON dict,
    and returns a valid ee.Geometry object.
    """
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


def extract_lake_area_timeseries(collection, aoi, out_dir="lake_outputs", thresholds=(0.1, 0.9)):
    """
    Process a Sentinel-1 likelihood image collection to extract lake area metrics.
    
    Parameters:
        collection (ee.ImageCollection): ImageCollection with 'lake_likelihood' band.
        aoi (shapely geometry or ee.Geometry): Area of interest.
        out_dir (str): Directory to save exported images.
        thresholds (tuple): Lower and upper likelihood bounds (default: (0.1, 0.9)).
    
    Returns:
        pd.DataFrame: Table with lake area time series.
    """
    pixel_area_m2 = 10 * 10
    lower_thresh, upper_thresh = thresholds

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

        likelihood = img.select('lake_likelihood')

        try:
            likelihood_sum = likelihood.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=aoi_ee,
                scale=10,
                maxPixels=1e9
            ).get('lake_likelihood').getInfo() or 0
            mean_area_km2 = likelihood_sum * pixel_area_m2 / 1e6

            lower_mask = likelihood.gt(lower_thresh)
            lower_sum = lower_mask.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=aoi_ee,
                scale=10,
                maxPixels=1e9
            ).get('lake_likelihood').getInfo() or 0
            lower_area_km2 = lower_sum * pixel_area_m2 / 1e6

            upper_mask = likelihood.gt(upper_thresh)
            upper_sum = upper_mask.reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=aoi_ee,
                scale=10,
                maxPixels=1e9
            ).get('lake_likelihood').getInfo() or 0
            upper_area_km2 = upper_sum * pixel_area_m2 / 1e6

        except Exception as e:
            print(f"Failed on image {i}: {e}", flush=True)
            mean_area_km2 = lower_area_km2 = upper_area_km2 = 0

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


def _build_user_drive_service(token_path):
    """
    Build a Drive client authenticated as the GEE user via saved OAuth token.
    The token is written once by the Dashboard login flow and auto-refreshed on
    every subsequent call.
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    creds = Credentials.from_authorized_user_file(
        token_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    return build("drive", "v3", credentials=creds, cache_discovery=False, static_discovery=False)

def _delete_drive_files(token_path, file_ids):
    """
    Moves a list of Google Drive files to trash using the user's OAuth credentials.
    GEE exports are owned by the authenticated user; only the owner can trash them.
    Errors are logged but do not raise so a cleanup failure never aborts the pipeline.
    """
    try:
        drive = _build_user_drive_service(token_path)
    except Exception as e:
        print(f"Warning: could not build Drive service for cleanup: {e}", flush=True)
        return
    for fid in file_ids:
        try:
            drive.files().update(fileId=fid, body={"trashed": True}).execute()
            print(f"Trashed Drive file: {fid}", flush=True)
        except Exception as e:
            print(f"Warning: could not trash Drive file {fid}: {e}", flush=True)


def export_images_via_drive(s1_collection, aoi_ee, token_path,
                            bands_to_export=None, output_dir="outputs",
                            prefix="S1", scale=10, drive_folder="GEE_Exports"):
    """
    Exports each image/band combination to Google Drive via GEE batch tasks,
    downloads completed files to output_dir, then deletes them from Drive.

    Filenames follow the pattern: {prefix}_{band}_{img_id}.tif
    This matches the original main.py pipeline naming convention.

    Args:
        s1_collection (ee.ImageCollection): Scored Sentinel-1 collection.
        aoi_ee: Area of interest (ee.Geometry or shapely geometry).
        token_path (str): Path to the saved OAuth Drive token JSON.
        bands_to_export (list): Band names to export. Defaults to VV_raw, VV_corrected, VV_smoothed.
        output_dir (str): Local directory to download files into.
        prefix (str): Filename prefix, e.g. 'tracking_s1'.
        scale (int): Export resolution in metres.
        drive_folder (str): Google Drive folder name for GEE exports.
    """
    import os
    import io
    import time
    import ee
    from googleapiclient.http import MediaIoBaseDownload

    if bands_to_export is None:
        bands_to_export = ['VV_raw', 'VV_corrected', 'VV_smoothed']

    os.makedirs(output_dir, exist_ok=True)
    drive_service = _build_user_drive_service(token_path)

    count = s1_collection.size().getInfo()
    s1_list = s1_collection.toList(count)

    # --- 1. Launch one GEE task per image per band ---
    task_list = []
    for i in range(count):
        img = ee.Image(s1_list.get(i))
        # Use the GEE asset ID as part of filename to match main.py convention.
        # Fall back to index if ID unavailable.
        img_id = (img.id().getInfo() or f"img{i:03d}").replace("/", "_")

        for band in bands_to_export:
            local_filename = f"{prefix}_{band}_{img_id}.tif"
            local_path = os.path.join(output_dir, local_filename)

            if os.path.exists(local_path):
                print(f"File exists, skipping: {local_filename}", flush=True)
                continue

            # Strip the .tif — GEE appends it automatically
            file_prefix = local_filename[:-4]

            try:
                band_image = img.select(band).clip(aoi_ee)
                task = ee.batch.Export.image.toDrive(
                    image=band_image,
                    description=file_prefix[:100],   # GEE description limit
                    folder=drive_folder,
                    fileNamePrefix=file_prefix,
                    region=aoi_ee.bounds(),
                    scale=scale,
                    maxPixels=1e12,
                )
                task.start()
                task_list.append({
                    'task': task,
                    'file_prefix': file_prefix,
                    'local_path': local_path,
                    'drive_file_id': None,
                    'done': False,
                })
                print(f"Task started: {local_filename}", flush=True)
            except Exception as e:
                print(f"Failed to start task for {local_filename}: {e}", flush=True)

    if not task_list:
        print("No tasks to run (all files already exist or none launched).", flush=True)
        return

    # --- 2. Poll tasks and download as each completes ---
    print(f"Waiting for {len(task_list)} GEE task(s)...", flush=True)
    completed = 0
    while completed < len(task_list):
        for item in task_list:
            if item['done']:
                continue

            status = item['task'].status()

            if status['state'] == 'COMPLETED':
                fname = f"{item['file_prefix']}.tif"
                res = drive_service.files().list(
                    q=f"name='{fname}' and trashed=false", fields="files(id)"
                ).execute()
                files = res.get('files', [])
                if files:
                    file_id = files[0]['id']
                    request = drive_service.files().get_media(fileId=file_id)
                    with io.FileIO(item['local_path'], 'wb') as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                    print(f"Downloaded: {fname}", flush=True)
                    item['drive_file_id'] = file_id  # store for cleanup
                    item['done'] = True
                    completed += 1
                # If file not yet visible in Drive, loop will retry on next pass

            elif status['state'] in ['FAILED', 'CANCELLED']:
                print(f"Task failed: {item['file_prefix']} — {status.get('error_message', '')}", flush=True)
                item['done'] = True
                completed += 1

        if completed < len(task_list):
            time.sleep(30)

    # --- 3. Delete all successfully downloaded files from Drive ---
    drive_ids_to_delete = [item['drive_file_id'] for item in task_list if item.get('drive_file_id')]
    if drive_ids_to_delete:
        print(f"Cleaning up {len(drive_ids_to_delete)} file(s) from Google Drive...", flush=True)
        _delete_drive_files(token_path, drive_ids_to_delete)
                
import os
import glob
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from PIL import Image, ImageDraw, ImageFont, ImageOps

PANEL_LABELS = {
    'VV_raw':          'Raw VV (dB)',
    'VV_corrected':    'Corrected VV (dB)',
    'lake_likelihood': 'Lake Likelihood',
}

def _read_masked(path):
    """Read a single-band raster, replacing nodata and -9999 fill with nan."""
    with rasterio.open(path) as src:
        data = src.read(1).astype(float)
        nodata = src.nodata
    if nodata is not None:
        data[data == nodata] = np.nan
    data[data <= -9999] = np.nan
    return data

def _render_band(data, cmap, vmin, vmax, nan_fill):
    """Normalise and colourise a band array into a PIL Image."""
    norm = np.clip((data - vmin) / (vmax - vmin), 0, 1)
    norm = np.where(np.isnan(norm), nan_fill, norm)
    rgb  = (cmap(norm)[:, :, :3] * 255).astype(np.uint8)
    return Image.fromarray(rgb)

def _fit_height(im, target_h):
    """Resize image to target_h, preserving aspect ratio."""
    ratio = target_h / im.height
    return im.resize((int(im.width * ratio), target_h), Image.LANCZOS)

def build_lake_monitoring_gif(out_dir, dates, areas_km2,
                              bands=['VV_raw', 'VV_corrected', 'lake_likelihood'],
                              target_size=(1800, 600),
                              gif_filename=None,
                              duration=600,
                              font_path=None):
    """
    Build a lake monitoring GIF from exported TIF files.
    Panels: Raw VV | Corrected VV | Lake Likelihood, with date and area overlay.
    """
    if gif_filename is None:
        gif_filename = os.path.join(out_dir, "lake_monitoring.gif")

    band_files = {
        band: sorted(glob.glob(os.path.join(out_dir, f"*{band}*.tif")))
        for band in bands
    }

    count = len(dates)
    for band in bands:
        n = len(band_files[band])
        if n < count:
            print(f"Warning: found {n} files for band '{band}', expected {count}.", flush=True)

    try:
        font       = ImageFont.truetype(font_path or "arial.ttf", 22)
        font_small = ImageFont.truetype(font_path or "arial.ttf", 16)
    except Exception:
        font       = ImageFont.load_default()
        font_small = font

    frames = []

    for i in range(count):
        # Skip frame if any band file is missing
        if any(i >= len(band_files[band]) for band in bands):
            print(f"Missing files for frame {i} ({dates[i]}), skipping.", flush=True)
            continue

        # --- Render each panel ---
        vv_raw  = _read_masked(band_files['VV_raw'][i])
        im_raw  = _render_band(vv_raw,  plt.cm.gray,   vmin=-25, vmax=0, nan_fill=0.5)

        vv_corr = _read_masked(band_files['VV_corrected'][i])
        im_corr = _render_band(vv_corr, plt.cm.gray,   vmin=-25, vmax=0, nan_fill=0.5)

        lkl     = _read_masked(band_files['lake_likelihood'][i])
        im_lkl  = _render_band(lkl,    plt.cm.viridis, vmin=0,   vmax=1, nan_fill=0.0)

        # --- Equalise panel heights before combining ---
        target_h = max(im_raw.height, im_corr.height, im_lkl.height)
        im_raw   = _fit_height(im_raw,  target_h)
        im_corr  = _fit_height(im_corr, target_h)
        im_lkl   = _fit_height(im_lkl,  target_h)

        # --- Combine side-by-side ---
        total_w  = im_raw.width + im_corr.width + im_lkl.width
        combined = Image.new("RGB", (total_w, target_h))
        combined.paste(im_raw,  (0, 0))
        combined.paste(im_corr, (im_raw.width, 0))
        combined.paste(im_lkl,  (im_raw.width + im_corr.width, 0))

        combined = ImageOps.contain(combined, target_size)

        # --- Overlays ---
        draw    = ImageDraw.Draw(combined)
        panel_w = combined.width // 3

        # Panel labels centred at top of each column
        for j, band in enumerate(bands):
            label = PANEL_LABELS.get(band, band)
            bbox  = draw.textbbox((0, 0), label, font=font_small)
            lw    = bbox[2] - bbox[0]
            x     = j * panel_w + (panel_w - lw) // 2
            draw.text((x, 6), label, font=font_small, fill='white',
                      stroke_width=1, stroke_fill='black')

        # Date — top left
        draw.text((10, 28), dates[i], font=font, fill='white',
                  stroke_width=2, stroke_fill='black')

        # Lake area — bottom left
        area_text = f"Lake area: {areas_km2[i]:.2f} km²"
        bbox = draw.textbbox((0, 0), area_text, font=font)
        y    = combined.height - (bbox[3] - bbox[1]) - 10
        draw.text((10, y), area_text, font=font, fill='white',
                  stroke_width=2, stroke_fill='black')

        frames.append(combined)
        print(f"  Frame {i+1}/{count}: {dates[i]}", flush=True)

    if frames:
        frames[0].save(
            gif_filename,
            save_all=True,
            append_images=frames[1:],
            duration=duration,
            loop=0,
            optimize=True,
        )
        print(f"GIF saved to: {gif_filename}", flush=True)
    else:
        print("No frames generated. Check TIF files and dates.", flush=True)
