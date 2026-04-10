# -*- coding: utf-8 -*-
"""
THAW - Reporting module

Post-processing and output generation for both pipelines:

  Lakedetection: cluster_processing — DBSCAN on z_score raster, saves GeoJSON + CSV
  Tracking:      extract_cluster_area_timeseries — DBSCAN on likelihood TIFs per
                 frame, computes area at three likelihood levels
                 generate_lake_metrics_report — orchestrates metrics, plot, GIF

Replaces water_detection.py (reporting portions) and analysis.py entirely.
"""

import os
import re
import glob
import json
import math

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import rasterio
from rasterio.features import shapes
from rasterio.warp import transform_geom
from sklearn.cluster import DBSCAN
from PIL import Image, ImageDraw, ImageFont, ImageOps
from datetime import datetime


# ============================================================
# LAKEDETECTION — CLUSTER PROCESSING
# ============================================================

def cluster_processing(tif_path, timestamp, z_thres=-2, min_size_cluster=20, pix=6):
    """
    DBSCAN clustering on a z_score raster. Candidate pixels are those <= z_thres
    (anomalously low backscatter). Saves a GeoJSON polygon file and a CSV summary.

    Parameters
    ----------
    tif_path         : str   — path to the z_score GeoTIFF
    timestamp        : str   — run timestamp used in output filenames
    z_thres          : float — z_score threshold; pixels <= this are candidates
    min_size_cluster : int   — DBSCAN min_samples
    pix              : int   — DBSCAN eps (pixels)

    Returns
    -------
    (poly_path, summary_path) or (None, None) if no clusters found
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
        print("No suspicious patterns found.", flush=True)
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

    with open(poly_path, 'w') as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)

    with open(summary_path, 'w') as f:
        f.write(summary_data)

    print("Clustering complete.", flush=True)
    return poly_path, summary_path


# ============================================================
# TRACKING — CLUSTER-BASED AREA TIME SERIES
# ============================================================

def extract_cluster_area_timeseries(out_dir, thresholds=(0.1, 0.5, 0.9),
                                     min_size_cluster=20, pix=6):
    """
    Compute lake area time series from locally downloaded lake_likelihood TIFs.

    For each frame, DBSCAN is run at the mid threshold to identify lake clusters.
    Area is then computed at three likelihood levels:
      lower_area_km2 — pixels within clusters where likelihood >= lower threshold
      mean_area_km2  — likelihood-weighted area within clusters (sum × pixel area)
      upper_area_km2 — pixels within clusters where likelihood >= upper threshold

    Parameters
    ----------
    out_dir          : str   — directory containing *lake_likelihood*.tif files
    thresholds       : tuple — (lower, mid, upper) likelihood thresholds
    min_size_cluster : int   — DBSCAN min_samples
    pix              : int   — DBSCAN eps (pixels)

    Returns
    -------
    pd.DataFrame with columns: date, mean_area_km2, lower_area_km2, upper_area_km2
    """
    lower_thresh, mid_thresh, upper_thresh = thresholds

    tif_files = sorted(glob.glob(os.path.join(out_dir, "*lake_likelihood*.tif")))
    if not tif_files:
        raise ValueError(f"No lake_likelihood TIF files found in {out_dir}")

    dates = []
    areas_mean = []
    areas_lower = []
    areas_upper = []

    for tif_path in tif_files:
        # Extract date from filename — GEE asset IDs contain YYYYMMDDTHHMMSS
        match = re.search(r'(\d{4})(\d{2})(\d{2})T\d{6}', os.path.basename(tif_path))
        date_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}" if match else os.path.basename(tif_path)

        print(f"Processing frame: {date_str}", flush=True)

        with rasterio.open(tif_path) as src:
            data = src.read(1).astype(float)
            transform = src.transform
            res_x, res_y = src.res
            src_crs = src.crs
            nodata = src.nodata

        # Mask nodata
        if nodata is not None:
            data[data == nodata] = np.nan
        data[data <= -9999] = np.nan

        # Compute pixel area in km2
        if src_crs.is_projected:
            pix_area_km2 = abs(res_x) * abs(res_y) / 1e6
        else:
            # Approximate using image centre latitude
            center_row, center_col = data.shape[0] / 2, data.shape[1] / 2
            center_lon, center_lat = transform * (center_col, center_row)
            m_per_deg_lat = 111320
            m_per_deg_lon = 111320 * math.cos(math.radians(center_lat))
            pix_area_km2 = (abs(res_x) * m_per_deg_lon) * (abs(res_y) * m_per_deg_lat) / 1e6

        # Find candidate pixels at mid threshold for DBSCAN
        valid = np.isfinite(data)
        candidate = valid & (data >= mid_thresh)
        ys, xs = np.nonzero(candidate)

        if len(ys) == 0:
            print(f"  No clusters found above {mid_thresh}.", flush=True)
            dates.append(date_str)
            areas_mean.append(0.0)
            areas_lower.append(0.0)
            areas_upper.append(0.0)
            continue

        # DBSCAN clustering
        coords = np.column_stack([ys, xs])
        db = DBSCAN(eps=pix, min_samples=min_size_cluster).fit(coords)
        labels = db.labels_

        # Keep only valid clusters (noise label == -1 is excluded)
        cluster_mask = labels >= 0
        if not cluster_mask.any():
            print(f"  No valid clusters after DBSCAN.", flush=True)
            dates.append(date_str)
            areas_mean.append(0.0)
            areas_lower.append(0.0)
            areas_upper.append(0.0)
            continue

        cluster_ys = ys[cluster_mask]
        cluster_xs = xs[cluster_mask]
        cluster_vals = data[cluster_ys, cluster_xs]

        # Area at three levels, summed across all clusters
        lower_area = float((cluster_vals >= lower_thresh).sum()) * pix_area_km2
        mean_area  = float(cluster_vals.sum()) * pix_area_km2  # likelihood-weighted
        upper_area = float((cluster_vals >= upper_thresh).sum()) * pix_area_km2

        n_clusters = len(set(labels[cluster_mask]))
        print(f"  {n_clusters} cluster(s) - lower: {lower_area:.4f} km2, "
              f"mid: {mean_area:.4f} km2, upper: {upper_area:.4f} km2", flush=True)

        dates.append(date_str)
        areas_mean.append(mean_area)
        areas_lower.append(lower_area)
        areas_upper.append(upper_area)

    return pd.DataFrame({
        "date": dates,
        "mean_area_km2": areas_mean,
        "lower_area_km2": areas_lower,
        "upper_area_km2": areas_upper,
    })


# ============================================================
# GROWTH TREND FLAGGING
# ============================================================

def flag_growth_trend(areas, dates, threshold_km2=0.05):
    """
    Flag whether the most recent frame shows lake growth above a threshold.

    Parameters
    ----------
    areas        : list[float] — lake area values in km2, ordered by date
    dates        : list[str]   — corresponding date strings
    threshold_km2: float       — minimum area increase to flag as growth

    Returns
    -------
    bool: True if growth exceeds threshold, False otherwise.
    """
    if len(areas) < 2:
        return False
    growth = areas[-1] - areas[-2]
    return growth > threshold_km2


# ============================================================
# CSV AND PLOT
# ============================================================

def save_lake_metrics_plot_and_csv(df, output_csv='lake_metrics.csv', output_png='lake_plot.png'):
    """
    Save a lake area time series DataFrame to CSV and a PNG plot.
    """
    df.to_csv(output_csv, index=False)

    date_objs = [datetime.strptime(d, '%Y-%m-%d') for d in df['date']]
    plt.figure(figsize=(10, 6))
    plt.fill_between(date_objs, df['upper_area_km2'], df['lower_area_km2'],
                     color='lightblue', alpha=0.5, label='Uncertainty')
    plt.plot(date_objs, df['mean_area_km2'], label='Likelihood-weighted area', color='blue')
    plt.xlabel('Date')
    plt.ylabel('Water Area (km2)')
    plt.title('Lake Area over Time (Cluster-based Likelihood)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_png)
    plt.close()

    print(f"Saved plot to {output_png} and data to {output_csv}", flush=True)


# ============================================================
# REPORT ORCHESTRATOR (TRACKING PIPELINE)
# ============================================================

def generate_lake_metrics_report(
    output_dir,
    gif_output_path=None,
    csv_filename=None,
    png_filename=None,
    thresholds=(0.1, 0.5, 0.9),
):
    """
    Compute cluster-based lake area metrics from downloaded likelihood TIFs,
    save CSV + plot, flag growth trend, and build animated GIF.

    Parameters
    ----------
    output_dir      : str   — directory containing downloaded *lake_likelihood*.tif files
    gif_output_path : str   — path for the GIF (default: output_dir/lake_monitoring.gif)
    csv_filename    : str   — output CSV path (default: output_dir/lake_metrics.csv)
    png_filename    : str   — output plot path (default: output_dir/lake_metrics_plot.png)
    thresholds      : tuple — (lower, mid, upper) likelihood thresholds
    """
    os.makedirs(output_dir, exist_ok=True)
    if csv_filename is None:
        csv_filename = os.path.join(output_dir, "lake_metrics.csv")
    if png_filename is None:
        png_filename = os.path.join(output_dir, "lake_metrics_plot.png")
    if gif_output_path is None:
        gif_output_path = os.path.join(output_dir, "lake_monitoring.gif")

    # 1. Compute cluster-based area time series from local TIFs
    print("Computing cluster-based lake metrics...", flush=True)
    metrics_df = extract_cluster_area_timeseries(output_dir, thresholds=thresholds)

    # 2. Flag growth trend
    growing = flag_growth_trend(
        metrics_df['mean_area_km2'].tolist(),
        metrics_df['date'].tolist(),
    )
    if growing:
        print("Growth trend detected: lake area increased beyond threshold.", flush=True)
    else:
        print("No significant growth trend detected.", flush=True)

    # 3. Save CSV and plot
    save_lake_metrics_plot_and_csv(
        metrics_df,
        output_csv=csv_filename,
        output_png=png_filename,
    )

    # 4. Build animated GIF
    build_lake_monitoring_gif(
        out_dir=output_dir,
        dates=metrics_df['date'].tolist(),
        areas_km2=metrics_df['mean_area_km2'].tolist(),
        gif_filename=gif_output_path,
    )


# ============================================================
# GIF BUILDER
# ============================================================

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
    rgb = (cmap(norm)[:, :, :3] * 255).astype(np.uint8)
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
        if any(i >= len(band_files[band]) for band in bands):
            print(f"Missing files for frame {i} ({dates[i]}), skipping.", flush=True)
            continue

        vv_raw  = _read_masked(band_files['VV_raw'][i])
        im_raw  = _render_band(vv_raw,  plt.cm.gray,   vmin=-25, vmax=0, nan_fill=0.5)

        vv_corr = _read_masked(band_files['VV_corrected'][i])
        im_corr = _render_band(vv_corr, plt.cm.gray,   vmin=-25, vmax=0, nan_fill=0.5)

        lkl     = _read_masked(band_files['lake_likelihood'][i])
        im_lkl  = _render_band(lkl,    plt.cm.viridis, vmin=0,   vmax=1, nan_fill=0.0)

        target_h = max(im_raw.height, im_corr.height, im_lkl.height)
        im_raw   = _fit_height(im_raw,  target_h)
        im_corr  = _fit_height(im_corr, target_h)
        im_lkl   = _fit_height(im_lkl,  target_h)

        total_w  = im_raw.width + im_corr.width + im_lkl.width
        combined = Image.new("RGB", (total_w, target_h))
        combined.paste(im_raw,  (0, 0))
        combined.paste(im_corr, (im_raw.width, 0))
        combined.paste(im_lkl,  (im_raw.width + im_corr.width, 0))

        combined = ImageOps.contain(combined, target_size)

        draw    = ImageDraw.Draw(combined)
        panel_w = combined.width // 3

        for j, band in enumerate(bands):
            label = PANEL_LABELS.get(band, band)
            bbox  = draw.textbbox((0, 0), label, font=font_small)
            lw    = bbox[2] - bbox[0]
            x     = j * panel_w + (panel_w - lw) // 2
            draw.text((x, 6), label, font=font_small, fill='white',
                      stroke_width=1, stroke_fill='black')

        draw.text((10, 28), dates[i], font=font, fill='white',
                  stroke_width=2, stroke_fill='black')

        area_text = f"Lake area: {areas_km2[i]:.4f} km2"
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
