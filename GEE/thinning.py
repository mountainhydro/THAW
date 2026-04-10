# -*- coding: utf-8 -*-

"""
THAW - Terrain and glacier input utilities

Handles AOI loading, DEM loading, glacier mask extraction,
and glacier thinning correction from the Hugonnet (2021) dataset.

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger

Created on Feb 2 2026
"""

import ee
import json
import os
import math

import numpy as np
import requests
import rasterio
import rasterio.features
import rasterio.mask
import geopandas as gpd
import shapely.geometry
from shapely.geometry import mapping
from shapely.ops import transform


# ============================================================
# AOI AND TERRAIN INPUTS
# ============================================================

def load_aoi(aoi_input):
    """
    Load an AOI from a GeoJSON string, file, shapefile, or bounding box list.

    Parameters
    ----------
    aoi_input : str or list
        GeoJSON string, path to .geojson/.json/.shp, or [xmin, ymin, xmax, ymax].

    Returns
    -------
    ee.Geometry
    """

    if isinstance(aoi_input, str):
        if aoi_input.strip().startswith('{'):
            geojson = json.loads(aoi_input)
            return ee.Geometry(geojson['features'][0]['geometry'])

        elif aoi_input.lower().endswith(('.geojson', '.json')):
            with open(aoi_input, 'r') as f:
                geojson = json.load(f)
            return ee.Geometry(geojson['features'][0]['geometry'])

        elif aoi_input.lower().endswith('.shp'):
            gdf = gpd.read_file(aoi_input)
            geojson = json.loads(gdf.to_json())
            return ee.Geometry(geojson['features'][0]['geometry'])

        else:
            raise ValueError(
                "Unsupported string input. Must be a GeoJSON string, .geojson, .json, or .shp file."
            )

    elif isinstance(aoi_input, list) and len(aoi_input) == 4:
        return ee.Geometry.Rectangle(aoi_input)

    else:
        raise ValueError(
            "Unsupported AOI format. Must be a file path, GeoJSON string, or bounding box list."
        )

def load_dem(aoi, dem_source='JAXA/ALOS/AW3D30/V2_2'):
    dem = ee.Image(dem_source).select('AVE_DSM').clip(aoi)
    slope = ee.Terrain.slope(dem.focal_median(4))
    aspect = ee.Terrain.aspect(dem)
    terrain_mask = slope.focal_min(8).lt(6)
    return dem, slope, aspect, terrain_mask


def load_glacier_mask(aoi, buffer_m=200, output_dir="outputs"):
    """
    Load GLIMS glacier outlines within the AOI, buffer them, and return
    a single clipped ee.Geometry. Also exports the result as GeoJSON.

    Parameters
    ----------
    aoi : ee.Geometry
    buffer_m : int
        Buffer distance in metres.
    output_dir : str
        Directory to write glacier_geom.geojson into.

    Returns
    -------
    ee.Geometry
    """
    rgi = ee.FeatureCollection("GLIMS/20230607").filterBounds(aoi)
    buffered = rgi.map(lambda f: f.buffer(buffer_m))
    unioned = buffered.union().geometry()
    clipped = unioned.intersection(aoi)

    os.makedirs(output_dir, exist_ok=True)
    shapely_glacier_geom = ee_to_shapely(clipped)
    export_glacier_polygon(
        shapely_glacier_geom,
        crs_epsg=4326,
        output_path=os.path.join(output_dir, "glacier_geom.geojson")
    )

    return clipped


# ============================================================
# GEOMETRY UTILITIES
# ============================================================

def ee_to_shapely(ee_geom):
    geojson = ee_geom.getInfo()
    return shapely.geometry.shape(geojson)


def export_glacier_polygon(shapely_geom, crs_epsg=4326, output_path="glacier_geom.geojson"):
    """Export a Shapely geometry as GeoJSON or Shapefile."""
    gdf = gpd.GeoDataFrame({"geometry": [shapely_geom]}, crs=f"EPSG:{crs_epsg}")
    driver = "GeoJSON" if output_path.endswith(".geojson") else "ESRI Shapefile"
    gdf.to_file(output_path, driver=driver)
    print(f"Glacier polygon exported to {output_path}")


def rasterize_geom(geom, out_shape, transform):
    """Rasterize a Shapely geometry to a boolean mask."""
    mask = rasterio.features.rasterize(
        [(mapping(geom), 1)],
        out_shape=out_shape,
        transform=transform,
        fill=0,
        all_touched=True,
        dtype=np.uint8
    )
    return mask.astype(bool)


def lonlat_to_tilename(lon, lat):
    """Convert lon/lat to Hugonnet tile name convention, e.g. N45E007."""
    lat_prefix = 'N' if lat >= 0 else 'S'
    lon_prefix = 'E' if lon >= 0 else 'W'
    return f"{lat_prefix}{int(abs(lat)):02d}{lon_prefix}{int(abs(lon)):03d}"


def get_tile_names_for_geometry(geom):
    """Return the set of Hugonnet tile names intersecting an ee.Geometry."""
    coords = geom.bounds().coordinates().get(0).getInfo()
    xs = [pt[0] for pt in coords]
    ys = [pt[1] for pt in coords]
    tiles = set()
    for lat in range(math.floor(min(ys)), math.ceil(max(ys))):
        for lon in range(math.floor(min(xs)), math.ceil(max(xs))):
            tiles.add(lonlat_to_tilename(lon, lat))
    return tiles


# ============================================================
# RASTER I/O
# ============================================================

def download_tiff_tile(tile_url, download_path):
    """Download a GeoTIFF tile if not already cached or if the cached file is empty."""
    if os.path.exists(download_path):
        if os.path.getsize(download_path) > 0:
            print(f"File {download_path} already cached, skipping download.")
            return download_path
        else:
            print(f"File {download_path} is empty, re-downloading...")
            os.remove(download_path)

    print(f"Downloading {tile_url} ...")
    r = requests.get(tile_url, stream=True)
    r.raise_for_status()
    with open(download_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded to {download_path}")
    return download_path


def clip_raster_to_aoi(raster_path, aoi_geom):
    """Clip a raster to an ee.Geometry AOI, reprojecting the AOI to the raster CRS."""
    import pyproj

    sh_aoi_geom = ee_to_shapely(aoi_geom)

    with rasterio.open(raster_path) as src:
        project_to_raster_crs = pyproj.Transformer.from_crs(
            'EPSG:4326', src.crs, always_xy=True
        ).transform
        aoi_proj = transform(project_to_raster_crs, sh_aoi_geom)

        from shapely.geometry import box
        if not box(*src.bounds).intersects(aoi_proj):
            raise ValueError(f"AOI does not overlap raster tile {raster_path}")

        out_image, out_transform = rasterio.mask.mask(src, [mapping(aoi_proj)], crop=True)
        out_meta = src.meta.copy()
        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

    return out_image, out_meta


def export_raster(data, meta, out_path):
    """Write a numpy array to a GeoTIFF using the provided rasterio metadata."""
    meta.update({
        'count': data.shape[0],
        'dtype': data.dtype,
        'nodata': -9999
    })
    with rasterio.open(out_path, 'w', **meta) as dst:
        for i in range(data.shape[0]):
            dst.write(data[i], i + 1)


# ============================================================
# THINNING CORRECTION
# ============================================================

def get_glacier_thinning_correction(aoi_geom, sar_year, dem, dem_year=2000,
                                    cache_dir="thinning_cache", output_dir="outputs"):
    """
    Compute the glacier thinning correction for a given AOI and year.

    Downloads Hugonnet (2021) thinning rate tiles [m/yr over 2000-2020],
    clips them to the AOI, and scales to the number of years since the DEM epoch.
    The 25th percentile of thinning values is used as a conservative representative
    scalar, since lakes form preferentially in areas of pronounced thinning.

    Parameters
    ----------
    aoi_geom : ee.Geometry
    sar_year : int
        Target SAR acquisition year for scaling.
    dem : ee.Image
        DEM image (unused directly; kept for signature compatibility).
    dem_year : int
        Year of DEM acquisition (default 2000 for SRTM).
    cache_dir : str
        Directory to cache downloaded raster tiles.
    output_dir : str
        Directory to write clipped thinning rasters for inspection.

    Returns
    -------
    float
        Total thinning offset [m] from dem_year to sar_year.
    """
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    tile_names = get_tile_names_for_geometry(aoi_geom)
    print(f"Tiles intersecting AOI: {tile_names}")

    thinning_values = []
    pixel_counts = []

    for tile_name in tile_names:
        tile_url = f"https://services-theia.sedoo.fr/glaciers/data/v1_0/downloadtif/2000-2020/{tile_name}"
        local_tif = os.path.join(cache_dir, f"{tile_name}.tif")
        download_tiff_tile(tile_url, local_tif)

        clipped_data, clipped_meta = clip_raster_to_aoi(local_tif, aoi_geom)
        export_raster(clipped_data, clipped_meta,
                      os.path.join(output_dir, f"clipped_thinning_{tile_name}.tif"))

        # Flatten to 2D if single-band
        data_2d = clipped_data[0] if (clipped_data.ndim == 3 and clipped_data.shape[0] == 1) else clipped_data
        valid_pixels = data_2d[np.isfinite(data_2d)]
        valid_pixels = valid_pixels[valid_pixels > -9999]

        if valid_pixels.size > 0:
            thinning_values.append(np.mean(valid_pixels) * valid_pixels.size)
            pixel_counts.append(valid_pixels.size)

    if not pixel_counts:
        print("No valid thinning data found in AOI.")
        return 0.0

    mean_thinning = sum(thinning_values) / sum(pixel_counts)
    print(f"Mean thinning rate (2000-2019) over AOI: {mean_thinning:.4f} m/yr")

    percentile_25 = np.percentile(valid_pixels, 25)
    print(f"25th percentile thinning rate over AOI: {percentile_25:.4f} m/yr")

    n_years = sar_year - dem_year
    total_thinning = percentile_25 * n_years
    print(f"Estimated total thinning from {dem_year} to {sar_year}: {total_thinning:.4f} m")

    return total_thinning
    
