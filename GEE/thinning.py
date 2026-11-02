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

DEM_EPOCH_YEAR = 2013  # GLO-30 is built from TanDEM-X acquisitions spanning 2011-2015
TERRAIN_MASK_SCALE_M = 10  # matches the export resolution of the tracking pipeline


def utm_crs_for_aoi(aoi):
    """EPSG code string of the UTM zone containing the AOI's centre."""
    coords = aoi.bounds().coordinates().get(0).getInfo()
    lon = sum(pt[0] for pt in coords) / len(coords)
    lat = sum(pt[1] for pt in coords) / len(coords)
    zone = int((lon + 180) // 6) + 1
    return f"EPSG:{(32600 if lat >= 0 else 32700) + zone}"


def compute_terrain_mask(slope, crs, terrain_buffer_m=200, scale_m=TERRAIN_MASK_SCALE_M):
    """Valid terrain = flat (<6 deg) pixels plus a buffer around them, so lakes that
    have grown beyond the flat area of the (older) DEM are not masked out.

    Evaluated on a fixed UTM grid at the export resolution, with the buffer in whole
    pixels of that grid. A kernel in metres fails at export time ('non-standard
    transform'), and one in pixels on the DEM's own projection would change with the
    export resolution."""
    radius_px = max(1, int(round(terrain_buffer_m / scale_m)))
    return slope.reproject(ee.Projection(crs).atScale(scale_m)) \
        .focal_min(radius_px, 'circle', 'pixels').lt(6)


def load_dem(aoi, dem_source='COPERNICUS/DEM/GLO30_2024_1', terrain_buffer_m=200):
    # COPDEM GLO-30 is tiled as an ImageCollection (unlike the deprecated,
    # single-image JAXA/ALOS/AW3D30/V2_2 this replaces) — mosaic and
    # reproject to its native projection, matching the main lakedetection
    # pipeline's DEM handling.
    glo30 = ee.ImageCollection(dem_source)
    glo30_proj = glo30.first().projection()
    dem = glo30.select('DEM').mosaic().setDefaultProjection(glo30_proj).clip(aoi)
    slope = ee.Terrain.slope(dem.focal_median(4))
    aspect = ee.Terrain.aspect(dem)
    terrain_mask = compute_terrain_mask(slope, utm_crs_for_aoi(aoi), terrain_buffer_m)
    return dem, slope, aspect, terrain_mask


def load_thinning_rate_image(aoi, cache_dir="thinning_cache", grid_res=30,
                             margin_m=300, max_cells=100000):
    """
    Build an ee.Image of the Hugonnet (2021) 2000-2020 surface elevation change
    rate [m/yr] covering the AOI, for lowering the DEM pixel by pixel.

    The cached rate tiles are resampled (bilinear) onto a regular UTM grid
    matching the DEM resolution and embedded in the image as a constant array
    that is looked up per pixel. Pixels without data (outside glaciers) are 0.

    Parameters
    ----------
    aoi : ee.Geometry
        Area covered by the DEM.
    cache_dir : str
        Directory with the cached Hugonnet tiles.
    grid_res : float
        Grid resolution in metres (default 30, the DEM resolution); coarsened
        automatically if the grid would exceed max_cells.
    margin_m : float
        Margin added around the AOI bounds, in metres.
    max_cells : int
        Upper bound on the embedded array size.

    Returns
    -------
    ee.Image or None
        Single-band 'dhdt' image in m/yr, or None if there is no thinning data.
    """
    from rasterio.transform import from_origin
    from rasterio.warp import reproject, Resampling, transform_bounds

    os.makedirs(cache_dir, exist_ok=True)
    coords = aoi.bounds().coordinates().get(0).getInfo()
    xs = [pt[0] for pt in coords]
    ys = [pt[1] for pt in coords]
    zone = int((((min(xs) + max(xs)) / 2) + 180) // 6) + 1
    epsg = (32600 if (min(ys) + max(ys)) / 2 >= 0 else 32700) + zone
    crs = f"EPSG:{epsg}"

    left, bottom, right, top = transform_bounds(
        "EPSG:4326", crs, min(xs), min(ys), max(xs), max(ys)
    )
    left, bottom, right, top = left - margin_m, bottom - margin_m, right + margin_m, top + margin_m
    res = float(grid_res)
    while ((right - left) / res) * ((top - bottom) / res) > max_cells:
        res *= 1.5
    width = int(math.ceil((right - left) / res))
    height = int(math.ceil((top - bottom) / res))
    dst_transform = from_origin(left, top, res, res)

    rate = np.full((height, width), np.nan, dtype="float32")
    for tile_name in sorted(get_tile_names_for_geometry(aoi)):
        tile_url = f"https://services-theia.sedoo.fr/glaciers/data/v1_0/downloadtif/2000-2020/{tile_name}"
        local_tif = os.path.join(cache_dir, f"{tile_name}.tif")
        download_tiff_tile(tile_url, local_tif)
        tmp = np.full((height, width), np.nan, dtype="float32")
        with rasterio.open(local_tif) as src:
            reproject(
                source=rasterio.band(src, 1), destination=tmp,
                src_transform=src.transform, src_crs=src.crs,
                src_nodata=src.nodata if src.nodata is not None else -9999,
                dst_transform=dst_transform, dst_crs=crs, dst_nodata=np.nan,
                resampling=Resampling.bilinear,
            )
        tmp[tmp <= -9999] = np.nan
        rate = np.where(np.isfinite(tmp), tmp, rate)

    if not np.isfinite(rate).any():
        return None

    rate = np.nan_to_num(rate, nan=0.0)

    pix = ee.Image.pixelCoordinates(ee.Projection(crs, [res, 0, left, 0, -res, top]))
    row = pix.select('y').floor().toInt().clamp(0, height - 1).toInt()
    col = pix.select('x').floor().toInt().clamp(0, width - 1).toInt()
    inside = pix.select('x').floor().gte(0).And(pix.select('x').floor().lt(width)) \
        .And(pix.select('y').floor().gte(0)).And(pix.select('y').floor().lt(height))
    return ee.Image(ee.Array(rate.tolist())) \
        .arraySlice(0, row, row.add(1)).arraySlice(1, col, col.add(1)) \
        .arrayGet([0, 0]).updateMask(inside).unmask(0).rename('dhdt')


def load_dem_water_mask(aoi, dem_source='COPERNICUS/DEM/GLO30_2024_1'):
    """Binary mask of pixels the DEM itself classifies as water (lakes, rivers; band WBM)."""
    glo30 = ee.ImageCollection(dem_source)
    return glo30.select('WBM').mosaic().setDefaultProjection(glo30.first().projection()) \
        .clip(aoi).gt(0).unmask(0)


def make_dem_for_image(dem, thinning_rate, aoi, water_mask=None, terrain_buffer_m=200,
                       dem_epoch=DEM_EPOCH_YEAR):
    """
    Return a function mapping a Sentinel-1 image to (dem, terrain_mask) with the
    DEM lowered by the Hugonnet rate for the years between the DEM epoch and the
    image's acquisition year. The slope and terrain mask are recomputed on that
    adjusted DEM.

    Pixels in water_mask (the DEM's own flattened lake surfaces) are not lowered,
    since a spatially varying lowering would tilt their flat surface and wreck
    the flat-terrain footprint the mask is built from.
    """
    rate = thinning_rate if water_mask is None else thinning_rate.multiply(water_mask.Not())
    crs = utm_crs_for_aoi(aoi)

    def dem_for_image(image):
        year = ee.Date(image.get('system:time_start')).get('year')
        years = ee.Number(year).subtract(dem_epoch).max(0)
        dem_i = dem.add(rate.multiply(years))
        slope_i = ee.Terrain.slope(dem_i.focal_median(4))
        return dem_i, compute_terrain_mask(slope_i, crs, terrain_buffer_m)
    return dem_for_image


def load_glacier_mask(aoi, buffer_m=200, output_dir="outputs"):
    """
    Load GLIMS glacier outlines within the AOI, buffer them, and return
    a single clipped ee.Geometry. Also exports the result as GeoJSON.
    Falls back to the AOI itself if no glacier outlines are found.

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

    if rgi.size().getInfo() == 0:
        print("Warning: no glacier outlines found in AOI — skipping glacier mask and thinning correction.", flush=True)
        return None

    buffered = rgi.map(lambda f: f.buffer(buffer_m))
    unioned = buffered.union().geometry()
    geom = unioned.intersection(aoi)

    os.makedirs(output_dir, exist_ok=True)
    shapely_glacier_geom = ee_to_shapely(geom)
    export_glacier_polygon(
        shapely_glacier_geom,
        crs_epsg=4326,
        output_path=os.path.join(output_dir, "glacier_geom.geojson")
    )

    return geom


def get_elevation_threshold_from_nearest_glacier(
    aoi, elev_image, search_radius_m=100_000, margin_m=500, fallback_m=1000
):
    """
    Derive an elevation threshold from the lowest DEM pixel of the nearest
    GLIMS glacier within search_radius_m of the AOI.

    Returns float: (min_glacier_elevation - margin_m).
    Falls back to fallback_m if no glacier is found within the search radius.
    """
    search_area = aoi.buffer(search_radius_m)
    glims = ee.FeatureCollection("GLIMS/20230607").filterBounds(search_area)

    count = glims.size().getInfo()
    if count == 0:
        print(f"Terrain elevation threshold: {fallback_m} m (fallback, no glaciers within {search_radius_m/1000:.0f} km).", flush=True)
        return float(fallback_m)

    centroid = aoi.centroid(maxError=100)
    nearest = (
        glims
        .map(lambda f: f.set("_dist", f.geometry().distance(centroid, maxError=100)))
        .sort("_dist")
        .first()
    )
    nearest_geom = nearest.geometry()

    result = elev_image.reduceRegion(
        reducer=ee.Reducer.toList(),
        geometry=nearest_geom,
        scale=100,
        maxPixels=1e9,
        bestEffort=True,
    ).getInfo()

    values = sorted(
        v for v in (next(iter(result.values()), []) if result else [])
        if v is not None
    )
    lowest_5 = values[:5]
    min_elev = float(np.median(lowest_5)) if lowest_5 else None

    if min_elev is None:
        print(f"Terrain elevation threshold: {fallback_m} m (fallback, glacier elevation unreadable).", flush=True)
        return float(fallback_m)

    threshold = float(min_elev) - margin_m
    print(f"Terrain elevation threshold: {threshold:.0f} m (nearest glacier min {min_elev:.0f} m − {margin_m} m).", flush=True)
    return threshold


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
            return download_path
        os.remove(download_path)

    r = requests.get(tile_url, stream=True)
    r.raise_for_status()
    with open(download_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
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

    percentile_25 = np.percentile(valid_pixels, 25)

    n_years = sar_year - dem_year
    total_thinning = percentile_25 * n_years

    return total_thinning
    
