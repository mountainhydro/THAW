import ee
import json
import os
import geopandas as gpd
import os
import requests
import rasterio
import rasterio.mask
import rasterio.features
import numpy as np
from shapely.geometry import shape, mapping, box
import math
import shapely.geometry
import rasterio
from rasterio.mask import mask


def load_aoi(aoi_input):
    """
    Load an AOI (Area of Interest) from a GeoJSON string, file, shapefile, or bounding box list.

    Parameters:
        aoi_input: str or list
            - GeoJSON string
            - Path to a GeoJSON or Shapefile
            - Bounding box list: [xmin, ymin, xmax, ymax]

    Returns:
        ee.Geometry
    """

    if isinstance(aoi_input, str):
        if aoi_input.strip().startswith('{'):
            # GeoJSON string
            geojson = json.loads(aoi_input)
            coords = geojson['features'][0]['geometry']['coordinates']
            return ee.Geometry(geojson['features'][0]['geometry'])

        elif aoi_input.lower().endswith('.geojson') or aoi_input.lower().endswith('.json'):
            with open(aoi_input, 'r') as f:
                geojson = json.load(f)
            return ee.Geometry(geojson['features'][0]['geometry'])

        elif aoi_input.lower().endswith('.shp'):
            gdf = gpd.read_file(aoi_input)
            geojson = json.loads(gdf.to_json())
            return ee.Geometry(geojson['features'][0]['geometry'])

        else:
            raise ValueError("Unsupported string input format. Must be GeoJSON string, .geojson, .json, or .shp file.")

    elif isinstance(aoi_input, list) and len(aoi_input) == 4:
        return ee.Geometry.Rectangle(aoi_input)

    else:
        raise ValueError("Unsupported AOI input format. Must be a file path, GeoJSON string, or bounding box list.")

def load_dem(aoi, dem_source='JAXA/ALOS/AW3D30/V2_2'):
    dem = ee.Image(dem_source).select('AVE_DSM').clip(aoi)
    slope = ee.Terrain.slope(dem.focal_median(4))
    aspect = ee.Terrain.aspect(dem)
    terrain_mask = slope.focal_min(8).lt(6)
    return dem, slope, aspect, terrain_mask

def load_glacier_mask(aoi, buffer_m=200, output_dir="outputs"):
    """
    Loads the GLIMS glacier outlines within the AOI and returns a single geometry
    buffered and clipped to the AOI.

    Parameters:
        aoi (ee.Geometry): Area of interest
        buffer_m (int): Buffer distance in meters
        output_dir (str): Directory to write glacier_geom.geojson into

    Returns:
        ee.Geometry: Buffered and clipped union of glacier outlines
    """
    rgi = ee.FeatureCollection("GLIMS/20230607").filterBounds(aoi)
    buffered = rgi.map(lambda f: f.buffer(buffer_m))

    # Merge all buffered outlines into one geometry
    unioned = buffered.union().geometry()

    # Clip the buffered glacier geometry to the AOI
    clipped = unioned.intersection(aoi)

    # Export shapefile for inspection
    os.makedirs(output_dir, exist_ok=True)
    shapely_glacier_geom = ee_to_shapely(clipped)
    export_glacier_polygon(shapely_glacier_geom, crs_epsg=4326,
                           output_path=os.path.join(output_dir, "glacier_geom.geojson"))

    return clipped


def export_glacier_polygon(shapely_geom, crs_epsg=4326, output_path="glacier_geom.geojson"):
    """
    Export a Shapely geometry as a GeoJSON (or shapefile).
    
    shapely_geom: Shapely geometry (Polygon or MultiPolygon)
    crs_epsg: coordinate reference system (default WGS84 EPSG:4326)
    output_path: filename, .geojson or .shp
    """
    # Create GeoDataFrame with one geometry
    gdf = gpd.GeoDataFrame({"geometry": [shapely_geom]}, crs=f"EPSG:{crs_epsg}")

    # Save to file
    gdf.to_file(output_path, driver="GeoJSON" if output_path.endswith(".geojson") else "ESRI Shapefile")

    print(f"Glacier polygon exported to {output_path}")

def download_tiff_tile(tile_url, download_path):
    """Download GeoTIFF tile if not present or if existing file is empty/corrupted."""
    if os.path.exists(download_path):
        if os.path.getsize(download_path) > 0:
            print(f"File {download_path} already exists, skipping download.")
            return download_path
        else:
            print(f"File {download_path} exists but is empty. Re-downloading...")
            os.remove(download_path)

    print(f"Downloading {tile_url} ...")
    r = requests.get(tile_url, stream=True)
    r.raise_for_status()
    with open(download_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded to {download_path}")
    return download_path

def ee_to_shapely(ee_geom):
    geojson = ee_geom.getInfo()  # gets geojson dict
    shapely_geom = shapely.geometry.shape(geojson)
    return shapely_geom

def clip_raster_to_aoi(raster_path, aoi_geom):
    import rasterio
    import rasterio.mask
    from shapely.geometry import mapping
    from shapely.ops import transform
    import pyproj
    
    sh_aoi_geom = ee_to_shapely(aoi_geom)

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs

        # Reproject AOI to raster CRS
        project_to_raster_crs = pyproj.Transformer.from_crs(
            'EPSG:4326', raster_crs, always_xy=True).transform
        aoi_proj = transform(project_to_raster_crs, sh_aoi_geom)

        geom_for_mask = [mapping(aoi_proj)]

        # Optional: check intersection before masking
        from shapely.geometry import box
        raster_bbox = box(*src.bounds)
        if not raster_bbox.intersects(aoi_proj):
            raise ValueError(f"AOI does not overlap raster tile {raster_path}")

        out_image, out_transform = rasterio.mask.mask(src, geom_for_mask, crop=True)
        out_meta = src.meta.copy()

        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

    return out_image, out_meta


# def compute_mean_thinning_over_aoi(tile_url, aoi_geom, glacier_mask=None, cache_dir="thinning_cache"):
#     """
#     Download tile, clip to AOI, compute mean thinning over glacier area within AOI.
#     Optionally mask to glacier area only (if glacier_mask is a boolean mask array same shape as clipped data).
#     """
#     os.makedirs(cache_dir, exist_ok=True)
#     tile_name = tile_url.rstrip('/').split('/')[-1]
#     local_tif = os.path.join(cache_dir, f"{tile_name}.tif")

#     # Download tile if needed
#     download_tiff_tile(tile_url + ".tif", local_tif)

#     # Clip to AOI
#     clipped_data, clipped_meta = clip_raster_to_aoi(local_tif, aoi_geom)

#     if glacier_mask is not None:
#         masked_data = np.where(glacier_mask, clipped_data, np.nan)
#     else:
#         masked_data = clipped_data

#     # Remove nodata values (assuming nodata is zero or less or a specific value)
#     # Here we assume no data <= -9999
#     valid_pixels = masked_data[np.isfinite(masked_data)]
#     valid_pixels = valid_pixels[valid_pixels > -9999]

#     if valid_pixels.size == 0:
#         print("No valid thinning data found in AOI!")
#         return 0.0

#     mean_thinning = np.mean(valid_pixels)
#     print(f"Mean thinning (2000-2019) over AOI: {mean_thinning:.4f} meters")

#     return mean_thinning


def scale_thinning_for_year(mean_thinning_2000_2019, year):
    """
    Scale the thinning to a specific year assuming linear thinning from 2000-2019.
    Year must be in 2000-2019 range or extrapolated.
    """
    base_year = 2000
    end_year = 2019
    total_years = end_year - base_year + 1
    if year < base_year:
        return 0.0
    elif year > end_year:
        # Extrapolate linearly beyond 2019
        return mean_thinning_2000_2019 * (year - base_year + 1) / total_years
    else:
        return mean_thinning_2000_2019 * (year - base_year + 1) / total_years

   
def lonlat_to_tilename(lon, lat):
    """Convert lon/lat to the tile naming convention e.g. N45E007"""
    lat_prefix = 'N' if lat >= 0 else 'S'
    lon_prefix = 'E' if lon >= 0 else 'W'
    return f"{lat_prefix}{int(abs(lat)):02d}{lon_prefix}{int(abs(lon)):03d}"

def get_tile_names_for_geometry(geom):
    # geom: ee.Geometry
    bounds_geom = geom.bounds()  # ee.Geometry.Rectangle
    coords = bounds_geom.coordinates().get(0).getInfo()  # list of coordinates of rectangle polygon
    # coords is a list of [ [x1, y1], [x2, y2], [x3, y3], [x4, y4], [x1, y1] ]

    # Extract minx, miny, maxx, maxy from coordinates
    xs = [pt[0] for pt in coords]
    ys = [pt[1] for pt in coords]

    minx = min(xs)
    maxx = max(xs)
    miny = min(ys)
    maxy = max(ys)

    tiles = set()
    for lat in range(math.floor(miny), math.ceil(maxy)):
        for lon in range(math.floor(minx), math.ceil(maxx)):
            tiles.add(lonlat_to_tilename(lon, lat))
    return tiles

def rasterize_geom(geom, out_shape, transform):
    """Rasterize shapely geometry to a boolean mask"""
    mask = rasterio.features.rasterize(
        [(mapping(geom), 1)],
        out_shape=out_shape,
        transform=transform,
        fill=0,
        all_touched=True,
        dtype=np.uint8
    )
    return mask.astype(bool)

        
def export_raster(data, meta, out_path):
    meta.update({
        'count': data.shape[0],  # number of bands
        'dtype': data.dtype,
        'nodata': -9999  # or whatever nodata value used
    })

    with rasterio.open(out_path, 'w', **meta) as dst:
        for i in range(data.shape[0]):
            dst.write(data[i], i+1)
        
def get_glacier_thinning_correction(aoi_geom, sar_year, dem, dem_year=2000,
                                    cache_dir="thinning_cache", output_dir="outputs"):
    """
    Computes glacier thinning correction for a given AOI and year by:
    - Determining intersecting tiles
    - Clipping each tile to the AOI
    - Averaging valid thinning pixels

    Parameters:
        aoi_geom (ee.Geometry): Area of interest (can be glacier-restricted)
        sar_year (int): Target year for scaling thinning
        dem: DEM image (unused directly, kept for signature compatibility)
        dem_year (int): Base year of the DEM (default 2000)
        cache_dir (str): Directory to cache/download raster tiles
        output_dir (str): Directory to write clipped thinning rasters into

    Returns:
        float: Scaled mean thinning for the AOI and specified year
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

        # Clip thinning raster to AOI
        clipped_data, clipped_meta = clip_raster_to_aoi(local_tif, aoi_geom)

        export_raster(clipped_data, clipped_meta,
                      os.path.join(output_dir, f"clipped_thinning_{tile_name}.tif"))

        # Ensure data is 2D (single band)
        if clipped_data.ndim == 3 and clipped_data.shape[0] == 1:
            data_2d = clipped_data[0]
        else:
            data_2d = clipped_data

        # Filter out invalid values
        valid_pixels = data_2d[np.isfinite(data_2d)]
        valid_pixels = valid_pixels[valid_pixels > -9999]

        if valid_pixels.size > 0:
            thinning_values.append(np.mean(valid_pixels) * valid_pixels.size)
            pixel_counts.append(valid_pixels.size)

    if not pixel_counts:
        print("No valid thinning data found in AOI.")
        return 0.0

    percentile_75 = np.percentile(valid_pixels, 25)
    print(f"75th centile thinning (2000–2019) over AOI: {percentile_75:.4f} m. Lakes form in areas of pronounced thinning")

    # Compute weighted mean
    mean_thinning = sum(thinning_values) / sum(pixel_counts)
    print(f"Mean thinning (2000–2019) over AOI: {mean_thinning:.4f} m")

    # Scale based on years since DEM
    n_years = sar_year - dem_year
    total_thinning = percentile_75 * n_years
    print(f"Estimated total thinning (using 75th centile value) from {dem_year} to {sar_year}: {total_thinning:.4f} m")

    return total_thinning


# Example usage:
if __name__ == "__main__":
    from shapely.geometry import box

    # Define AOI (replace with your actual AOI polygon, e.g. read from GeoJSON)
    aoi = box(7.2, 44.8, 7.3, 44.9)  # Example AOI inside tile N45E007

    # URL of the thinning tile (no file extension, function adds '.tif')
    tile_url = "https://services-theia.sedoo.fr/glaciers/data/v1_0/downloadtif/2000-2020/N45E007"

    mean_thinning = compute_mean_thinning_over_aoi(tile_url, aoi)

    # Example scale for year 2015
    year = 2015
    scaled_thinning = scale_thinning_for_year(mean_thinning, year)
    print(f"Scaled thinning for year {year}: {scaled_thinning:.4f} meters")
    
