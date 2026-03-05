import numpy as np
import rasterio
from rasterio.features import shapes
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from shapely.geometry import shape
from shapely.ops import unary_union


tif = r"C:\Users\fugger\Documents\Lake_detection\THAW\Outputs\Outputs_2025-07-05\z_score_20260204_1519_cog.tif"

z_thres = -1.5 # threshold for z-score to consider as potential water 
min_size_cluster = 20 # minimum number of pixels in a cluster to be considered valid
pix = 3 # number of pixels tolerance for clustering (eps parameter in DBSCAN)

with rasterio.open(tif) as src:
    data = src.read(1).astype(float)
    nodata = src.nodata
    transform = src.transform
data = np.where(data <= 0, data, np.nan)

extent = [
    transform.c,                                    # left   (min longitude)
    transform.c + transform.a * data.shape[1],      # right  (max longitude)
    transform.f + transform.e * data.shape[0],      # bottom (min latitude)
    transform.f                                      # top    (max latitude)
]

candidate = data <= z_thres
ys, xs = np.nonzero(candidate)
coords = np.column_stack([ys, xs])

db = DBSCAN(eps=pix, min_samples=min_size_cluster).fit(coords)
labels = db.labels_
labels_raster = np.full(candidate.shape, -1, dtype=int)
labels_raster[ys, xs]=labels.astype(int)

geom_map = {}
for geom, val in shapes(labels_raster, mask=(labels_raster != -1), transform=transform):
    lbl = int(val)
    geom_map.setdefault(lbl, []).append(shape(geom))

clusters = []
for lbl, geoms in geom_map.items():
    merged = unary_union(geoms)
    pix_count = int((labels_raster == lbl).sum())
    clusters.append({"label": int(lbl), "pixels": pix_count, "geometry": merged})
n_clusters = len(clusters)








