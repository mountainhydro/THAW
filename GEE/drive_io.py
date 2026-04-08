# -*- coding: utf-8 -*-
"""
THAW - Google Drive I/O module

Handles all GEE export → Drive → local download operations, COG conversion,
and Drive cleanup. 

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger

Created on Feb 2 2026
"""

import os
import io
import sys
import glob
import time
import ee
import rasterio
from googleapiclient.http import MediaIoBaseDownload
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from gee_auth import build_drive_service


# ============================================================
# LOGGER
# ============================================================

class Logger:
    """
    Passes stdout/stderr to both the terminal and a log file simultaneously.

    Used by both headless pipeline scripts to ensure all console output
    is preserved in the run log without losing live terminal feedback.

    Parameters
    ----------
    filename : str
        Path to the log file. Opened in append mode.
    """
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()


# ============================================================
# SHARED POLL-DOWNLOAD-DELETE LOOP
# ============================================================

def delete_drive_files(token_path, file_ids):
    """
    Permanently delete a list of Google Drive files by file ID.

    Uses the user's OAuth credentials — as file owner, permanent deletion is
    permitted. Errors are logged but never raised so a cleanup failure does
    not abort the pipeline.

    Parameters
    ----------
    token_path : str
        Path to drive_token.json.
    file_ids : list[str]
        Drive file IDs to delete.
    """
    try:
        drive = build_drive_service(token_path)
    except Exception as e:
        print(f"Warning: could not build Drive service for cleanup: {e}", flush=True)
        return
    for fid in file_ids:
        try:
            drive.files().delete(fileId=fid).execute()
            print(f"Deleted Drive file: {fid}", flush=True)
        except Exception as e:
            print(f"Warning: could not delete Drive file {fid}: {e}", flush=True)


def _download_and_merge_tiles(drive_service, file_prefix, local_path):
    """
    Find all Drive files whose name starts with file_prefix, download them,
    merge into a single GeoTIFF at local_path, and return all Drive file IDs.

    GEE appends tile suffixes (-0000000000-0000000000, -0000000000-0000016384,
    etc.) whenever an export is split across multiple files. This function
    handles both single-file and multi-tile exports transparently.

    Parameters
    ----------
    drive_service : googleapiclient.discovery.Resource
    file_prefix   : str  — GEE fileNamePrefix (without .tif)
    local_path    : str  — desired final merged output path

    Returns
    -------
    list[str]  Drive file IDs that were downloaded (empty if nothing found yet)
    """
    res = drive_service.files().list(
        q=f"name contains '{file_prefix}' and trashed=false",
        fields="files(id, name)",
        orderBy="name",
    ).execute()
    files = res.get("files", [])

    if not files:
        return []

    drive_ids = [f["id"] for f in files]
    local_dir = os.path.dirname(local_path)

    if len(files) == 1:
        # Single file — download directly to final path
        request = drive_service.files().get_media(fileId=files[0]["id"])
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        print(f"Downloaded: {files[0]['name']}", flush=True)

    else:
        # Multiple tiles — download to temp files then merge
        tile_paths = []
        for f in files:
            tile_path = os.path.join(local_dir, f["name"])
            request = drive_service.files().get_media(fileId=f["id"])
            with io.FileIO(tile_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            print(f"Downloaded tile: {f['name']}", flush=True)
            tile_paths.append(tile_path)

        print(f"Merging {len(tile_paths)} tiles into {os.path.basename(local_path)}...", flush=True)
        src_files = [rasterio.open(p) for p in tile_paths]
        try:
            # Compute combined bounds and output profile without loading data
            from rasterio.transform import from_bounds
            lefts   = [s.bounds.left   for s in src_files]
            bottoms = [s.bounds.bottom for s in src_files]
            rights  = [s.bounds.right  for s in src_files]
            tops    = [s.bounds.top    for s in src_files]
            out_left, out_bottom = min(lefts), min(bottoms)
            out_right, out_top   = max(rights), max(tops)
            res = src_files[0].res
            out_w = int(round((out_right  - out_left)   / res[1]))
            out_h = int(round((out_top    - out_bottom) / res[0]))
            out_transform = from_bounds(out_left, out_bottom, out_right, out_top, out_w, out_h)

            profile = src_files[0].profile.copy()
            profile.update({
                "height":    out_h,
                "width":     out_w,
                "transform": out_transform,
                "dtype":     "float32",
                "nodata":    src_files[0].nodata,
            })

            # Write output window by window — no full mosaic in memory
            CHUNK = 1024
            with rasterio.open(local_path, "w", **profile) as dst:
                import numpy as np
                for row_off in range(0, out_h, CHUNK):
                    row_end = min(row_off + CHUNK, out_h)
                    chunk_h = row_end - row_off
                    chunk = np.full((1, chunk_h, out_w), np.nan, dtype=np.float32)
                    for src in src_files:
                        # Find overlap between this chunk and this tile
                        from rasterio.windows import from_bounds as win_from_bounds
                        chunk_top    = out_top    - row_off * res[0]
                        chunk_bottom = out_top    - row_end * res[0]
                        win = win_from_bounds(out_left, chunk_bottom, out_right, chunk_top, src.transform)
                        try:
                            win = win.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
                        except rasterio.errors.WindowError:
                            continue  # this tile doesn't overlap this chunk row
                        if win.width <= 0 or win.height <= 0:
                            continue
                        data = src.read(1, window=win).astype(np.float32)
                        nodata = src.nodata
                        if nodata is not None:
                            data[data == nodata] = np.nan
                        # Place into correct position in chunk
                        tile_left   = src.bounds.left
                        tile_top    = src.bounds.top
                        col_start = int(round((tile_left  - out_left) / res[1]))
                        row_start = int(round((out_top    - tile_top)  / res[0])) - row_off
                        r0 = max(row_start, 0)
                        c0 = max(col_start, 0)
                        dr = r0 - row_start
                        dc = c0 - col_start
                        r1 = min(r0 + data.shape[0] - dr, chunk_h)
                        c1 = min(c0 + data.shape[1] - dc, out_w)
                        src_r1 = dr + (r1 - r0)
                        src_c1 = dc + (c1 - c0)
                        valid = ~np.isnan(data[dr:src_r1, dc:src_c1])
                        chunk[0, r0:r1, c0:c1][valid] = data[dr:src_r1, dc:src_c1][valid]
                    dst.write(chunk, window=rasterio.windows.Window(0, row_off, out_w, chunk_h))
            print(f"Merged: {os.path.basename(local_path)}", flush=True)
        finally:
            for src in src_files:
                src.close()
            for tile_path in tile_paths:
                try:
                    os.remove(tile_path)
                except Exception:
                    pass

    return drive_ids


def _poll_and_download(task_list, drive_service, token_path):
    """
    Poll a list of GEE export tasks, download each file as it completes
    (handling multi-tile exports transparently), then delete all files from Drive.

    Each item in task_list must be a dict with keys:
        task          : ee.batch.Task  — the running GEE export task
        file_prefix   : str            — GEE export fileNamePrefix (without .tif)
        local_path    : str            — full local path to write the downloaded file
        label         : str            — human-readable name used in log messages
        drive_file_ids: []             — populated with Drive IDs once downloaded
        done          : False          — set to True when the item is resolved

    Parameters
    ----------
    task_list : list[dict]
    drive_service : googleapiclient.discovery.Resource
    token_path : str
    """
    print(f"Waiting for {len(task_list)} GEE task(s)...", flush=True)
    completed = 0

    while completed < len(task_list):
        for item in task_list:
            if item["done"]:
                continue

            status = item["task"].status()

            if status["state"] == "COMPLETED":
                drive_ids = _download_and_merge_tiles(
                    drive_service, item["file_prefix"], item["local_path"]
                )
                if drive_ids:
                    item["drive_file_ids"] = drive_ids
                    item["done"] = True
                    completed += 1
                # No files visible yet in Drive — retry on next pass

            elif status["state"] in ["FAILED", "CANCELLED"]:
                print(
                    f"Task failed: {item['label']} — {status.get('error_message', '')}",
                    flush=True,
                )
                item["done"] = True
                completed += 1

        if completed < len(task_list):
            time.sleep(30)

    # Delete all downloaded files from Drive
    ids_to_delete = [
        fid
        for item in task_list
        for fid in item.get("drive_file_ids", [])
    ]
    if ids_to_delete:
        print(f"Cleaning up {len(ids_to_delete)} file(s) from Google Drive...", flush=True)
        delete_drive_files(token_path, ids_to_delete)


# ============================================================
# LAKEDETECTION EXPORT
# ============================================================

def export_and_download(images_to_export, reference_date, aoi, token_path,
                        output_root, timestamp, task_name=""):
    """
    Export a dict of GEE images to Drive, download them locally, then delete
    from Drive. Used by the lakedetection pipeline.

    Parameters
    ----------
    images_to_export : dict[str, ee.Image]
        Keys are output name prefixes (e.g. 'z_score'), values are EE images.
    reference_date : datetime.datetime
        Reference date used to name the output subdirectory.
    aoi : ee.Geometry
        Export region.
    token_path : str
        Path to drive_token.json.
    output_root : str
        Parent directory; a dated subfolder is created inside it.
    timestamp : str
        Run timestamp string (YYYYMMDD_HHMM) used in filenames.
    task_name : str, optional
        Optional suffix appended to the output folder name.

    Returns
    -------
    str
        Path to the local output directory.
    """
    date_str = reference_date.strftime("%Y-%m-%d")
    name_suffix = f"_{task_name}" if task_name else ""
    local_dir = os.path.join(output_root, f"Outputs_{date_str}{name_suffix}")
    os.makedirs(local_dir, exist_ok=True)

    drive_service = build_drive_service(token_path)
    task_list = []

    for name, img in images_to_export.items():
        file_prefix = f"{name}_{timestamp}"
        task = ee.batch.Export.image.toDrive(
            image=img,
            description=file_prefix,
            folder="GEE_Exports",
            fileNamePrefix=file_prefix,
            region=aoi,
            scale=10,
            maxPixels=1e12,
        )
        task.start()
        task_list.append({
            "task":           task,
            "file_prefix":    file_prefix,
            "local_path":     os.path.join(local_dir, f"{file_prefix}.tif"),
            "label":          name,
            "drive_file_ids": [],
            "done":           False,
        })
        print(f"Started GEE task: {name}", flush=True)

    _poll_and_download(task_list, drive_service, token_path)
    return local_dir


def resume_download(export_names, reference_date, token_path, output_root,
                    timestamp, task_name=""):
    """
    Resume a failed lakedetection download without re-running GEE tasks.

    Use this when GEE exports completed and files are still in Drive, but the
    pipeline crashed before finishing the download/merge/COG/cluster steps.
    Files that already exist locally are skipped automatically.

    Parameters
    ----------
    export_names : list[str]
        Export name prefixes to recover, e.g. ['potential_water', 'z_score', 'mean_diff'].
    reference_date : datetime.datetime
        Reference date used to locate the output subdirectory.
    token_path : str
        Path to drive_token.json.
    output_root : str
        Parent directory containing the dated output subfolder.
    timestamp : str
        Run timestamp string (YYYYMMDD_HHMM) from the failed run.
    task_name : str, optional
        Task name suffix used in the output folder name.

    Returns
    -------
    str
        Path to the local output directory.
    """
    date_str = reference_date.strftime("%Y-%m-%d")
    name_suffix = f"_{task_name}" if task_name else ""
    local_dir = os.path.join(output_root, f"Outputs_{date_str}{name_suffix}")
    os.makedirs(local_dir, exist_ok=True)

    drive_service = build_drive_service(token_path)

    for name in export_names:
        file_prefix = f"{name}_{timestamp}"
        local_path = os.path.join(local_dir, f"{file_prefix}.tif")

        if os.path.exists(local_path):
            print(f"Already exists, skipping: {os.path.basename(local_path)}", flush=True)
            continue

        print(f"Resuming download: {file_prefix}", flush=True)
        drive_ids = _download_and_merge_tiles(drive_service, file_prefix, local_path)

        if drive_ids:
            delete_drive_files(token_path, drive_ids)
        else:
            print(f"Warning: no Drive files found for {file_prefix}. "
                  f"They may have already been deleted.", flush=True)

    return local_dir


# ============================================================
# TRACKING EXPORT
# ============================================================

def export_images_via_drive(s1_collection, aoi_ee, token_path,
                            bands_to_export=None, output_dir="outputs",
                            prefix="S1", scale=10, drive_folder="GEE_Exports"):
    """
    Export each image × band from a Sentinel-1 collection to Drive, download
    locally, then delete from Drive. Used by the tracking pipeline.

    Parameters
    ----------
    s1_collection : ee.ImageCollection
        Scored Sentinel-1 collection to export.
    aoi_ee : ee.Geometry
        Export region.
    token_path : str
        Path to drive_token.json.
    bands_to_export : list[str], optional
        Bands to export. Defaults to ['VV_raw', 'VV_corrected', 'VV_smoothed'].
    output_dir : str, optional
        Local directory for downloaded files (default 'outputs').
    prefix : str, optional
        Filename prefix (default 'S1').
    scale : int, optional
        Export resolution in metres (default 10).
    drive_folder : str, optional
        GEE Drive export folder name (default 'GEE_Exports').
    """
    if bands_to_export is None:
        bands_to_export = ["VV_raw", "VV_corrected", "VV_smoothed"]

    os.makedirs(output_dir, exist_ok=True)
    drive_service = build_drive_service(token_path)

    count = s1_collection.size().getInfo()
    s1_list = s1_collection.toList(count)
    task_list = []

    for i in range(count):
        img = ee.Image(s1_list.get(i))
        img_id = (img.id().getInfo() or f"img{i:03d}").replace("/", "_")

        for band in bands_to_export:
            local_filename = f"{prefix}_{band}_{img_id}.tif"
            local_path = os.path.join(output_dir, local_filename)

            if os.path.exists(local_path):
                print(f"File exists, skipping: {local_filename}", flush=True)
                continue

            file_prefix = local_filename[:-4]  # GEE appends .tif automatically

            try:
                band_image = img.select(band).clip(aoi_ee)
                task = ee.batch.Export.image.toDrive(
                    image=band_image,
                    description=file_prefix[:100],  # GEE description limit
                    folder=drive_folder,
                    fileNamePrefix=file_prefix,
                    region=aoi_ee.bounds(),
                    scale=scale,
                    maxPixels=1e12,
                )
                task.start()
                task_list.append({
                    "task":           task,
                    "file_prefix":    file_prefix,
                    "local_path":     local_path,
                    "label":          local_filename,
                    "drive_file_ids": [],
                    "done":           False,
                })
                print(f"Task started: {local_filename}", flush=True)
            except Exception as e:
                print(f"Failed to start task for {local_filename}: {e}", flush=True)

    if not task_list:
        print("No tasks to run (all files already exist or none launched).", flush=True)
        return

    _poll_and_download(task_list, drive_service, token_path)


# ============================================================
# COG CONVERSION
# ============================================================

def convert_to_cog(folder):
    """
    Convert all plain GeoTIFFs in a folder to Cloud-Optimised GeoTIFFs (COG).

    Files already ending in _cog.tif are skipped to avoid double-conversion.

    Parameters
    ----------
    folder : str
        Directory containing .tif files to convert.
    """
    dst_profile = cog_profiles.get("deflate")

    for tif in glob.glob(os.path.join(folder, "*.tif")):
        if tif.endswith("_cog.tif"):
            continue
        output_cog = tif.replace(".tif", "_cog.tif")
        print(f"Converting to COG: {os.path.basename(tif)}...", flush=True)
        try:
            cog_translate(tif, output_cog, dst_profile, in_memory=False, quiet=True)
        except Exception as e:
            print(f"Failed to convert {tif}: {e}", flush=True)
