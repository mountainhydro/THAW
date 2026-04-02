# -*- coding: utf-8 -*-
"""
THAW - Google Drive I/O module

Handles all GEE export → Drive → local download operations, COG conversion,
and Drive cleanup. Both pipeline scripts import from here so the poll-download-
delete loop is never duplicated.
"""

import os
import io
import sys
import glob
import time
import ee
from googleapiclient.http import MediaIoBaseDownload
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from gee_auth import build_drive_service





# ============================================================
# LOGGER
# ============================================================

class Logger:
    """
    Tees stdout/stderr to both the terminal and a log file simultaneously.
    Used by both headless pipeline scripts.
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
    permitted. Errors are logged but never raised so a cleanup failure does not
    abort the pipeline.

    Parameters
    ----------
    token_path : str       — path to drive_token.json
    file_ids   : list[str] — Drive file IDs to delete
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
            
def _poll_and_download(task_list, drive_service, token_path):
    """
    Poll a list of GEE export tasks, download each file as it completes,
    then permanently delete all downloaded files from Drive.

    Each item in task_list must be a dict with:
        task         : ee.batch.Task — the running GEE export task
        file_prefix  : str           — GEE export fileNamePrefix (without .tif)
        local_path   : str           — full local path to write the downloaded file
        label        : str           — human-readable name used in log messages
        drive_file_id: None          — populated once the file is found in Drive
        done         : False         — set to True when the item is resolved

    Parameters
    ----------
    task_list    : list[dict]
    drive_service: googleapiclient.discovery.Resource  — authenticated Drive client
    token_path   : str  — path to drive_token.json, used for the cleanup step
    """
    print(f"Waiting for {len(task_list)} GEE task(s)...", flush=True)
    completed = 0

    while completed < len(task_list):
        for item in task_list:
            if item["done"]:
                continue

            status = item["task"].status()

            if status["state"] == "COMPLETED":
                fname = f"{item['file_prefix']}.tif"
                res = drive_service.files().list(
                    q=f"name='{fname}' and trashed=false",
                    fields="files(id)",
                ).execute()
                files = res.get("files", [])

                if files:
                    file_id = files[0]["id"]
                    request = drive_service.files().get_media(fileId=file_id)
                    with io.FileIO(item["local_path"], "wb") as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                    print(f"Downloaded: {fname}", flush=True)
                    item["drive_file_id"] = file_id
                    item["done"] = True
                    completed += 1
                # File not yet visible in Drive — retry on next pass

            elif status["state"] in ["FAILED", "CANCELLED"]:
                print(
                    f"Task failed: {item['label']} — {status.get('error_message', '')}",
                    flush=True,
                )
                item["done"] = True
                completed += 1

        if completed < len(task_list):
            time.sleep(30)

    # Delete all successfully downloaded files from Drive
    ids_to_delete = [item["drive_file_id"] for item in task_list if item.get("drive_file_id")]
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
    reference_date   : datetime.datetime
    aoi              : ee.Geometry
    token_path       : str
    output_root      : str   — parent directory; a dated subfolder is created here
    timestamp        : str   — run timestamp string (YYYYMMDD_HHMM)
    task_name        : str   — optional suffix for the output folder name

    Returns
    -------
    str : path to the local output directory
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
            "task":          task,
            "file_prefix":   file_prefix,
            "local_path":    os.path.join(local_dir, f"{file_prefix}.tif"),
            "label":         name,
            "drive_file_id": None,
            "done":          False,
        })
        print(f"Started GEE Task: {name}", flush=True)

    _poll_and_download(task_list, drive_service, token_path)
    return local_dir


# ============================================================
# TRACKING EXPORT
# ============================================================

def export_images_via_drive(s1_collection, aoi_ee, token_path,
                            bands_to_export=None, output_dir="outputs",
                            prefix="S1", scale=10, drive_folder="GEE_Exports"):
    """
    Export each image × band combination from a Sentinel-1 collection to Drive,
    download locally, then delete from Drive. Used by the tracking pipeline.

    Parameters
    ----------
    s1_collection  : ee.ImageCollection  — scored Sentinel-1 collection
    aoi_ee         : ee.Geometry or shapely geometry
    token_path     : str
    bands_to_export: list[str]  — defaults to VV_raw, VV_corrected, VV_smoothed
    output_dir     : str        — local directory for downloaded files
    prefix         : str        — filename prefix (e.g. 'tracking_s1')
    scale          : int        — export resolution in metres
    drive_folder   : str        — GEE Drive export folder name
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
                    "task":          task,
                    "file_prefix":   file_prefix,
                    "local_path":    local_path,
                    "label":         local_filename,
                    "drive_file_id": None,
                    "done":          False,
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
    Files already ending in _cog.tif are skipped.

    Parameters
    ----------
    folder : str — directory containing .tif files to convert
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
