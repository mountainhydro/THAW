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
from rasterio.merge import merge as rio_merge
from googleapiclient.http import MediaIoBaseDownload
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from gee_auth import build_drive_service


class CancelledError(RuntimeError):
    """Raised when GEE tasks are cancelled by the user."""
    pass


class CancelledError(RuntimeError):
    """Raised when one or more GEE tasks were cancelled by the user."""
    pass


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
        import io as _io
        # Wrap terminal in a UTF-8 writer so Unicode characters don't crash on
        # Windows where the default console encoding is cp1252.
        raw = sys.stdout
        if hasattr(raw, 'buffer'):
            self.terminal = _io.TextIOWrapper(raw.buffer, encoding='utf-8', errors='replace', line_buffering=True)
        else:
            self.terminal = raw
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
    Errors are logged but never raised so cleanup never aborts the pipeline.
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
            print(f"Warning: could not delete Drive file {fid} ({type(e).__name__}: {e})", flush=True)


def _poll_and_download(task_list, drive_service, token_path):
    """
    Poll a list of GEE export tasks, download each file as it completes,
    then permanently delete all downloaded files from Drive.
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
                safe_fname = fname.replace("'", "\\'")
                res = drive_service.files().list(
                    q=f"name='{safe_fname}' and trashed=false",
                    fields="files(id)",
                ).execute()
                files = res.get("files", [])

                if files:
                    file_id = files[0]["id"].strip().rstrip("-")
                    if not file_id:
                        item["done"] = True
                        completed += 1
                        continue
                    request = drive_service.files().get_media(fileId=file_id)
                    with io.FileIO(item["local_path"], "wb") as fh:
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                    print(f"Downloaded: {fname}", flush=True)
                    item["drive_file_ids"] = [file_id]
                    item["done"] = True
                    completed += 1
                # File not yet visible in Drive — retry on next pass

            elif status["state"] in ["FAILED", "CANCELLED"]:
                state = status["state"]
                print(
                    f"Task {state.lower()}: {item['label']} — {status.get('error_message', '')}",
                    flush=True,
                )
                item["done"]      = True
                item["failed"]    = True
                item["cancelled"] = (state == "CANCELLED")
                completed += 1

        if completed < len(task_list):
            time.sleep(30)

    # Delete all downloaded files from Drive — errors here must never abort the pipeline
    ids_to_delete = [
        fid
        for item in task_list
        for fid in item.get("drive_file_ids", [])
    ]
    if ids_to_delete:
        print(f"Cleaning up {len(ids_to_delete)} file(s) from Google Drive...", flush=True)
        try:
            delete_drive_files(token_path, ids_to_delete)
        except Exception as e:
            print(f"Warning: Drive cleanup failed (files may remain): {e}", flush=True)

    # Raise appropriate error if any tasks did not complete successfully
    cancelled = [item["label"] for item in task_list if item.get("cancelled")]
    failed    = [item["label"] for item in task_list if item.get("failed") and not item.get("cancelled")]

    if cancelled:
        raise CancelledError(f"GEE task(s) cancelled by user: {', '.join(cancelled)}")
    if failed:
        raise RuntimeError(f"GEE task(s) failed: {', '.join(failed)}")


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

    import re as _re

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
                # Colons in timestamps cause [Errno 22] on Windows via the GEE API
                safe_desc = _re.sub(r'[^A-Za-z0-9_\-]', '_', file_prefix)[:100]
                task = ee.batch.Export.image.toDrive(
                    image=band_image,
                    description=safe_desc,
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
# RESUME DOWNLOAD
# ============================================================

def resume_download(export_names, reference_date, token_path, output_root,
                    timestamp, task_name=""):
    """
    Resume a failed lakedetection download without re-running GEE tasks.

    Use when GEE exports completed and files are still in Drive but the
    pipeline crashed before finishing download/COG/cluster steps.
    Files already present locally are skipped automatically.

    Parameters
    ----------
    export_names   : list[str]  — e.g. ['potential_water', 'z_score']
    reference_date : datetime.datetime
    token_path     : str
    output_root    : str
    timestamp      : str        — YYYYMMDD_HHMM from the failed run
    task_name      : str, optional

    Returns
    -------
    str  — path to the local output directory
    """
    date_str    = reference_date.strftime("%Y-%m-%d")
    name_suffix = f"_{task_name}" if task_name else ""
    local_dir   = os.path.join(output_root, f"Outputs_{date_str}{name_suffix}")
    os.makedirs(local_dir, exist_ok=True)

    drive_service = build_drive_service(token_path)

    for name in export_names:
        file_prefix = f"{name}_{timestamp}"
        local_path  = os.path.join(local_dir, f"{file_prefix}.tif")

        if os.path.exists(local_path):
            print(f"Already exists, skipping: {os.path.basename(local_path)}", flush=True)
            continue

        print(f"Resuming download: {file_prefix}", flush=True)
        res = drive_service.files().list(
            q=f"name='{file_prefix}.tif' and trashed=false",
            fields="files(id)",
        ).execute()
        files = res.get("files", [])
        if not files:
            print(f"Warning: no Drive file found for {file_prefix} — may have been deleted.", flush=True)
            continue
        file_id = files[0]["id"]
        request = drive_service.files().get_media(fileId=file_id)
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        print(f"Downloaded: {file_prefix}.tif", flush=True)
        delete_drive_files(token_path, [file_id])

    return local_dir


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
