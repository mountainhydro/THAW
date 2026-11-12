# -*- coding: utf-8 -*-
"""
THAW - Google Drive I/O module

Handles all GEE export → Drive → local download operations, direct
(task-free) downloads for small tracking AOIs, COG conversion, and Drive cleanup.

GEE Processing code: Dr. Evan Miles
Tool/Operationalization: Dr. Stefan Fugger

Created on Feb 2 2026
"""

import os
import io
import sys
import glob
import time
import socket
import datetime
import ee
import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from googleapiclient.http import MediaIoBaseDownload
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from gee_auth import build_drive_service


class CancelledError(RuntimeError):
    """Raised when GEE tasks are cancelled by the user."""
    pass


def _is_valid_tif(path):
    """Return True only if path is a readable GeoTIFF."""
    if not os.path.isfile(path):
        return False
    try:
        with rasterio.open(path) as src:
            if src.driver != "GTiff" or src.count < 1:
                return False
            src.read(1, window=((0, 1), (0, 1)))
            src.read(1, window=((src.height - 1, src.height),
                                (src.width - 1, src.width)))
        return True
    except (OSError, rasterio.errors.RasterioError):
        return False


def _merge_tif_tiles(tile_paths, output_path):
    """
    Merge one or more GeoTIFF tiles into a single output file.
    Single tile: fast rename. Multiple tiles: rasterio.merge then delete temps.
    """
    import shutil
    if len(tile_paths) == 1:
        shutil.move(tile_paths[0], output_path)
        return
    from rasterio.merge import merge as rio_merge
    datasets = [rasterio.open(p) for p in tile_paths]
    try:
        mosaic, transform = rio_merge(datasets)
        profile = datasets[0].profile.copy()
        profile.update({
            "height":    mosaic.shape[1],
            "width":     mosaic.shape[2],
            "transform": transform,
        })
    finally:
        for ds in datasets:
            ds.close()
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(mosaic)
    for p in tile_paths:
        try:
            os.remove(p)
        except OSError:
            pass


def _download_tiles_and_merge(drive_service, drive_files, local_path, file_prefix):
    """
    Download all Drive tiles matching a file_prefix and merge into local_path.

    GEE splits large exports into tiles named like ``prefix-0000000000-0000023296.tif``.
    This function downloads every tile and merges them in filename order.

    Returns
    -------
    (success : bool, file_ids : list[str])
        success  — True if local_path is a valid TIF after the call.
        file_ids — IDs of every tile downloaded (for Drive cleanup).
    """
    tiles = sorted(drive_files, key=lambda f: f["name"])
    exact = [f for f in tiles if f["name"] == f"{file_prefix}.tif"]

    if exact:
        # Non-tiled: single file with the exact expected name
        file_id = exact[0]["id"].strip()
        ok = _download_file_with_retry(drive_service, file_id, local_path,
                                       file_prefix=file_prefix)
        return ok, [file_id]

    # Tiled output: download each tile to a temp path alongside local_path
    tmp_dir    = os.path.dirname(local_path)
    tile_paths = []
    tile_ids   = []
    for tf in tiles:
        tid = tf["id"].strip()
        if not tid:
            continue
        tmp_path = os.path.join(tmp_dir, f"_tile_{tf['name']}")
        ok = _download_file_with_retry(drive_service, tid, tmp_path,
                                       file_prefix=file_prefix)
        if ok:
            tile_paths.append(tmp_path)
            tile_ids.append(tid)

    if not tile_paths:
        return False, []

    try:
        _merge_tif_tiles(tile_paths, local_path)
    except Exception as merge_e:
        print(f"Warning: tile merge failed for {os.path.basename(local_path)}: {merge_e}", flush=True)
        for p in tile_paths:
            try:
                os.remove(p)
            except OSError:
                pass
        return False, tile_ids

    return True, tile_ids


def _download_file_with_retry(drive_service, file_id, local_path, max_attempts=8, base_wait=30, file_prefix=None):
    """Download a single Drive file with exponential-backoff retry.
    On 404, re-queries Drive for a fresh file ID using file_prefix.
    If re-query returns the same ID that just failed, gives up immediately
    (same ID = unrecoverable, not a transient error).
    Returns True on success, False on final failure."""
    last_404_id = None
    for attempt in range(1, max_attempts + 1):
        try:
            socket.setdefaulttimeout(300)  # 5-minute timeout per chunk — prevents silent hang
            request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
            with io.FileIO(local_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            return True
        except Exception as e:
            is_404 = "404" in str(e) or "notFound" in str(e)
            if is_404 and file_prefix:
                # Re-query Drive for a fresh file ID — old one may be deleted
                try:
                    safe_prefix = file_prefix.replace("'", "\\'")
                    res = drive_service.files().list(
                        q=f"name contains '{safe_prefix}' and trashed=false",
                        fields="files(id, name)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    ).execute()
                    fresh_files = res.get("files", [])
                    if fresh_files:
                        exact = [f for f in fresh_files if f["name"] == f"{file_prefix}.tif"]
                        chosen = exact[0] if exact else fresh_files[0]
                        new_id = chosen["id"].strip()
                        if new_id == last_404_id:
                            # Same dead ID returned again — unrecoverable, stop immediately
                            print(f"Warning: file persistently not accessible on Drive, skipping: {os.path.basename(local_path)}", flush=True)
                            return False
                        last_404_id = file_id
                        file_id = new_id
                        print(f"Re-queried Drive, got fresh file ID: {file_id}", flush=True)
                except Exception:
                    pass
            # Remove partial file before retrying so it won't be mistaken for a complete download
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
            if attempt == max_attempts:
                print(f"Warning: download failed after {max_attempts} attempts for {os.path.basename(local_path)}: {e}", flush=True)
                return False
            wait = base_wait * (2 ** (attempt - 1))
            print(f"Download attempt {attempt}/{max_attempts} failed: {e} — retrying in {wait}s...", flush=True)
            time.sleep(wait)


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
        self.log.write(message)   # log file first — must not be lost
        self.log.flush()
        try:
            self.terminal.write(message)
            self.terminal.flush()
        except Exception:
            pass  # broken or full pipe must never block or crash the pipeline

    def flush(self):
        self.log.flush()
        try:
            self.terminal.flush()
        except Exception:
            pass


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
    deleted = 0
    for fid in file_ids:
        try:
            drive.files().delete(fileId=fid).execute()
            deleted += 1
        except Exception as e:
            print(f"Warning: could not delete Drive file {fid} ({type(e).__name__}: {e})", flush=True)
    if deleted:
        print(f"Cleaned {deleted} file(s) from Google Drive.", flush=True)


def cleanup_stale_drive_exports(token_path, drive_folder="GEE_Exports", max_age_hours=12 * 24):
    """
    Delete any file older than max_age_hours still sitting in the Drive export
    folder. Safety net for files a run never got to clean up itself — e.g. a
    crashed/killed process, or a quota/permission error inside
    delete_drive_files — since such files no longer match any prefix a later
    run looks for (once already valid on disk, a prefix is never re-queried),
    and would otherwise sit in Drive forever.

    Silent by design (no print output) — this runs on every pipeline start and
    would otherwise clutter the run logs with unrelated cleanup noise.
    """
    try:
        drive = build_drive_service(token_path)
        folders = drive.files().list(
            q=f"name='{drive_folder}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute().get("files", [])
        cutoff = (datetime.datetime.now(datetime.timezone.utc)
                  - datetime.timedelta(hours=max_age_hours)).strftime("%Y-%m-%dT%H:%M:%S")
        stale_ids = []
        for folder in folders:
            page_token = None
            while True:
                res = drive.files().list(
                    q=f"'{folder['id']}' in parents and trashed=false and createdTime < '{cutoff}'",
                    fields="nextPageToken, files(id)", pageSize=1000, pageToken=page_token,
                    supportsAllDrives=True, includeItemsFromAllDrives=True,
                ).execute()
                stale_ids.extend(f["id"] for f in res.get("files", []))
                page_token = res.get("nextPageToken")
                if not page_token:
                    break
        for fid in stale_ids:
            try:
                drive.files().delete(fileId=fid).execute()
            except Exception:
                pass
    except Exception:
        pass


def _poll_and_download(task_list, drive_service, token_path):
    """
    Poll a list of GEE export tasks, downloading and immediately deleting
    each file's Drive copy as soon as it completes (rather than batching
    deletion until every task is done), so a crash partway through a large
    batch leaves at most the file in flight orphaned in Drive, not every
    file already finished.
    """
    print(f"Waiting for {len(task_list)} GEE task(s)...", flush=True)
    completed  = 0
    downloaded = 0
    stall_counts = {i: 0 for i in range(len(task_list))}

    while completed < len(task_list):
        # Rebuild drive service each pass to prevent stale connections
        try:
            drive_service = build_drive_service(token_path)
        except Exception as e:
            print(f"Warning: could not refresh Drive service: {e}", flush=True)

        pass_states = {}  # cache states from this pass — no re-querying

        for idx, item in enumerate(task_list):
            if item["done"]:
                continue

            try:
                status = item["task"].status()
                state  = status.get("state", "UNKNOWN")
            except Exception as e:
                print(f"Warning: could not get task status for {item['label']}: {e}", flush=True)
                state = "UNKNOWN"
                status = {}

            pass_states[idx] = state

            if state == "COMPLETED":
                try:
                    safe_prefix = item['file_prefix'].replace("'", "\\'")
                    res = drive_service.files().list(
                        q=f"name contains '{safe_prefix}' and trashed=false",
                        fields="files(id, name)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    ).execute()
                    files = res.get("files", [])
                except Exception as e:
                    print(f"Warning: Drive query failed for {item['label']}: {e}", flush=True)
                    stall_counts[idx] += 1
                    if stall_counts[idx] >= 10:
                        print(f"Warning: Drive query kept failing for {item['label']}, giving up.", flush=True)
                        item["done"]   = True
                        item["failed"] = True
                        completed += 1
                    continue

                if files:
                    stall_counts[idx] = 0
                    ok, tile_ids = _download_tiles_and_merge(
                        drive_service, files, item["local_path"],
                        file_prefix=item["file_prefix"])
                    if not ok or not _is_valid_tif(item["local_path"]):
                        print(f"Warning: downloaded file is corrupt or unreadable, skipping: {os.path.basename(item['local_path'])}", flush=True)
                        try:
                            os.remove(item["local_path"])
                        except OSError:
                            pass
                        item["done"] = True
                        completed += 1
                        continue
                    downloaded += 1
                    print(f"Downloaded files: ({downloaded}/{len(task_list)})", flush=True)
                    delete_drive_files(token_path, tile_ids)
                    item["done"] = True
                    completed += 1
                    pass_states[idx] = "DOWNLOADED"
                else:
                    stall_counts[idx] += 1
                    if stall_counts[idx] >= 10:
                        print(f"Warning: file never appeared in Drive for {item['label']}, skipping.", flush=True)
                        item["done"]   = True
                        item["failed"] = True
                        completed += 1

            elif state in ["FAILED", "CANCELLED"]:
                print(
                    f"Task {state.lower()}: {item['label']} — {status.get('error_message', '')}",
                    flush=True,
                )
                item["done"]      = True
                item["failed"]    = True
                item["cancelled"] = (state == "CANCELLED")
                completed += 1

        if completed < len(task_list):
            # Use cached states — no extra API calls
            pending_states = [pass_states.get(i, "UNKNOWN")
                              for i, item in enumerate(task_list) if not item["done"]]
            state_counts = {}
            for s in pending_states:
                label = "processing" if s == "RUNNING" else "queued" if s == "READY" else s.lower()
                state_counts[label] = state_counts.get(label, 0) + 1
            state_str = ", ".join(f"{v} {k}" for k, v in state_counts.items())
            print(f"Waiting... {len(pending_states)} file(s) remaining ({state_str}).", flush=True)
            time.sleep(30)

    # Raise if any tasks failed
    cancelled = [item["label"] for item in task_list if item.get("cancelled")]
    failed    = [item["label"] for item in task_list if item.get("failed") and not item.get("cancelled")]

    if cancelled:
        raise CancelledError(f"GEE task(s) cancelled by user: {', '.join(cancelled)}")
    if failed:
        raise RuntimeError(f"GEE task(s) failed: {', '.join(failed)}")


def export_and_download(images_to_export, reference_date, aoi, token_path,
                        output_root, run_label, task_name=""):
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
    run_label : str
        Label used in filenames, e.g. '2026-05-01_001'.
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
    cleanup_stale_drive_exports(token_path)

    # Separate files already valid on disk from those needing download
    missing = {}
    for name, img in images_to_export.items():
        file_prefix = f"{name}_{run_label}"
        local_path  = os.path.join(local_dir, f"{file_prefix}.tif")
        if _is_valid_tif(local_path):
            print(f"Already exists locally, skipping: {file_prefix}.tif", flush=True)
        else:
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
            missing[name] = (img, file_prefix, local_path)

    if not missing:
        return local_dir

    # Check Drive for files already exported (avoids re-submitting GEE tasks on retry)
    drive_available = []  # (file_prefix, local_path, drive_files_list)
    need_gee = {}         # name → (img, file_prefix, local_path)

    print(f"Checking Google Drive for {len(missing)} missing file(s)...", flush=True)
    for name, (img, file_prefix, local_path) in missing.items():
        try:
            safe_prefix = file_prefix.replace("'", "\\'")
            res = drive_service.files().list(
                q=f"name contains '{safe_prefix}' and trashed=false",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
            files = res.get("files", [])
            if files:
                drive_available.append((file_prefix, local_path, files))
                continue
        except Exception:
            pass
        need_gee[name] = (img, file_prefix, local_path)

    if drive_available:
        print(f"Found {len(drive_available)} file(s) already in Drive — downloading directly.", flush=True)
        ids_downloaded = []
        for file_prefix, local_path, drive_files in drive_available:
            ok, tile_ids = _download_tiles_and_merge(drive_service, drive_files, local_path,
                                                     file_prefix=file_prefix)
            if ok:
                ids_downloaded.extend(tile_ids)
        if ids_downloaded:
            delete_drive_files(token_path, ids_downloaded)

    if not need_gee:
        return local_dir

    # Submit GEE tasks only for files not found anywhere
    task_list = []
    for name, (img, file_prefix, local_path) in need_gee.items():
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
            "local_path":     local_path,
            "label":          name,
            "done":           False,
        })
        print(f"Started GEE task: {name}", flush=True)

    print("To monitor your task on the GEE server, visit code.earthengine.google.com/tasks", flush=True)

    _poll_and_download(task_list, drive_service, token_path)

    return local_dir


# ============================================================
# TRACKING EXPORT
# ============================================================

def export_images_via_drive(s1_collection, aoi_ee, token_path,
                            bands_to_export=None, output_dir="outputs",
                            prefix="tracking", scale=10, drive_folder="GEE_Exports",
                            band_groups=None):
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
        Filename prefix including task name and submission date,
        e.g. 'Khumbu_20260502'.
    scale : int, optional
        Export resolution in metres (default 10).
    drive_folder : str, optional
        GEE Drive export folder name (default 'GEE_Exports').
    band_groups : dict[str, list[str]], optional
        Maps a logical band name in bands_to_export to multiple underlying
        ee.Image band names selected and exported together as one multi-band
        GeoTIFF (e.g. {'true_color': ['true_color_R', 'true_color_G', 'true_color_B']}).
        Band names not present default to selecting themselves (single-band export).
    """
    if bands_to_export is None:
        bands_to_export = ["VV_raw", "VV_corrected", "VV_smoothed"]
    band_groups = band_groups or {}

    os.makedirs(output_dir, exist_ok=True)
    drive_service = build_drive_service(token_path)
    cleanup_stale_drive_exports(token_path, drive_folder=drive_folder)

    count = s1_collection.size().getInfo()
    s1_list = s1_collection.toList(count)

    import re as _re

    # Build expected file list: get image date from system:time_start
    expected = []
    for i in range(count):
        img = ee.Image(s1_list.get(i))
        try:
            img_time = img.get('system:time_start').getInfo()
            img_date = datetime.datetime.fromtimestamp(img_time / 1000, tz=datetime.timezone.utc).strftime('%Y-%m-%d')
        except Exception:
            img_date = f"img{i:03d}"
        for band in bands_to_export:
            local_filename = f"{prefix}_{img_date}_{band}.tif"
            local_path     = os.path.join(output_dir, local_filename)
            file_prefix    = local_filename[:-4]
            expected.append((local_path, file_prefix, i, band, img, img_date))

    # Validity check — delete and re-download invalid files
    for local_path, *_ in expected:
        if os.path.exists(local_path) and not _is_valid_tif(local_path):
            print(f"Invalid file detected, removing: {os.path.basename(local_path)}", flush=True)
            try:
                os.remove(local_path)
            except OSError:
                pass

    missing_locally   = [(lp, fp, i, b, im, id_) for lp, fp, i, b, im, id_ in expected if not _is_valid_tif(lp)]
    already_local     = len(expected) - len(missing_locally)
    if already_local:
        print(f"{already_local} file(s) already on disk, skipping.", flush=True)

    # Check Drive for the locally-missing files
    drive_available = []   # (local_path, file_prefix, drive_files_list) — found in Drive
    need_gee        = []   # (local_path, file_prefix, i, band, img, img_date) — need GEE export

    if missing_locally:
        print(f"Checking Google Drive for {len(missing_locally)} missing file(s)...", flush=True)
        for local_path, file_prefix, i, band, img, img_date in missing_locally:
            try:
                safe_prefix = file_prefix.replace("'", "\\'")
                res = drive_service.files().list(
                    q=f"name contains '{safe_prefix}' and trashed=false",
                    fields="files(id, name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                ).execute()
                files = res.get("files", [])
                if files:
                    drive_available.append((local_path, file_prefix, files))
                    continue
            except Exception:
                pass
            need_gee.append((local_path, file_prefix, i, band, img, img_date))

        if drive_available:
            print(f"Found {len(drive_available)} file(s) already in Drive — downloading directly.", flush=True)
            dl_count = 0
            ids_downloaded = []
            for local_path, file_prefix, drive_files in drive_available:
                ok, tile_ids = _download_tiles_and_merge(drive_service, drive_files, local_path,
                                                         file_prefix=file_prefix)
                if ok:
                    dl_count += 1
                    ids_downloaded.extend(tile_ids)
            print(f"Downloaded {dl_count}/{len(drive_available)} file(s) from Drive.", flush=True)
            if ids_downloaded:
                delete_drive_files(token_path, ids_downloaded)

        if not need_gee:
            print("All files accounted for — no GEE tasks needed.", flush=True)
            return

        print(f"Submitting {len(need_gee)} GEE task(s) for remaining files...", flush=True)

    task_list = []
    items_to_submit = need_gee if missing_locally else \
        [(lp, fp, i, b, im, id_) for lp, fp, i, b, im, id_ in expected if not _is_valid_tif(lp)]
    for local_path, file_prefix, i, band, img, img_date in items_to_submit:
        try:
            select_bands = band_groups.get(band, [band])
            band_image = img.select(select_bands).clip(aoi_ee)
            safe_desc  = _re.sub(r'[^A-Za-z0-9_\-]', '_', file_prefix)[:100]
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
                "label":          os.path.basename(local_path),
                "done":           False,
            })
        except Exception as e:
            print(f"Failed to start task for {file_prefix}: {e}", flush=True)

    if not task_list:
        print("No tasks to run (all files already exist or none launched).", flush=True)
        return

    try:
        _poll_and_download(task_list, drive_service, token_path)
    except (OSError, IOError) as e:
        all_downloaded = all(_is_valid_tif(item["local_path"]) for item in task_list)
        if all_downloaded:
            print(f"Warning: Drive cleanup error ignored (all files downloaded): {e}", flush=True)
        else:
            raise


def download_images_direct(s1_collection, aoi_ee, token_path,
                           bands_to_export, output_dir="outputs",
                           prefix="tracking", scale=10, crs=None,
                           band_groups=None, max_workers=6,
                           max_attempts=3, base_wait=10, drive_fallback=True):
    """
    Download each image × band straight from GEE via getDownloadURL, skipping
    the Drive export-task queue. Suited to the small tracking AOIs (a request
    is limited to ~32 MB and ~5 min of server-side compute). Files that still
    fail after retries are handed to export_images_via_drive, which skips the
    files already on disk.

    Parameters match export_images_via_drive, plus:
    crs : str, optional
        Output CRS (e.g. 'EPSG:32643'); should be set explicitly since, unlike
        Drive exports, direct downloads otherwise default to EPSG:4326.
    max_workers : int
        Number of parallel download requests.
    max_attempts, base_wait :
        Per-file retries with linear backoff (base_wait × attempt seconds).
    drive_fallback : bool
        Export files that failed every direct attempt via Drive instead.
    """
    import requests
    import threading
    from concurrent.futures import ThreadPoolExecutor

    if max_workers < 1 or max_attempts < 1:
        raise ValueError("max_workers and max_attempts must be at least 1")

    band_groups = band_groups or {}
    os.makedirs(output_dir, exist_ok=True)

    count = s1_collection.size().getInfo()
    dates = s1_collection.aggregate_array('system:time_start').getInfo()
    if len(dates) != count:
        raise ValueError(f"Expected {count} frame timestamps, received {len(dates)}")
    s1_list = s1_collection.toList(count)
    region = aoi_ee.bounds()

    jobs = []
    for i, img_time in enumerate(dates):
        img_date = datetime.datetime.fromtimestamp(
            img_time / 1000, tz=datetime.timezone.utc).strftime('%Y-%m-%d')
        img = ee.Image(s1_list.get(i))
        for band in bands_to_export:
            local_path = os.path.join(output_dir, f"{prefix}_{img_date}_{band}.tif")
            if _is_valid_tif(local_path):
                continue
            jobs.append((i, img, band, local_path))

    total = len(jobs)
    already = count * len(bands_to_export) - total
    if already:
        print(f"{already} file(s) already on disk, skipping.", flush=True)
    if not jobs:
        print("All files accounted for — nothing to download.", flush=True)
        return

    print(f"Downloading {total} file(s) directly from GEE "
          f"({max_workers} in parallel, crs={crs or 'default'})...", flush=True)
    lock = threading.Lock()
    done_count = [0]
    failed = []

    def _fetch(job):
        index, img, band, local_path = job
        select_bands = band_groups.get(band, [band])
        band_image = img.select(select_bands).clip(aoi_ee)
        if band not in band_groups:
            band_image = band_image.toFloat()
        params = {"region": region, "scale": scale, "format": "GEO_TIFF"}
        if crs:
            params["crs"] = crs
        tmp_path = f"{local_path}.{index}.part"
        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                url = band_image.getDownloadURL(params)
                resp = requests.get(url, timeout=600)
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                with open(tmp_path, "wb") as f:
                    f.write(resp.content)
                if not _is_valid_tif(tmp_path):
                    raise RuntimeError("downloaded file is not a valid GeoTIFF")
                os.replace(tmp_path, local_path)
                with lock:
                    done_count[0] += 1
                    print(f"Downloaded files: ({done_count[0]}/{total}) "
                          f"{os.path.basename(local_path)}", flush=True)
                return
            except Exception as e:
                last_err = e
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError as cleanup_error:
                        print(f"Warning: could not remove {tmp_path}: {cleanup_error}",
                              flush=True)
                if attempt < max_attempts:
                    print(f"Direct download failed for {os.path.basename(local_path)} "
                          f"(attempt {attempt}/{max_attempts}): {e}. "
                          f"Retrying in {base_wait * attempt}s.", flush=True)
                    time.sleep(base_wait * attempt)
        with lock:
            failed.append(local_path)
            print(f"Direct download failed for {os.path.basename(local_path)} "
                  f"after {max_attempts} attempt(s): {last_err}", flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(_fetch, jobs))

    if not failed:
        return
    if not drive_fallback:
        raise RuntimeError(f"{len(failed)} file(s) could not be downloaded directly.")
    print(f"Falling back to Drive export for {len(failed)} file(s)...", flush=True)
    export_images_via_drive(
        s1_collection, aoi_ee, token_path,
        bands_to_export=bands_to_export, output_dir=output_dir,
        prefix=prefix, scale=scale, band_groups=band_groups,
    )
    still_missing = [path for path in failed if not _is_valid_tif(path)]
    if still_missing:
        names = ", ".join(os.path.basename(path) for path in still_missing)
        raise RuntimeError(f"Drive fallback did not produce valid GeoTIFFs: {names}")


# ============================================================
# COG CONVERSION
# ============================================================

def compute_snow_filtered_zscore(local_dir, run_label):
    """
    Build the snow-filtered z-score raster locally instead of on GEE.

    Combines the already-downloaded 'z_score' and 'snow_mask' GeoTIFFs
    (exporting a binary mask from GEE is far cheaper than exporting a whole
    second z-score raster). The mask is resampled onto the z_score grid
    (nearest-neighbour) if the two exports don't already share one, since
    each is reprojected independently by GEE. Snow-flagged pixels are set to
    NaN, matching how cluster_processing() already treats invalid pixels.

    Parameters
    ----------
    local_dir : str — output folder containing the downloaded GeoTIFFs
    run_label : str — run label used in filenames, e.g. '2026-05-01_39084_71499_001'

    Returns
    -------
    str or None — path to the written 'zscore_snowfilter_<run_label>.tif', or
    None if either input file is missing (already logged as a warning).
    """
    z_path    = os.path.join(local_dir, f"z_score_{run_label}.tif")
    mask_path = os.path.join(local_dir, f"snow_mask_{run_label}.tif")
    out_path  = os.path.join(local_dir, f"zscore_snowfilter_{run_label}.tif")

    if _is_valid_tif(out_path):
        return out_path
    if not (_is_valid_tif(z_path) and _is_valid_tif(mask_path)):
        print("Skipping local snow-filter: z_score or snow_mask file missing.", flush=True)
        return None

    with rasterio.open(z_path) as z_src:
        z_data    = z_src.read(1).astype("float32")
        z_profile = z_src.profile.copy()

    with rasterio.open(mask_path) as m_src:
        same_grid = (
            m_src.transform == z_profile["transform"] and
            m_src.width == z_profile["width"] and
            m_src.height == z_profile["height"] and
            m_src.crs == z_profile["crs"]
        )
        if same_grid:
            mask_data = m_src.read(1)
        else:
            mask_data = np.zeros((z_profile["height"], z_profile["width"]), dtype=m_src.dtypes[0])
            reproject(
                source=rasterio.band(m_src, 1),
                destination=mask_data,
                dst_transform=z_profile["transform"],
                dst_crs=z_profile["crs"],
                resampling=Resampling.nearest,
            )

    out_data = np.where(mask_data.astype(bool), np.nan, z_data)
    z_profile.update(dtype="float32", nodata=np.nan)

    with rasterio.open(out_path, "w", **z_profile) as dst:
        dst.write(out_data, 1)

    print(f"Computed local snow-filtered z-score: {os.path.basename(out_path)}", flush=True)
    return out_path


def compute_snow_filtered_likelihood(output_dir):
    """
    Build snow-filtered lake-likelihood rasters for every downloaded tracking
    frame. Directory-wide equivalent of compute_snow_filtered_zscore() — the
    tracking pipeline has one frame per date/period instead of a single
    run_label, so each '<prefix>_<date>_snow_mask.tif' is paired with its
    matching '<prefix>_<date>_lake_likelihood.tif' by filename.

    Writes '<prefix>_<date>_likelihood_snowfilter.tif' (snow-flagged pixels
    set to NaN) next to the inputs. Missing or unreadable inputs raise an error
    unless the filtered output already exists and is valid.

    Parameters
    ----------
    output_dir : str — tracking results folder containing the downloaded GeoTIFFs

    Returns
    -------
    int — number of snow-filtered rasters written (including ones already present)
    """
    likelihood_files = sorted(glob.glob(os.path.join(output_dir, "*_lake_likelihood.tif")))
    if not likelihood_files:
        raise ValueError(f"No lake likelihood rasters found in {output_dir}")

    written = 0
    for likelihood_path in likelihood_files:
        mask_path = likelihood_path.replace("_lake_likelihood.tif", "_snow_mask.tif")
        out_path = likelihood_path.replace("_lake_likelihood.tif", "_likelihood_snowfilter.tif")
        if _is_valid_tif(out_path):
            written += 1
            continue
        if not (_is_valid_tif(likelihood_path) and _is_valid_tif(mask_path)):
            raise RuntimeError(
                f"Cannot snow-filter {os.path.basename(likelihood_path)}: "
                f"likelihood or snow mask is missing or unreadable ({os.path.basename(mask_path)})"
            )

        with rasterio.open(likelihood_path) as l_src:
            l_data    = l_src.read(1, masked=True).astype("float32").filled(np.nan)
            l_profile = l_src.profile.copy()

        with rasterio.open(mask_path) as m_src:
            source_mask = m_src.read(1, masked=True).filled(0)
            same_grid = (
                m_src.transform == l_profile["transform"] and
                m_src.width == l_profile["width"] and
                m_src.height == l_profile["height"] and
                m_src.crs == l_profile["crs"]
            )
            if same_grid:
                mask_data = source_mask
            else:
                mask_data = np.zeros((l_profile["height"], l_profile["width"]), dtype=m_src.dtypes[0])
                reproject(
                    source=source_mask,
                    destination=mask_data,
                    src_transform=m_src.transform,
                    src_crs=m_src.crs,
                    dst_transform=l_profile["transform"],
                    dst_crs=l_profile["crs"],
                    resampling=Resampling.nearest,
                )

        out_data = np.where(mask_data > 0, np.nan, l_data)
        l_profile.update(dtype="float32", nodata=np.nan)

        with rasterio.open(out_path, "w", **l_profile) as dst:
            dst.write(out_data, 1)
        written += 1

    print(f"Computed {written} snow-filtered likelihood raster(s).", flush=True)
    return written


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
