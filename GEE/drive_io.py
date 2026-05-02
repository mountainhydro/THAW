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
import socket
import datetime
import ee
import rasterio
from googleapiclient.http import MediaIoBaseDownload
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from gee_auth import build_drive_service


class CancelledError(RuntimeError):
    """Raised when GEE tasks are cancelled by the user."""
    pass


def _is_valid_tif(path):
    """Return True only if path exists, is non-trivially sized, and rasterio can open it."""
    if not os.path.exists(path):
        return False
    if os.path.getsize(path) < 1024:
        return False
    try:
        with rasterio.open(path):
            pass
        return True
    except Exception:
        return False


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
                        new_id = chosen["id"].strip().rstrip("-")
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
                    continue

                if files:
                    stall_counts[idx] = 0
                    exact = [f for f in files if f["name"] == f"{item['file_prefix']}.tif"]
                    chosen = exact[0] if exact else files[0]
                    file_id = chosen["id"].strip().rstrip("-")
                    if not file_id:
                        item["done"] = True
                        completed += 1
                        continue
                    _download_file_with_retry(drive_service, file_id, item["local_path"],
                                              file_prefix=item["file_prefix"])
                    if not _is_valid_tif(item["local_path"]):
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
                    item["drive_file_ids"] = [file_id]
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

    # Return IDs for caller to delete — deletion is best-effort outside retry scope
    return [fid for item in task_list for fid in item.get("drive_file_ids", [])]


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
    drive_available = []  # (file_prefix, local_path, file_id)
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
                exact = [f for f in files if f["name"] == f"{file_prefix}.tif"]
                chosen = exact[0] if exact else files[0]
                file_id = chosen["id"].strip().rstrip("-")
                if file_id:
                    try:
                        drive_service.files().get(fileId=file_id, fields="id", supportsAllDrives=True).execute()
                        drive_available.append((file_prefix, local_path, file_id))
                        continue
                    except Exception:
                        pass
        except Exception:
            pass
        need_gee[name] = (img, file_prefix, local_path)

    if drive_available:
        print(f"Found {len(drive_available)} file(s) already in Drive — downloading directly.", flush=True)
        ids_downloaded = []
        for file_prefix, local_path, file_id in drive_available:
            ok = _download_file_with_retry(drive_service, file_id, local_path,
                                           file_prefix=file_prefix)
            if ok:
                ids_downloaded.append(file_id)
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
            "drive_file_ids": [],
            "done":           False,
        })
        print(f"Started GEE task: {name}", flush=True)

    ids_to_delete = []
    try:
        ids_to_delete = _poll_and_download(task_list, drive_service, token_path)
    finally:
        if ids_to_delete:
            print(f"Cleaning up {len(ids_to_delete)} file(s) from Google Drive...", flush=True)
            try:
                delete_drive_files(token_path, ids_to_delete)
            except Exception as e:
                print(f"Warning: Drive cleanup failed (files may remain): {e}", flush=True)

    return local_dir


# ============================================================
# TRACKING EXPORT
# ============================================================

def export_images_via_drive(s1_collection, aoi_ee, token_path,
                            bands_to_export=None, output_dir="outputs",
                            prefix="tracking", scale=10, drive_folder="GEE_Exports"):
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
    """
    if bands_to_export is None:
        bands_to_export = ["VV_raw", "VV_corrected", "VV_smoothed"]

    os.makedirs(output_dir, exist_ok=True)
    drive_service = build_drive_service(token_path)

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
    drive_available = []   # (local_path, file_prefix, file_id) — found in Drive
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
                    exact = [f for f in files if f["name"] == f"{file_prefix}.tif"]
                    chosen = exact[0] if exact else files[0]
                    file_id = chosen["id"].strip().rstrip("-")
                    if file_id:
                        try:
                            drive_service.files().get(fileId=file_id, fields="id", supportsAllDrives=True).execute()
                            drive_available.append((local_path, file_prefix, file_id))
                            continue
                        except Exception:
                            pass
            except Exception:
                pass
            need_gee.append((local_path, file_prefix, i, band, img, img_date))

        if drive_available:
            print(f"Found {len(drive_available)} file(s) already in Drive — downloading directly.", flush=True)
            dl_count = 0
            ids_downloaded = []
            for local_path, file_prefix, file_id in drive_available:
                ok = _download_file_with_retry(drive_service, file_id, local_path,
                                               file_prefix=file_prefix, max_attempts=8)
                if ok:
                    dl_count += 1
                    ids_downloaded.append(file_id)
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
            band_image = img.select(band).clip(aoi_ee)
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
                "drive_file_ids": [],
                "done":           False,
            })
        except Exception as e:
            print(f"Failed to start task for {file_prefix}: {e}", flush=True)

    if not task_list:
        print("No tasks to run (all files already exist or none launched).", flush=True)
        return

    os_error_occurred = False
    ids_to_delete = []
    try:
        ids_to_delete = _poll_and_download(task_list, drive_service, token_path)
    except (OSError, IOError) as e:
        all_downloaded = all(_is_valid_tif(item["local_path"]) for item in task_list)
        if all_downloaded:
            print(f"Warning: Drive cleanup error ignored (all files downloaded): {e}", flush=True)
            os_error_occurred = True
        else:
            raise
    finally:
        if ids_to_delete and not os_error_occurred:
            print(f"Cleaning up {len(ids_to_delete)} file(s) from Google Drive...", flush=True)
            try:
                delete_drive_files(token_path, ids_to_delete)
            except Exception as e:
                print(f"Warning: Drive cleanup failed (files may remain): {e}", flush=True)


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
