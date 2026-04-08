# -*- coding: utf-8 -*-
"""
THAW - Centralised authentication module

Handles OAuth token loading, GEE initialisation, and Google Drive service
construction. Both pipeline scripts (lakedetection_headless.py and
tracking_headless.py) import from here so auth logic is never duplicated.

The OAuth token (drive_token.json) is written once by the Dashboard login flow
and covers both the Earth Engine and Drive scopes.
"""

import os
import ee
from googleapiclient.discovery import build


SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/earthengine",
]


def load_credentials(token_path):
    """
    Load the saved OAuth token and refresh it if expired.

    Parameters
    ----------
    token_path : str
        Path to drive_token.json written by the Dashboard login flow.

    Returns
    -------
    google.oauth2.credentials.Credentials
        Valid, refreshed credentials covering Drive and Earth Engine scopes.
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"OAuth token not found at: {token_path}\n"
            f"Please open the THAW Dashboard and log in to generate it."
        )

    creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return creds


def initialize_ee(token_path, project_id):
    """
    Initialise Google Earth Engine using the saved OAuth token.

    Parameters
    ----------
    token_path : str
        Path to drive_token.json.
    project_id : str
        GEE cloud project ID.

    Returns
    -------
    google.oauth2.credentials.Credentials
        The loaded credentials, so the caller can reuse them for Drive without
        reading the token file a second time.
    """
    creds = load_credentials(token_path)
    ee.Initialize(credentials=creds, project=project_id)
    return creds


def build_drive_service(token_path):
    """
    Build an authenticated Google Drive API client using the saved OAuth token.

    Parameters
    ----------
    token_path : str
        Path to drive_token.json.

    Returns
    -------
    googleapiclient.discovery.Resource
        Authenticated Drive v3 service object.
    """
    creds = load_credentials(token_path)
    return build("drive", "v3", credentials=creds, cache_discovery=False, static_discovery=False)
