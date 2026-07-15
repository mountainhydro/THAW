# -*- coding: utf-8 -*-
"""
THAW - Shared partner-logo header

Pins the logos from docs/logos in the upper-right corner of every page.
"""

import os
import glob
import base64
import streamlit as st

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_CURRENT_DIR)
_LOGOS_DIR = os.path.join(_ROOT_DIR, "docs", "logos")

# Display order for known logos; any other logos found in docs/logos are
# appended afterwards, sorted alphabetically.
_LOGO_ORDER = ["UNESCO", "Adaptation_Fund", "SDC", "ETH_Zurich", "UZH"]


def render_logo_header(height_px=53):
    """Render the partner logos fixed to the upper-right corner of the page."""
    all_paths = glob.glob(os.path.join(_LOGOS_DIR, "*.png"))
    by_stem = {os.path.splitext(os.path.basename(p))[0]: p for p in all_paths}

    logo_paths = [by_stem.pop(name) for name in _LOGO_ORDER if name in by_stem]
    logo_paths += [by_stem[name] for name in sorted(by_stem)]

    if not logo_paths:
        return

    imgs_html = ""
    for path in logo_paths:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        imgs_html += f'<img src="data:image/png;base64,{b64}" style="height:{height_px}px;width:auto;">'

    st.markdown(
        f"""
        <style>
        .thaw-logo-header {{
            position: fixed;
            top: 3.6rem;
            right: 1rem;
            z-index: 999999;
            display: flex;
            flex-wrap: wrap;
            justify-content: flex-end;
            max-width: calc(100vw - 6rem);
            align-items: center;
            gap: 12px;
            background: rgba(255,255,255,0.85);
            padding: 4px 10px;
            border-radius: 6px;
        }}
        /* Reserve room below the logo bar so it never overlaps page content,
           even on narrow windows where the logos wrap onto a second line. */
        .block-container {{
            padding-top: 12rem;
        }}
        </style>
        <div class="thaw-logo-header">{imgs_html}</div>
        """,
        unsafe_allow_html=True,
    )
