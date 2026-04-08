# -*- coding: utf-8 -*-
"""
THAW - Tracking Results Viewer

"""

import os
import re
import glob
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from PIL import Image, ImageOps, ImageDraw, ImageFont
from io import BytesIO
import streamlit as st

# ── helpers ────────────────────────────────────────────────────────────────

PANEL_CFG = {
    "VV_raw":          dict(label="Raw VV (dB)",       cmap=plt.cm.gray,   vmin=-25, vmax=0,  nan_fill=0.5),
    "VV_corrected":    dict(label="Corrected VV (dB)", cmap=plt.cm.gray,   vmin=-25, vmax=0,  nan_fill=0.5),
    "lake_likelihood": dict(label="Lake Likelihood",   cmap=plt.cm.viridis, vmin=0,  vmax=1,  nan_fill=0.0),
}

def _read_masked(path):
    with rasterio.open(path) as src:
        data = src.read(1).astype(float)
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
    data[data <= -9999] = np.nan
    return data

def _render_to_pil(data, cmap, vmin, vmax, nan_fill):
    norm = np.clip((data - vmin) / (vmax - vmin), 0, 1)
    norm = np.where(np.isnan(norm), nan_fill, norm)
    rgb  = (cmap(norm)[:, :, :3] * 255).astype(np.uint8)
    return Image.fromarray(rgb)

def _pil_to_bytes(im):
    buf = BytesIO()
    im.save(buf, format="PNG")
    buf.seek(0)
    return buf

def _extract_date(filename):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if m:
        return m.group(1)
    m = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return os.path.basename(filename)

def _discover_frames(tracking_dir):
    """
    Returns a list of dicts, one per timestep, sorted by date:
        [{"date": "2025-06-09", "VV_raw": path, "VV_corrected": path, "lake_likelihood": path}, ...]
    Only includes timesteps where ALL three bands are present.
    """
    frames = {}
    for band in PANEL_CFG:
        for path in sorted(glob.glob(os.path.join(tracking_dir, f"*{band}*.tif"))):
            date = _extract_date(os.path.basename(path))
            frames.setdefault(date, {})[band] = path

    # Keep only complete frames
    complete = {
        date: bands
        for date, bands in frames.items()
        if all(b in bands for b in PANEL_CFG)
    }
    return [{"date": d, **complete[d]} for d in sorted(complete)]


# ── viewer section (paste into Output_Preview.py) ──────────────────────────

def render_tracking_viewer(folder_path):
    """
    Call this function at the bottom of Output_Preview.py, passing the
    already-resolved folder_path variable (the dated Outputs_* folder).

    Example (in Output_Preview.py):
        from tracking_viewer import render_tracking_viewer
        render_tracking_viewer(folder_path)
    """
    tracking_dir = os.path.join(folder_path, "tracking_results")

    st.write("---")
    st.subheader("Tracking Results Viewer")

    # Constrain all widgets in this viewer to match TOTAL_WIDTH
    TOTAL_WIDTH = 900
    st.markdown(
        f"""<style>
        /* Scope to tracking viewer slider and captions */
        div[data-testid="stSlider"], div[data-testid="stCaptionContainer"] {{
            max-width: {TOTAL_WIDTH}px !important;
        }}
        </style>""",
        unsafe_allow_html=True,
    )

    if not os.path.isdir(tracking_dir):
        st.info("No tracking results found for this date. Run a tracking analysis first.")
        return

    frames = _discover_frames(tracking_dir)

    if not frames:
        st.warning("No time tracking results available. Please draw AOI and launch a tracking analysis.")
        return

    dates = [f["date"] for f in frames]

    # st.select_slider crashes when only one option exists (internal range [0,0])
    if len(dates) == 1:
        selected_date = dates[0]
        st.caption(f"Single date available: {selected_date}")
        idx = 0
    else:
        selected_date = st.select_slider(
            "Date",
            options=dates,
            value=dates[0],
            key="tracking_slider",
        )
        idx = dates.index(selected_date)
        st.caption(f"Image {idx + 1} of {len(frames)}")

    frame = frames[idx]

    panel_w   = TOTAL_WIDTH // 3
    CAPTION_H = 22

    # ── render three panels + baked captions as one image ──
    panels, captions = [], []
    for band in PANEL_CFG:
        cfg = PANEL_CFG[band]
        try:
            data = _read_masked(frame[band])
            im   = _render_to_pil(data, cfg["cmap"], cfg["vmin"], cfg["vmax"], cfg["nan_fill"])
            ratio = panel_w / im.width
            im = im.resize((panel_w, max(1, int(im.height * ratio))), Image.LANCZOS)
            panels.append(im)
        except Exception:
            panels.append(Image.new("RGB", (panel_w, panel_w), (80, 80, 80)))
        captions.append(cfg["label"])

    img_h    = max(p.height for p in panels)
    combined = Image.new("RGB", (TOTAL_WIDTH, img_h + CAPTION_H), (255, 255, 255))
    x = 0
    for p in panels:
        combined.paste(p, (x, 0))
        x += p.width

    draw = ImageDraw.Draw(combined)
    try:
        font = ImageFont.truetype("arial.ttf", 12)
    except Exception:
        font = ImageFont.load_default()
    for i, caption in enumerate(captions):
        cx = i * panel_w + panel_w // 2
        draw.text((cx, img_h + 4), caption, fill=(80, 80, 80), font=font, anchor="mt")

    buf = BytesIO()
    combined.save(buf, format="PNG")
    buf.seek(0)
    st.image(buf, width=TOTAL_WIDTH)

    # ── lake area chart at same width ──
    metrics_csv = os.path.join(tracking_dir, "lake_metrics.csv")
    if os.path.isfile(metrics_csv):
        try:
            import pandas as pd
            from datetime import datetime as _dt

            df = pd.read_csv(metrics_csv)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)

            dpi   = 100
            fig_w = TOTAL_WIDTH / dpi
            fig, ax = plt.subplots(figsize=(fig_w, fig_w * 0.56))
            fig.patch.set_facecolor("#ffffff")
            ax.set_facecolor("#ffffff")

            ax.fill_between(df["date"], df["lower_area_km2"], df["upper_area_km2"],
                            color="#4a90d9", alpha=0.25, label="Uncertainty band")
            ax.plot(df["date"], df["mean_area_km2"],
                    color="#4a90d9", linewidth=1.8, label="Mean area")
            ax.scatter(df["date"], df["mean_area_km2"], color="#4a90d9", s=22, zorder=5)

            try:
                indicator_dt = _dt.strptime(selected_date, "%Y-%m-%d")
                ax.axvline(indicator_dt, color="#FF6B00", linewidth=1.6,
                           linestyle="--", zorder=6, label=selected_date)
                nearest_idx = (df["date"] - pd.Timestamp(indicator_dt)).abs().idxmin()
                ax.scatter([df["date"][nearest_idx]], [df["mean_area_km2"][nearest_idx]],
                           color="#FF6B00", s=55, zorder=7)
            except Exception:
                pass

            ax.set_xlabel("Date", color="#333333", fontsize=9)
            ax.set_ylabel("Lake Area (km²)", color="#333333", fontsize=9)
            ax.tick_params(colors="#333333", labelsize=8)
            for spine in ax.spines.values():
                spine.set_edgecolor("#cccccc")
            ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y-%m-%d"))
            fig.autofmt_xdate(rotation=30, ha="right")
            ax.legend(fontsize=8, facecolor="#ffffff", edgecolor="#cccccc",
                      labelcolor="#333333", loc="upper left")
            fig.tight_layout()

            chart_buf = BytesIO()
            fig.savefig(chart_buf, format="PNG", dpi=dpi, bbox_inches="tight")
            plt.close(fig)
            chart_buf.seek(0)
            st.image(chart_buf, width=TOTAL_WIDTH)

        except Exception as e:
            st.warning(f"Could not load lake metrics: {e}")


# ── if running this file directly for testing ───────────────────────────────
if __name__ == "__main__":
    import sys
    test_folder = sys.argv[1] if len(sys.argv) > 1 else "."
    st.set_page_config(layout="wide", page_title="Tracking Viewer Test")
    render_tracking_viewer(test_folder)
