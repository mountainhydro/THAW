# -*- coding: utf-8 -*-
"""
THAW - Local raster tile server

Serves COG raster tiles on demand (via rio-tiler) so folium/Leaflet can lazily
fetch only the tiles visible in the viewport, instead of embedding a full
pre-rendered PNG in the Streamlit websocket message (which hits Streamlit's
message size limit for large AOIs).
"""

import io
import os
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
from rio_tiler.io import Reader
from rio_tiler.errors import TileOutsideBounds

TILE_SIZE = 256


def _transparent_tile():
    buf = io.BytesIO()
    Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0)).save(buf, format="PNG")
    return buf.getvalue()


_TRANSPARENT_TILE_BYTES = _transparent_tile()


def _render_single_band(path, x, y, z, vmin, vmax, palette, mask_below_zero):
    try:
        with Reader(path) as reader:
            img = reader.tile(x, y, z, tilesize=TILE_SIZE)
    except TileOutsideBounds:
        return _TRANSPARENT_TILE_BYTES

    data = img.data[0].astype(np.float32)
    transparent = img.mask == 0
    transparent |= np.isnan(data)
    if mask_below_zero:
        transparent |= data < 0

    norm = np.clip((data - vmin) / (vmax - vmin), 0, 1)
    cmap = plt.get_cmap(palette)
    rgba = (cmap(norm) * 255).astype(np.uint8)
    rgba[transparent, 3] = 0

    buf = io.BytesIO()
    Image.fromarray(rgba, mode="RGBA").save(buf, format="PNG")
    return buf.getvalue()


def _render_rgb(path, x, y, z):
    try:
        with Reader(path) as reader:
            img = reader.tile(x, y, z, tilesize=TILE_SIZE, indexes=(1, 2, 3))
    except TileOutsideBounds:
        return _TRANSPARENT_TILE_BYTES

    rgb = np.moveaxis(img.data, 0, -1).astype(np.uint8)
    alpha = img.mask.astype(np.uint8)
    rgba = np.dstack([rgb, alpha])

    buf = io.BytesIO()
    Image.fromarray(rgba, mode="RGBA").save(buf, format="PNG")
    return buf.getvalue()


class _TileRequestHandler(BaseHTTPRequestHandler):
    # Restricts served files to this directory (set by start_tile_server); prevents
    # the local tile endpoint from being used to read arbitrary filesystem paths.
    allowed_root = None

    def log_message(self, fmt, *args):
        pass  # keep stdout/stderr quiet

    def _resolve_path(self, raw_path):
        path = os.path.realpath(raw_path)
        root = os.path.realpath(self.allowed_root) if self.allowed_root else None
        if root and os.path.commonpath([path, root]) != root:
            raise PermissionError("path outside allowed root")
        return path

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            qs = urllib.parse.parse_qs(parsed.query)
            if len(parts) != 5 or parts[0] != "tile":
                raise ValueError("bad tile path")

            kind = parts[1]
            z, x = int(parts[2]), int(parts[3])
            y = int(parts[4].split(".")[0])
            path = self._resolve_path(qs["path"][0])

            if kind == "rgb":
                png_bytes = _render_rgb(path, x, y, z)
            else:
                vmin = float(qs["vmin"][0])
                vmax = float(qs["vmax"][0])
                palette = qs["palette"][0]
                mask_below_zero = qs.get("mbz", ["0"])[0] == "1"
                png_bytes = _render_single_band(path, x, y, z, vmin, vmax, palette, mask_below_zero)
        except Exception:
            png_bytes = _TRANSPARENT_TILE_BYTES

        try:
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(png_bytes)))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(png_bytes)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            # Browser cancelled the tile request (e.g. pan/zoom); nothing to do.
            pass


class _TileHTTPServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        # Silence the default traceback dump for expected client-side disconnects.
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionAbortedError, ConnectionResetError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


_lock = threading.Lock()
_state = {"port": None}


def start_tile_server(allowed_root):
    """Start (once per process) a local daemon tile server; return its port."""
    with _lock:
        if _state["port"] is not None:
            return _state["port"]
        handler = type("_ScopedTileRequestHandler", (_TileRequestHandler,), {"allowed_root": allowed_root})
        httpd = _TileHTTPServer(("127.0.0.1", 0), handler)
        port = httpd.server_address[1]
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        _state["port"] = port
        return port


def single_band_tile_url(port, path, vmin, vmax, palette, mask_below_zero=False):
    """Build a folium/Leaflet {z}/{x}/{y} tile URL template for a single-band layer."""
    q = urllib.parse.urlencode({
        "path": path, "vmin": vmin, "vmax": vmax,
        "palette": palette, "mbz": 1 if mask_below_zero else 0,
    })
    return f"http://127.0.0.1:{port}/tile/single/{{z}}/{{x}}/{{y}}.png?{q}"


def rgb_tile_url(port, path):
    """Build a folium/Leaflet {z}/{x}/{y} tile URL template for a 3-band true-color layer."""
    q = urllib.parse.urlencode({"path": path})
    return f"http://127.0.0.1:{port}/tile/rgb/{{z}}/{{x}}/{{y}}.png?{q}"
