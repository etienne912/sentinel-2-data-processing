import logging

import pystac
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.windows import from_bounds
from rasterio.warp import transform_bounds, reproject

from src.config import SCL_ASSET_KEY

logger = logging.getLogger(__name__)


def rasterio_env():
    """
    GDAL config for efficient cloud streaming.
    Works for HTTPS (COGs), AWS S3, and GCS.
    """
    return rasterio.Env(
        AWS_NO_SIGN_REQUEST="YES",  # public S3 buckets (Sentinel-2, Landsat)
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.tiff,.jp2",
        GDAL_HTTP_MULTIRANGE="YES",
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
    )


def build_reference_grid(ref_url: str, bounds_wgs84: tuple[float, float, float, float]) -> dict:
    with rasterio_env(), rasterio.open(ref_url) as ref:
        xmin, ymin, xmax, ymax = transform_bounds("EPSG:4326", ref.crs, *bounds_wgs84, densify_pts=21)

        window = from_bounds(xmin, ymin, xmax, ymax, ref.transform)
        window = window.round_offsets().round_lengths()

        return {
            "crs": ref.crs,
            "transform": ref.window_transform(window),
            "height": int(window.height),
            "width": int(window.width),
        }


def read_band_window(href: str, ref_grid: dict, resampling: Resampling) -> np.ndarray:
    with rasterio_env(), rasterio.open(href) as src:
        dst = np.empty((ref_grid["height"], ref_grid["width"]), dtype=np.float32)

        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=ref_grid["transform"],
            dst_crs=ref_grid["crs"],
            resampling=resampling,
            dst_nodata=np.nan,
        )

        if src.nodata is not None:
            dst[dst == src.nodata] = np.nan

        return dst


def fetch_aoi_data_bands(
    item: pystac.Item,
    ref_grid: dict,
    band_keys: list[str],
) -> dict[str, np.ndarray]:
    product: dict[str, np.ndarray] = {}
    for band_key in band_keys:
        if band_key not in item.assets:
            raise ValueError(f"Item {item.id} is missing asset '{band_key}'.")
        product[band_key] = read_band_window(item.assets[band_key].href, ref_grid, resampling=Resampling.bilinear)
    return product


def fetch_aoi_scl(
    item: pystac.Item,
    ref_grid: dict,
) -> np.ndarray:
    return read_band_window(item.assets[SCL_ASSET_KEY].href, ref_grid, resampling=Resampling.nearest)
