import logging

import numpy as np
import pystac
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import transform_bounds, reproject
from rasterio.windows import from_bounds

from src.config import SCL_ASSET_KEY

logger = logging.getLogger(__name__)


def build_reference_grid(ref_url: str, bounds_wgs84: tuple[float, float, float, float]) -> dict:
    """
    Build a reference grid for an area of interest (AOI) based on a reference raster.

    Parameters
    ----------
    ref_url : str
        The URL or path to the reference raster.
    bounds_wgs84 : tuple[float, float, float, float]
        The bounding box of the AOI in WGS84 coordinates (xmin, ymin, xmax, ymax).

    Returns
    -------
    dict
        A dictionary containing the reference grid information including CRS, transform, height, and width.
    """
    with rasterio.open(ref_url) as ref:
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
    """
    Download band data and reproject it following the reference grid

    Parameters
    ----------
    href : str
        The URL or path to the reference raster.
    ref_grid : dict
        The reference grid.
    resampling : Resampling
        The resampling parameter.

    Returns
    -------
    np.ndarray
        A numpy array containing the band data.
    """
    with rasterio.open(href) as src:
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
    """
    Fetch aoi data bands from a STAC item.

    Parameters
    ----------
    item : pystac.Item
        The STAC item containing the data bands.
    ref_grid : dict
        The reference grid for the AOI.
    band_keys : list[str]
        List of band keys to fetch from the item.

    Returns
    -------
    dict[str, np.ndarray]
        Dictionary mapping band keys to their corresponding data arrays.

    Raises
    ------
    ValueError
        If the item is missing any of the specified band assets.
    """
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
