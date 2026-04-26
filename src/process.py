import affine
import numpy as np
from rasterio.features import geometry_mask
from shapely import Polygon
from shapely.geometry import mapping

from src.config import PERCENTILE_HIGH, SCL_INVALID_CLASSES, PERCENTILE_LOW
from src.grid import TileSpec


def apply_mask(
    bands: dict[str, np.ndarray],
    scl: np.ndarray,
    aoi: Polygon,
    transform: affine.Affine,
    invalid_classes: set[int] = SCL_INVALID_CLASSES,
) -> dict[str, np.ndarray]:
    """
    Mask using the SCL and the AOI each band in a raster.

    Parameters
    ----------
    bands : dict
        Dict containing band names as keys and numpy arrays as values.
    scl : np.ndarray
        The SCL data array
    aoi : Polygon
        The AOI
    transform : affine.Affine
        The affine transform to apply to the raster.
    invalid_classes : set[int]
        Set of invalid classes to mask.

    Returns
    -------
    dict
        Masked bands
    """
    h, w = scl.shape

    scl_mask = np.isin(scl.astype(np.uint8), list(invalid_classes))

    # True OUTSIDE the AOI, False INSIDE (rasterio convention)
    aoi_mask = geometry_mask(
        geometries=[mapping(aoi)],
        out_shape=(h, w),
        transform=transform,
        all_touched=True,
        invert=False,
    )

    mask = np.logical_or(aoi_mask, scl_mask)

    return {band_name: np.where(mask, np.nan, band.astype(np.float32)) for band_name, band in bands.items()}


def normalize_bands(
    bands: dict[str, np.ndarray], low_pct: float = PERCENTILE_LOW, high_pct: float = PERCENTILE_HIGH
) -> dict[str, np.ndarray]:
    """
    Normalize the band values in each band in a raster.

    Parameters
    ----------
    bands : dict
        Dict containing band names as keys and numpy arrays as values.
    low_pct : float
        Lower percentile value to normalize band values.
    high_pct : float
        Higher percentile value to normalize band values.

    Returns
    -------
    dict
        The bands normalized
    """
    result = {}
    for band_name, band in bands.items():
        low = np.nanpercentile(band, low_pct)
        high = np.nanpercentile(band, high_pct)
        if high == low:
            result[band_name] = np.full_like(band, np.nan)
        else:
            result[band_name] = np.clip((band - low) / (high - low), 0, 1)
    return result


def _extract_patch(
    band: np.ndarray,
    row_start: int,
    col_start: int,
    height_px: int,
    width_px: int,
) -> np.ndarray:
    aoi_h, aoi_w = band.shape
    # Pre-fill with NaN so out-of-bounds areas remain as nodata
    patch = np.full((height_px, width_px), np.nan, dtype=np.float32)

    # Clamp the source window to the AOI raster bounds
    src_r0, src_c0 = max(row_start, 0), max(col_start, 0)
    src_r1, src_c1 = min(row_start + height_px, aoi_h), min(col_start + width_px, aoi_w)

    if src_r1 > src_r0 and src_c1 > src_c0:
        dst_r0, dst_c0 = src_r0 - row_start, src_c0 - col_start
        patch[dst_r0 : dst_r0 + (src_r1 - src_r0), dst_c0 : dst_c0 + (src_c1 - src_c0)] = band[
            src_r0:src_r1, src_c0:src_c1
        ]

    return patch


def split_into_tiles(
    bands: dict[str, np.ndarray], tiles: list[TileSpec], aoi_transform: affine.Affine
) -> dict[str, dict[str, np.ndarray]]:
    """Slice each band into per-tile patches aligned to the tile grid.

    Uses the inverse affine transform to convert each tile's CRS bounds
    into pixel coordinates within the AOI raster, then delegates the
    actual windowed copy to `_extract_patch`.

    Returns a nested dict: {tile_id: {band_name: patch_array}}.
    """
    # Inverse transform maps CRS coordinates -> pixel (col, row) offsets
    inv_transform = ~aoi_transform

    return {
        tile.id: {
            band_name: _extract_patch(
                band, *_tile_origin_px(inv_transform, tile.bounds_crs), tile.height_px, tile.width_px
            )
            for band_name, band in bands.items()
        }
        for tile in tiles
    }


def _tile_origin_px(inv_transform: affine.Affine, bounds_crs: tuple[float, float, float, float]) -> tuple[int, int]:
    """Return (row_start, col_start) of the tile's top-left corner in AOI pixel space."""
    xmin, _, _, ymax = bounds_crs  # top-left corner in CRS is (xmin, ymax)
    col, row = inv_transform * (xmin, ymax)
    return int(round(row)), int(round(col))
