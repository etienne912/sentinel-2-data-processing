import itertools
from dataclasses import dataclass

import affine
import shapely
from pyproj import Transformer

from src.config import DEFAULT_TILE_SIZE_PX, DEFAULT_OVERLAP_PX, NATIVE_RESOLUTION_M


@dataclass(frozen=True)
class TileSpec:
    col: int
    row: int
    bounds_crs: tuple[float, float, float, float]  # (xmin, ymin, xmax, ymax) in image_crs
    width_px: int
    height_px: int
    transform: affine.Affine

    @property
    def id(self) -> str:
        return f"{self.col}_{self.row}"


def build_tile_grid(
    aoi: shapely.Polygon,  # WGS-84
    product_crs: str,
    tile_size_px: int = DEFAULT_TILE_SIZE_PX,
    overlap_px: int = DEFAULT_OVERLAP_PX,
    resolution_m: float = NATIVE_RESOLUTION_M,
) -> list[TileSpec]:
    """
    Build a tile grid with fixed size to cover the whole AOI.

    Parameters
    ----------
    aoi : shapely.Polygon
        The AOI to build the tile grid from.
    product_crs : string
        The CRS of the product.
    tile_size_px : int
        The size of the tile in pixels.
    overlap_px : int
        The overlap in pixels.
    resolution_m : float
        The resolution of the tiles in meters.

    Returns
    -------
    list[TileSpec]
        A list of TileSpec objects that all together cover the AOI.
    """
    # 1. Project AOI EPSG:4326 -> image_crs
    t = Transformer.from_crs("EPSG:4326", product_crs, always_xy=True)
    bounds = aoi.bounds
    b_xmin, b_ymin = t.transform(bounds[0], bounds[1])
    b_xmax, b_ymax = t.transform(bounds[2], bounds[3])

    # 2. Compute tile size and stride in meters
    tile_size_m = tile_size_px * resolution_m
    stride_m = (tile_size_px - overlap_px) * resolution_m

    # 3. Snap origin to the stride grid (floor) so tiles align consistently
    origin_x = (b_xmin // stride_m) * stride_m
    origin_y = (b_ymin // stride_m) * stride_m

    # 4. Enumerate columns and rows, computing positions from origin to avoid float drift
    cols = range(int((b_xmax - origin_x) // stride_m) + 1)
    rows = range(int((b_ymax - origin_y) // stride_m) + 1)

    return [
        TileSpec(
            col=col,
            row=row,
            bounds_crs=(
                origin_x + col * stride_m,
                origin_y + row * stride_m,
                origin_x + col * stride_m + tile_size_m,
                origin_y + row * stride_m + tile_size_m,
            ),
            width_px=tile_size_px,
            height_px=tile_size_px,
            transform=affine.Affine(
                resolution_m,
                0.0,
                origin_x + col * stride_m,
                0.0,
                -resolution_m,
                origin_y + row * stride_m + tile_size_m,
            ),
        )
        for col, row in itertools.product(cols, rows)
    ]
