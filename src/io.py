import json
from pathlib import Path

import numpy as np
import pystac
import rasterio
import shapely
from rasterio.crs import CRS
from shapely import Polygon

from src.config import NODATA_VALUE, MANIFEST_FILENAME
from src.grid import TileSpec


def write_tiles(
    tiles: dict[str, dict[str, np.ndarray]],
    tile_grid: list[TileSpec],
    output_dir: Path,
    band_keys: list[str],
    image_crs: CRS,
    source_item_id: str,
    acquisition_date: str,
) -> list[Path]:
    """
    Write tiles on disk

    Parameters
    ----------
    tiles
    tile_grid
    output_dir
    band_keys
    image_crs
    source_item_id
    acquisition_date

    Returns
    -------

    """
    # rasterio.open(..., 'w', driver='GTiff', dtype='float32', nodata=NaN)
    # Per-band tags: band_name, source_item, acquisition_date
    epoch_dir = output_dir / source_item_id
    epoch_dir.mkdir(parents=True, exist_ok=True)

    tile_paths: list[Path] = []
    for tile_spec in tile_grid:
        tile_bands = tiles[tile_spec.id]
        filename = f"{source_item_id}_{tile_spec.col}_{tile_spec.row}.tif"
        out_path = epoch_dir / filename

        stack = np.stack([tile_bands[b] for b in band_keys], axis=0)

        with rasterio.open(
            out_path,
            "w",
            driver="GTiff",
            dtype="float32",
            count=len(band_keys),
            height=tile_spec.height_px,
            width=tile_spec.width_px,
            crs=image_crs,
            transform=tile_spec.transform,
            nodata=NODATA_VALUE,
        ) as dst:
            dst.write(stack)
            for i, band_name in enumerate(band_keys, start=1):
                dst.update_tags(
                    i, band_name=band_name, source_item=source_item_id, acquisition_date=str(acquisition_date)
                )

        tile_paths.append(out_path)

    return tile_paths


def build_manifest_entry(
    t1_item: pystac.Item,
    t2_item: pystac.Item,
    tile_paths_t1: list[Path],
    tile_paths_t2: list[Path],
    output_dir: Path,
    aoi: Polygon,
    band_keys: list[str],
) -> dict:
    """
    Build a manifest entry

    Parameters
    ----------
    t1_item : pystac.Item
        Selected item for the T1 epoch
    t2_item : pystac.Item
        Selected item for the T2 epoch
    tile_paths_t1 : list[Path]
        The path list of the T1 tiles
    tile_paths_t2 : list[Path]
        The path list of the T1 tiles
    output_dir : Path
        The output directory
    aoi : Polygon
        The AOI
    band_keys : list[str]
        The keys of the bands requested by the user

    Returns
    -------
    dict
        A complet manifest entry
    """
    return {
        "t1_item_id": t1_item.id,
        "t2_item_id": t2_item.id,
        "aoi": shapely.to_geojson(aoi),
        "band_keys": band_keys,
        "t1_tiles": [str(p.relative_to(output_dir)) for p in tile_paths_t1],
        "t2_tiles": [str(p.relative_to(output_dir)) for p in tile_paths_t2],
    }


def write_manifest(entries: dict, output_dir: Path) -> Path:
    """
    Write manifest file on disk

    Parameters
    ----------
    entries : dict
        The content of the manifest
    output_dir : Path
        The output directory

    Returns
    -------
    Path
        The path to the manifest file
    """
    manifest_path = output_dir / MANIFEST_FILENAME

    with open(manifest_path, "w") as f:
        json.dump(entries, f, indent=2)

    return manifest_path


def load_geojson(path: str | Path) -> shapely.Polygon:
    """
    Load geojson from path

    Parameters
    ----------
    path : str | Path
        The path to the geojson

    Returns
    -------
    shapely.Polygon
        A shapely polygon that represents the geojson

    Raises
    ------
    FileNotFoundError
        If the file does not exist
    ValueError
        If the geojson is not a Polygon
    """
    geojson_path = Path(path)
    if not geojson_path.exists():
        raise FileNotFoundError(f"GeoJSON file not found: '{path}'.")

    with open(geojson_path) as f:
        geojson = shapely.from_geojson(f.read())

    if not isinstance(geojson, shapely.Polygon):
        raise ValueError(f"GeoJSON must be a Polygon, got {type(geojson).__name__}.")

    return geojson
