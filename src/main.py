import argparse
import logging
from datetime import datetime
from pathlib import Path

import shapely
from pyproj import Transformer
from pystac import Item
from pystac_client import Client

from src.config import (
    SPECTRAL_BANDS,
    DEFAULT_TILE_SIZE_PX,
    DEFAULT_OVERLAP_PX,
    OUTPUT_DIR,
    STAC_CATALOG_URL,
    MAX_CLOUD_COVER,
    SPECTRAL_BAND_REF,
)
from src.download import fetch_aoi_scl, fetch_aoi_data_bands, build_reference_grid
from src.grid import build_tile_grid
from src.io import load_geojson, write_manifest, write_tiles, build_manifest_entry
from src.process import normalize_bands, split_into_tiles, apply_mask
from src.stac import open_catalog, search_products, select_best_product, get_item_crs
from src.utils import parse_date

logger = logging.getLogger(__name__)


def _process_epoch(
    t_id: str,
    date_range: tuple[datetime, datetime],
    aoi: shapely.Polygon,
    catalog: Client,
    bands_keys: list[str],
    tile_size_px: int,
    overlap_px: int,
    output_dir: Path,
    max_cloud_cover: float,
) -> tuple[Item, list[Path]]:
    logger.info(f"Processing {t_id} images between {date_range[0]} and {date_range[1]}")

    # Products
    products = search_products(catalog, aoi, date_range[0], date_range[1], max_items=10)
    product = select_best_product(products, max_cloud_cover)
    product_crs = get_item_crs(product)

    logger.debug(f"Best product: {product.id} ({product.datetime})")

    product_ref_grid = build_reference_grid(product.assets[SPECTRAL_BAND_REF].href, aoi.bounds)

    # Reproject the AOI into the selected product CRS
    transformer = Transformer.from_crs(4326, product_crs, always_xy=True)
    projected_aoi = shapely.transform(aoi, transformer.transform, interleaved=False)

    # Build tile grid
    tile_grid = build_tile_grid(
        aoi=aoi,
        product_crs=product_crs,
        tile_size_px=tile_size_px,
        overlap_px=overlap_px,
    )
    logger.debug(f"Grid created with {len(tile_grid)} tiles")

    # Download bands
    bands = fetch_aoi_data_bands(item=product, band_keys=bands_keys, ref_grid=product_ref_grid)
    scl = fetch_aoi_scl(item=product, ref_grid=product_ref_grid)

    # Process bands
    bands = apply_mask(bands, scl, projected_aoi, product_ref_grid["transform"])
    bands = normalize_bands(bands)

    # Split bands in tiles
    tiles = split_into_tiles(bands, tile_grid, product_ref_grid["transform"])

    tile_paths = write_tiles(
        tiles, tile_grid, output_dir, bands_keys, product_ref_grid["crs"], product.id, str(product.datetime)
    )
    return product, tile_paths


def run_pipeline(
    aoi: shapely.Polygon,
    t1_date_range: tuple[datetime, datetime],
    t2_date_range: tuple[datetime, datetime],
    bands_keys: list[str] = SPECTRAL_BANDS,
    tile_size_px: int = DEFAULT_TILE_SIZE_PX,
    overlap_px: int = DEFAULT_OVERLAP_PX,
    output_dir: Path = OUTPUT_DIR,
    max_cloud_cover: float = MAX_CLOUD_COVER,
    stac_url: str = STAC_CATALOG_URL,
) -> dict:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    catalog = open_catalog(stac_url)

    epoch_kwargs = dict(
        aoi=aoi,
        catalog=catalog,
        bands_keys=bands_keys,
        tile_size_px=tile_size_px,
        overlap_px=overlap_px,
        output_dir=output_dir,
        max_cloud_cover=max_cloud_cover,
    )
    t1_item, t1_tile_paths = _process_epoch("t1", t1_date_range, **epoch_kwargs)
    t2_item, t2_tile_paths = _process_epoch("t2", t2_date_range, **epoch_kwargs)

    entries = build_manifest_entry(t1_item, t2_item, t1_tile_paths, t2_tile_paths, output_dir, aoi, bands_keys)
    manifest_path = write_manifest(entries, output_dir)

    return {"manifest": manifest_path, "t1_tiles": t1_tile_paths, "t2_tiles": t2_tile_paths}


def validate_args(args):
    if args.t1_end <= args.t1_start:
        raise ValueError("--t1-end must be after --t1-start.")
    if args.t2_end <= args.t2_start:
        raise ValueError("--t2-end must be after --t2-start.")
    if args.t2_start <= args.t1_end:
        raise ValueError("T2 window must start after T1 window ends.")

    invalid = set(args.bands) - set(SPECTRAL_BANDS)
    if invalid:
        raise ValueError(f"Unknown bands: {invalid}. Valid options are: {SPECTRAL_BANDS}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Change detection pipeline for Sentinel-2 L2A imagery.")

    parser.add_argument(
        "--aoi",
        required=True,
        help="Path to a GeoJSON file defining the area of interest.",
    )
    parser.add_argument(
        "--t1-start",
        required=True,
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Start date for the T1 (before) search window.",
    )
    parser.add_argument(
        "--t1-end",
        required=True,
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="End date for the T1 (before) search window.",
    )

    parser.add_argument(
        "--t2-start",
        required=True,
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Start date for the T2 (after) search window.",
    )
    parser.add_argument(
        "--t2-end",
        required=True,
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="End date for the T2 (after) search window.",
    )

    parser.add_argument(
        "--tile-size",
        required=False,
        default="256",
        type=int,
        help="Output patch size in pixels. Default: 256.",
    )

    parser.add_argument(
        "--bands",
        required=False,
        default=SPECTRAL_BANDS,
        nargs="+",
        metavar="BAND",
        help="List of Sentinel-2 bands to process (e.g. B02 B03 B04 B08).",
    )

    parser.add_argument(
        "--output-dir",
        required=False,
        default=OUTPUT_DIR,
        help="Directory where processed patches and manifest will be saved. Default: ./output.",
    )

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")

    args = parse_args()
    validate_args(args)

    try:
        aoi = load_geojson(args.aoi)
    except (FileNotFoundError, ValueError) as e:
        raise SystemExit(f"Invalid AOI: {e}") from e

    run_pipeline(
        aoi=aoi,
        t1_date_range=(args.t1_start, args.t1_end),
        t2_date_range=(args.t2_start, args.t2_end),
        bands_keys=args.bands,
        tile_size_px=args.tile_size,
        output_dir=Path(args.output_dir),
    )


if __name__ == "__main__":
    main()
