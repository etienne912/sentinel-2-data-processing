import pystac
import pystac_client
from pystac_client import ItemSearch
from shapely.geometry.polygon import Polygon

from src import config
from src.config import STAC_COLLECTION


def open_catalog(url: str = config.STAC_CATALOG_URL) -> pystac_client.Client:
    catalog = pystac_client.Client.open(url)
    return catalog


def search_products(catalog: pystac_client.Client, aoi: Polygon, date_start, date_end, max_items: int) -> ItemSearch:
    results = catalog.search(
        max_items=max_items,
        bbox=aoi.bounds,
        datetime=[date_start, date_end],
        collections=[STAC_COLLECTION],
        sortby="+properties.eo:cloud_cover",
    )
    return results


def select_best_product(items: ItemSearch, max_cloud_cover: float | None = None) -> pystac.Item:
    try:
        best_item = next(items.items())
    except StopIteration:
        raise ValueError("No items found in the search results.")
    if max_cloud_cover is not None and best_item.properties["eo:cloud_cover"] > max_cloud_cover:
        raise ValueError(
            f"Best item cloud cover ({best_item.properties['eo:cloud_cover']:.1f}%) "
            f"exceeds threshold of {max_cloud_cover}%"
        )
    return best_item


def get_item_crs(item: pystac.Item) -> str:
    crs = item.properties.get("proj:code") or item.properties.get("proj:epsg")
    if crs is None:
        raise ValueError(f"Item {item.id} has no CRS property (proj:code / proj:epsg).")
    return str(crs)
