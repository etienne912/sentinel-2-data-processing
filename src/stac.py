from datetime import datetime

import pystac
import pystac_client
from pystac_client import ItemSearch
from shapely.geometry.polygon import Polygon

from src import config
from src.config import STAC_COLLECTION


def open_catalog(url: str = config.STAC_CATALOG_URL) -> pystac_client.Client:
    """
    Opens the STAC Catalog and returns the client.

    Parameters
    ----------
    url : str
        URL of the STAC Catalog

    Returns
    -------
    Client
        The STAC Catalog client
    """
    catalog = pystac_client.Client.open(url)
    return catalog


def search_products(
    catalog: pystac_client.Client, aoi: Polygon, date_start: datetime, date_end: datetime, max_items: int
) -> ItemSearch:
    """
    Search for products within a polygon.

    Parameters
    ----------
    catalog : pystac_client.Client
        The STAC Catalog
    aoi : Polygon
        The AOI
    date_start : datetime
        The start date to search for the products
    date_end : datetime
        The end date to search for the products
    max_items : int
        The maximum number of products to return

    Returns
    -------
    ItemSearch
        List of items order by cloud cover
    """
    results = catalog.search(
        max_items=max_items,
        bbox=aoi.bounds,
        datetime=[date_start, date_end],
        collections=[STAC_COLLECTION],
        sortby="+properties.eo:cloud_cover",
    )
    return results


def select_best_product(items: ItemSearch, max_cloud_cover: float | None = None) -> pystac.Item:
    """
    Selects the best product within a collection.
    (function is quite basic for now)

    Parameters
    ----------
    items : ItemSearch
        List of items to select
    max_cloud_cover : float | None
        The maximum cloud cover

    Returns
    -------
    pystac.Item
        The best product within a collection regarding the criteria
    """
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
    """
    Return the CRS of the item.

    Parameters
    ----------
    item: pystac.Item
        The item to get the CRS

    Returns
    -------
    string
        The CRS of the item
    """
    crs = item.properties.get("proj:code") or item.properties.get("proj:epsg")
    if crs is None:
        raise ValueError(f"Item {item.id} has no CRS property (proj:code / proj:epsg).")
    return str(crs)
