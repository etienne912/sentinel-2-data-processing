import os
from pathlib import Path

## STAC
STAC_CATALOG_URL = os.environ.get("STAC_CATALOG_URL", "https://earth-search.aws.element84.com/v1")
STAC_COLLECTION = "sentinel-2-l2a"
MAX_STAC_ITEMS = int(os.environ.get("MAX_STAC_ITEMS", "10"))
MAX_CLOUD_COVER = float(os.environ.get("MAX_CLOUD_COVER", "75.0"))

## Bands Management
SPECTRAL_BANDS = ["blue", "green", "red", "nir", "rededge1", "rededge2", "rededge3", "swir16", "swir22"]
SCL_ASSET_KEY = "scl"
SPECTRAL_BAND_REF = "blue"  # Using blue band raster settings as reference for all bands

# ESA SCL classes: 0=no data, 1=saturated, 3=cloud shadow,
# 8=cloud med, 9=cloud high, 10=thin cirrus, 11=snow
SCL_INVALID_CLASSES = {0, 1, 3, 8, 9, 10, 11}

## Tile Management
NATIVE_RESOLUTION_M = 10
DEFAULT_TILE_SIZE_PX = int(os.environ.get("TILE_SIZE", "256"))
DEFAULT_OVERLAP_PX = int(os.environ.get("TILE_OVERLAP", "0"))

## Post-processing parameters
PERCENTILE_LOW = 2
PERCENTILE_HIGH = 98

## Output
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "output"))
MANIFEST_FILENAME = "manifest.json"
NODATA_VALUE = float("nan")
