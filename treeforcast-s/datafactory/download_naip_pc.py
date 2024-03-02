"""
Accessing NAIP data with the Planetary Computer STAC API
"""

# %% [markdown]
# ### Environment setup
# 
# This notebook works with or without an API key, but you will be given more permissive access to the data with an API key.
# The [Planetary Computer Hub](https://planetarycomputer.microsoft.com/compute) is pre-configured to use your API key.

# Set the environment variable PC_SDK_SUBSCRIPTION_KEY, or set it here.
# The Hub sets PC_SDK_SUBSCRIPTION_KEY automatically.
# pc.settings.set_subscription_key(<YOUR API Key>)

# %%
from pathlib import Path

import pystac_client
import planetary_computer as pc
import geopandas as gpd
from gdstools import ConfigLoader

# %%
conf = ConfigLoader(Path(__file__).parent.parent).load()

pc.settings.set_subscription_key(conf.PCKEY)

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=pc.sign_inplace,
)

import json
import shapely

# %% [markdown]
# ### Choose our region and times of interest
grid = gpd.read_file(conf.GRID)
grid = grid[grid.PRIMARY_STATE == 'Washington']
bbox = grid.total_bounds
shp = grid[grid.CELL_ID == 238213].geometry.values[0] 
shp_json = json.dumps(shapely.geometry.mapping(shp))

date_range = "2019-01-01/2022-12-31"

search = catalog.search(
    collections=["naip"], intersects=shp_json, datetime=date_range,
    # query={"eo:cloud_cover": {"lt": 10}},
)

items_new = search.item_collection()
print(f"{len(items_new)} Items found in the 'new' range")

# %% [markdown]
# As seen above, there are multiple items that intersect our area of interest for each year. The following code will choose the item that has the most overlap:

# %%
from shapely.geometry import shape

area_shape = shp
target_area = area_shape.area


def area_of_overlap(item):
    overlap_area = shape(item.geometry).intersection(shp).area
    return overlap_area / target_area


# item_old = sorted(items_old, key=area_of_overlap, reverse=True)[0]
item_new = sorted(items_new, key=area_of_overlap, reverse=True)[0]

# %% [markdown]
# ### Render images
# 
# Each Item has a `rendered_preview` which uses the Planetary Computer's Data API to dynamically create a preview image from the raw data.

# %%
from IPython.display import Image

# Image(url=item_old.assets["rendered_preview"].href)

# %%
Image(url=item_new.assets["rendered_preview"].href)

# %% [markdown]
# To read the raw Cloud Optimized GeoTIFF, use a library like [rioxarray](https://corteva.github.io/rioxarray/html/rioxarray.html) or [rasterio](https://rasterio.readthedocs.io/) and the `image` asset.

# %%
import rioxarray

ds = rioxarray.open_rasterio(item_new.assets["image"].href)#.sel(band=[1, 2, 3])
ds



# %%
