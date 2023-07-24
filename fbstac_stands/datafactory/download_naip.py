"""
Fetch NAIP images from Google Earth Engine (GEE) for each tile in the USGS 7.5 min grid.
"""
# %%
import os
from pathlib import Path
from datetime import datetime
from functools import partial
from PIL import Image
import json

from multiprocessing.pool import ThreadPool
import time

import ee
from affine import Affine
import geopandas as gpd
from shapely.geometry import box
import numpy as np

from pyproj import CRS

from gdstools import (
    split_bbox,
    GEEImageLoader,
    ConfigLoader,
    save_cog,
    image_collection,
    multithreaded_execution,
)

def timeit(method):
    """Decorator that times the execution of a method and prints the time taken."""
    def timed(*args, **kwargs):
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()
        print(f"{method.__name__} took {end_time - start_time:.2f} seconds to run.")
        return result
    return timed

# %%
def naip_from_gee(
    bbox: list,
    year: int,
    dim:int=1,
    num_threads:int=8,
    epsg:int=4326,
    scale:int=1
):
    """
    Fetch NAIP image url from Google Earth Engine (GEE) using a bounding box.

    :param bbox: Bounding box in the form [xmin, ymin, xmax, ymax].
    :type bbox: list
    :param year: Year (e.g. 2019)
    :type year: int
    :param epsg: EPSG code for coordinate reference system. Default is 4326.
    :type epsg: int, optional
    :param scale: Resolution in meters of the image to fetch. Default is 1.
    :type scale: int, optional
    :return: Returns a tuple containing the image as a numpy array and its metadata as a dictionary.
    :rtype: Tuple[np.ndarray, Dict]
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # get the naip image collection for our aoi and timeframe
    eebbox = ee.Geometry.BBox(*bbox)
    collection = (
        ee.ImageCollection("USDA/NAIP/DOQQ")
        .filterDate(start_date, end_date)
        .filterBounds(eebbox)
    )

    date_range = collection.reduceColumns(ee.Reducer.minMax(), ['system:time_start'])
    ts_end, ts_start = date_range.getInfo().values()

    image = GEEImageLoader(collection.median().clip(eebbox))
    imarray, profile = quad_fetch(
        collection, 
        bbox, 
        dim=dim, 
        num_threads=num_threads, 
        epsg=epsg, 
        scale=scale
    )

    image.metadata_from_collection(collection)
    image.set_property("system:time_start", ts_start)# * 1000)
    image.set_property("system:time_end", ts_end)# * 1000)
    image.set_params("scale", scale)
    image.set_params("crs", f"EPSG:{epsg}")
    image.set_params("region", bbox)
    image.set_viz_params("min", 0)
    image.set_viz_params("max", 255)
    image.set_viz_params("bands", ["R", "G", "B"])
    image.set_property("profile", profile)

    return imarray, image.metadata


def quad_fetch(
        collection:ee.Collection, 
        bbox: tuple, 
        dim:int=1, 
        num_threads:int=None, 
        **kwargs
    ):
    """
    Breaks user-provided bounding box into quadrants and retrieves data
    using `fetcher` for each quadrant in parallel using a ThreadPool.

    :param collection: Earth Engine image collection.
    :type collection: ee.Collection
    :param bbox: Coordinates of x_min, y_min, x_max, and y_max for bounding box of tile.
    :type bbox: tuple
    :param dim: Dimension of the quadrants to split the bounding box into. Default is 1.
    :type dim: int, optional
    :param num_threads: Number of threads to use for parallel executing of data requests. Default is None.
    :type num_threads: int, optional

    :return: Returns a tuple containing the image as a numpy array and its metadata as a dictionary.
    :rtype: tuple
    """
    def clip_image(bbox, scale, epsg):
        ee_bbox = ee.Geometry.BBox(*bbox)
        image = GEEImageLoader(collection.median().clip(ee_bbox))
        image.set_params("scale", scale)
        image.set_params("crs", f"EPSG:{epsg}")
        return image.to_array()

    if dim > 1:
        if num_threads is None:
            num_threads = dim**2

        bboxes = split_bbox(dim, bbox)

        get_quads = partial(clip_image, **kwargs)
        with ThreadPool(num_threads) as p:
            quads = p.map(get_quads, bboxes)

        # Split quads list in tuples of size dim
        images = [x[0] for x in quads]
        quad_list = [images[x:x + dim] for x in range(0, len(images), dim)]

        # Reverse order of rows to match rasterio's convention
        [x.reverse() for x in quad_list]
        image = np.concatenate(
            [
                np.hstack(quad_list[x]) for x in range(0, len(quad_list))
            ],
            2
        )

        profile = quads[0][1]
        first = quads[0][1]['transform']
        last = quads[-1][1]['transform']
        profile['transform'] = Affine(
            first.a,
            first.b,
            first.c,
            first.d,
            first.e,
            last.f
        )
        h, w = image.shape[1:]
        profile.update(width=w, height=h)

        return image, profile

    else:
        return naip_from_gee(bbox, **kwargs)


@timeit
def get_naip(
    bbox:tuple, 
    year:int, 
    outpath: str or Path, 
    dim:int=3, 
    overwrite:bool=False, 
    num_threads:int=None
):
    """
    Downloads a NAIP image from Google Earth Engine and saves it as a Cloud-Optimized GeoTIFF (COG) file.

    :param bbox: list-like
        list of bounding box coordinates (minx, miny, maxx, maxy)
    :type bbox: list
    :param year: int
        year of the NAIP image to download
    :type year: int
    :param outpath: str or Path
        path to save the downloaded image
    :type outpath: str or Path
    :param dim: int, optional
        dimension of the image to download (default is 3)
    :type dim: int
    :param overwrite: bool, optional
        whether to overwrite an existing file with the same name (default is False)
    :type overwrite: bool
    :param num_threads: int, optional
        number of threads to use for downloading (default is None)
    :type num_threads: int

    :return: None
    :rtype: None
    """
    if os.path.exists(outpath) and not overwrite:
        print(f"{outpath} already exists. Skipping...")
        return

    outpath = Path(outpath)
    try:
        image, metadata = naip_from_gee(bbox, dim=dim, year=year, num_threads=num_threads)
    except Exception as e:
        print(f"Failed to fetch {outpath.name}: {e}")
        return

    preview = Image.fromarray(
        np.moveaxis(image[:3], 0, -1).astype(np.uint8)
    ).convert('RGB')
    h, w = preview.size
    preview = preview.resize((w//30, h//30))
    preview.save(outpath.parent / outpath.name.replace('-cog.tif',
                 '-preview.png'), optimize=True)
    profile = metadata['properties']['profile']
    metadata['properties'].pop('profile')

    with open(outpath.parent / outpath.name.replace('-cog.tif', '-metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2)
    save_cog(image, profile, outpath, overwrite=overwrite)

    return


def infer_utm(bbox:tuple):
    """
    Infer the UTM Coordinate Reference System (CRS) by determining
    the UTM zone where a given lat/long bounding box is located.

    :param bbox: list-like
        List of bounding box coordinates (minx, miny, maxx, maxy)
    :type bbox: list-like

    :return: pyproj.CRS
        UTM crs for the bounding box
    :rtype: pyproj.CRS
    """
    xmin, _, xmax, _ = bbox
    midpoint = (xmax - xmin) / 2

    if xmax <= -120 + midpoint:
        epsg = 32610
    elif (xmin + midpoint > -120) and (xmax <= -114 + midpoint):
        epsg = 32611
    elif (xmin + midpoint > -114) and (xmax <= -108 + midpoint):
        epsg = 32612
    elif (xmin + midpoint > -108) and (xmax <= -102 + midpoint):
        epsg = 32613
    elif (xmin + midpoint > -102) and (xmax <= -96 + midpoint):
        epsg = 32614
    elif (xmin + midpoint > -96) and (xmax <= -90 + midpoint):
        epsg = 32615
    elif (xmin + midpoint > -90) and (xmax <= -84 + midpoint):
        epsg = 32616
    elif (xmin + midpoint > -84) and (xmax <= -78 + midpoint):
        epsg = 32617
    elif (xmin + midpoint > -78) and (xmax <= -72 + midpoint):
        epsg = 32618
    elif xmin + midpoint > -72:
        epsg = 32619

    return CRS.from_epsg(epsg)


def bbox_padding(geom:object, padding:int=1e3):
    """
    Add padding to a bounding box.

    :param geom: shapely.geometry.Polygon
        The geometry to add padding to.
    :type geom: shapely.geometry.Polygon
    :param padding: float, optional
        The amount of padding to add to the geometry, in meters. Default is 1000.
    :type padding: float

    :return: tuple
        A tuple of four floats representing the padded bounding box coordinates (minx, miny, maxx, maxy).
    :rtype: tuple
    """
    p_crs = infer_utm(geom.bounds)
    p_geom = gpd.GeoSeries(geom, crs=4326).to_crs(p_crs)
    if padding > 0:
        p_geom = p_geom.buffer(padding, join_style=2)

    return p_geom.to_crs(4326).bounds.values[0]


if "__main__" == __name__:

    run_as = "prod"
    conf = ConfigLoader(Path(__file__).parent.parent).load()
    api_url = conf['items']['naip']['api']

    if run_as == "dev":
        GRID = conf.DEV_GRID
        PROJDATADIR = conf.DEV_PROJDATADIR
        WORKERS = 3
    elif run_as == "prod":
        GRID = conf.GRID
        PROJDATADIR = conf.PROJDATADIR
        WORKERS = 3

    ee.Initialize(opt_url=api_url)

    # Load the QQ grid shapefile. Fetch data only for labels cellids
    labels = image_collection(PROJDATADIR + "/labels", file_pattern='*.geojson')
    cellids = [int(Path(x).name.split('_')[0]) for x in labels]
    years = set([int(Path(x).name.split('_')[1]) for x in labels])

    qq_shp = gpd.read_file(GRID)
    qq_shp['STATE'] = qq_shp.PRIMARY_STATE.apply(lambda x: str(x)[:2])
    qq_shp = qq_shp[qq_shp.CELL_ID.isin(cellids)]

    # Overwrite years if needed
    years = [2020, 2021, 2022]#2017, 2018, 2019, 2020, 2021]

    for year in years:
        outpath = Path(PROJDATADIR) / 'naip' / str(year)
        outpath.mkdir(exist_ok=True, parents=True)

        params = [
            {
                "bbox": row.geometry.bounds,
                "dim": 6,
                "year": year,
                "outpath": outpath / f"{row.CELL_ID}_{year}_{row.STATE.upper()}_NAIP_DOQQ-cog.tif",
                "num_threads": 11,
                "overwrite": False
            } for row in qq_shp.itertuples()
        ]

        multithreaded_execution(get_naip, params, WORKERS)
