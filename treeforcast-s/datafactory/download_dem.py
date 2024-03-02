"""
Download Digital Elevation Model (DEM) from the 3DEP web service and calculate topographic metrics.
"""
# %%
import os
import sys
from typing import List, Tuple
import requests
from pathlib import Path
import contextlib
from functools import partial
from multiprocessing.pool import ThreadPool
from PIL import Image
import matplotlib.pyplot as plt

import numpy as np
import geopandas as gpd
import rasterio
from rasterio import MemoryFile
from rasterio import transform
from rasterio.warp import reproject, Resampling
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from scipy.ndimage import convolve
from skimage import filters
from skimage.morphology import disk
from skimage.util import apply_parallel
from tqdm import tqdm
from pyproj import CRS

from gdstools import (
    create_directory_tree,
    degrees_to_meters,
    split_bbox,
    print_message,
    multithreaded_execution,
    image_collection,
    ConfigLoader
)

def dem_from_tnm(bbox, res=10, crs=4326, **kwargs):
    """
    Retrieve a Digital Elevation Model (DEM) image from The National Map (TNM)
    web service.

    :param bbox: list-like
        List of bounding box coordinates (minx, miny, maxx, maxy).
    :type bbox: list
    :param res: numeric
        Spatial resolution to use for returned DEM (grid cell size).
    :type res: float
    :param inSR: int
        Spatial reference for bounding box, such as an EPSG code (e.g., 4326).
    :type inSR: int

    :returns: numpy array
        DEM image as array.
    """
    xmin, ymin, xmax, ymax = bbox

    if crs == 4326:
        dx = degrees_to_meters(xmax - xmin)
        dy = degrees_to_meters(ymax - ymin, angle='lat')
    else:
        dx = xmax - xmin
        dy = ymax - ymin

    width = int(abs(dx) // res)  # type: ignore
    height = int(abs(dy) // res)  # type: ignore

    BASE_URL = ''.join([
        'https://elevation.nationalmap.gov/arcgis/rest/',
        'services/3DEPElevation/ImageServer/exportImage'
    ])

    params = dict(
        bbox=','.join([str(x) for x in bbox]),
        bboxSR=crs,
        size=f'{width},{height}',
        imageSR=crs,
        time=None,
        format='tiff',
        pixelType='F32',
        noData=None,
        noDataInterpretation='esriNoDataMatchAny',
        interpolation='+RSP_BilinearInterpolation',
        compression=None,
        compressionQuality=None,
        bandIds=None,
        mosaicRule=None,
        renderingRule=None,
        f='image'
    )

    for key, value in kwargs.items():
        params.update({key: value})

    r = requests.get(BASE_URL, params=params)
    with MemoryFile(r.content) as memfile:
        src = memfile.open()
        dem = src.read(1)

    return dem


def download_dem(filepath:str or Path, bbox:tuple, res:int=10, incrs:int=4326, **kwargs):
    """
    Retrieves a Digital Elevation Model (DEM) image from The National Map (TNM)
    web service.

    :param filepath: str
        Path to output file.
    :type filepath: str
    :param bbox: list-like
        List of bounding box coordinates (minx, miny, maxx, maxy).
    :type bbox: list
    :param res: numeric, optional
        Spatial resolution to use for returned DEM (grid cell size). Default is 10.
    :type res: int or float
    :param inSR: int, optional
        Spatial reference for bounding box, such as an EPSG code (e.g., 4326). Default is 4326.
    :type inSR: int
    :param kwargs: optional
        Additional parameters to pass to the requests.get() function.

    :return: numpy array
        DEM image as array.
    :rtype: numpy.ndarray
    """
    xmin, ymin, xmax, ymax = bbox

    if incrs == 4326:
        dx = degrees_to_meters(xmax - xmin)
        dy = degrees_to_meters(ymax - ymin, angle='lat')
    else:
        dx = xmax - xmin
        dy = ymax - ymin

    width = int(abs(dx) // res)  # type: ignore
    height = int(abs(dy) // res)  # type: ignore

    BASE_URL = ''.join([
        'https://elevation.nationalmap.gov/arcgis/rest/',
        'services/3DEPElevation/ImageServer/exportImage'
    ])

    params = dict(
        bbox=','.join([str(x) for x in bbox]),
        bboxSR=incrs,
        size=f'{width},{height}',
        imageSR=incrs,
        time=None,
        format='tiff',
        pixelType='F32',
        noData=None,
        noDataInterpretation='esriNoDataMatchAny',
        interpolation='+RSP_BilinearInterpolation',
        compression=None,
        compressionQuality=None,
        bandIds=None,
        mosaicRule=None,
        renderingRule=None,
        f='image'
    )

    for key, value in kwargs.items():
        params.update({key: value})

    r = requests.get(BASE_URL, params=params)

    #
    profile = dict(
        driver='GTiff',
        interleave='band',
        tiled=True,
        crs=incrs,
        width=width,
        height=height,
        transform=transform.from_bounds(*bbox, width, height),
        blockxsize=256,
        blockysize=256,
        compress='lzw',
        nodata=-9999,
        dtype=rasterio.float32,
        count=1,
    )

    with MemoryFile(r.content) as data:
        with rasterio.open(filepath, 'w', **profile) as dst:
            src = data.open()
            dst.write(src.read())
            dst.update_tags(**src.tags())
            del src


def download_quad(
        filepath: str, 
        bbox: Tuple[float, float, float, float], 
        dim: int = 1, 
        num_threads:int = None, 
        **kwargs
    ) -> None:
    """Breaks user-provided bounding box into quadrants and retrieves data
    for each quadrant in parallel using a ThreadPool.

    :param filepath: The path to the file to be downloaded.
    :type filepath: str
    :param bbox: A tuple containing the bounding box coordinates in the order (x_min, y_min, x_max, y_max).
    :type bbox: tuple
    :param dim: The number of quadrants to break the bounding box into, by default 1.
    :type dim: int, optional
    :param num_threads: The number of threads to use for parallel execution of data requests, by default None.
    :type num_threads: int or None, optional
    :param kwargs: Additional keyword arguments that will be passed to download_dem.
    :type kwargs: dict
    :return: None
    :rtype: None
    """
    from gdstools.geeutils import multithreaded_download

    if dim > 1:
        if num_threads is None:
            num_threads = dim**2

        bboxes = split_bbox(dim, bbox)
        n = len(bboxes)

        # Create an array of parameters for each quadrant
        params = np.array([
            [f'{filepath}_{i}.tif' for i in range(n)],
            bboxes.tolist(),
        ], dtype=object).T

        # Download each quadrant in parallel
        multithreaded_download(params, download_dem)

    else:
        # Download the single quadrant
        download_dem(f'{filepath}.tif', bbox, **kwargs)


def quad_fetch(bbox, dim=1, num_threads=None, **kwargs):
    """Breaks user-provided bounding box into quadrants and retrieves data
    for each quadrant in parallel using a ThreadPool.

    :param bbox: A tuple or list containing the bounding box coordinates in the order (x_min, y_min, x_max, y_max).
    :type bbox: tuple or list
    :param dim: The number of quadrants to break the bounding box into, by default 1.
    :type dim: int, optional
    :param num_threads: The number of threads to use for parallel execution of data requests, by default None.
    :type num_threads: int or None, optional
    :param kwargs: Additional keyword arguments that will be passed to dem_from_tnm.
    :type kwargs: dict
    :return: An array containing the image returned with quads stitched together into a single array.
    :rtype: array
    """
    if dim > 1:
        if num_threads is None:
            num_threads = dim**2

        bboxes = split_bbox(dim, bbox)
        n = len(bboxes)

        get_quads = partial(dem_from_tnm, **kwargs)
        with ThreadPool(num_threads) as p:
            quads = p.map(get_quads, bboxes)

        # Split quads list in tuples of size dim
        quad_list = [quads[x:x + dim] for x in range(0, len(quads), dim)]
        # Reverse order of rows to match rasterio's convention
        [x.reverse() for x in quad_list]
        return np.hstack([np.vstack(quad_list[x]) for x in range(0, len(quad_list))])

    else:
        dem = dem_from_tnm(bbox, **kwargs)

    return dem

# %%
def tpi(
        dem: np.ndarray, 
        irad: float = 5, 
        orad: float = 10, 
        res: float = 10, 
        norm: bool = False
    ) -> np.ndarray:
    """
    Generate a raster of Topographic Position Index (TPI) from a Digital
    Elevation Model (DEM).

    TPI is the difference between the elevation at a location from the average
    elevation of its surroundings, calculated using an annulus (ring). This
    function permits the calculation of average surrounding elevation using
    a coarser grain, and return the TPI user a higher-resolution DEM.

    :param dem: A numpy array containing the Digital Elevation Model (DEM).
    :type dem: np.ndarray
    :param irad: The inner radius of annulus used to calculate TPI, by default 5.
    :type irad: float, optional
    :param orad: The outer radius of annulus used to calculate TPI, by default 10.
    :type orad: float, optional
    :param res: The spatial resolution of Digital Elevation Model (DEM), by default 10.
    :type res: float, optional
    :param norm: Whether to return a normalized version of TPI, with mean = 0 and SD = 1, by default False.
    :type norm: bool, optional

    :return: A numpy array containing the Topographic Position Index (TPI) image.
    :rtype: np.ndarray
    """
    k_orad = orad // res
    k_irad = irad // res

    kernel = disk(k_orad) - np.pad(disk(k_irad), pad_width=(k_orad - k_irad))
    weights = kernel / kernel.sum()

    def conv(dem): return convolve(dem, weights, mode='nearest')

    convolved = apply_parallel(conv, dem, compute=True, depth=k_orad)
    tpi = dem - convolved

    if norm:
        tpi_mean = (dem - convolved).mean()
        tpi_std = (dem - convolved).std()
        tpi = (tpi - tpi_mean) / tpi_std

    return tpi


# %%
def infer_utm(bbox):
    """
    Infer the UTM Coordinate Reference System (CRS) by determining the UTM zone where a given lat/long bounding box is located.

    :param bbox: A list-like object containing the bounding box coordinates (minx, miny, maxx, maxy).
    :type bbox: list-like
    :return: A pyproj.CRS object representing the UTM CRS for the bounding box.
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
    elif (xmin + midpoint > -72):
        epsg = 32619

    return CRS.from_epsg(epsg)


# Supress output from c++ shared libs and python warnings
# so the progress bar doesn't get messed up
# See:
# 1. https://stackoverflow.com/a/57677370/1913361
# 2. https://stackoverflow.com/a/28321717/1913361
# 3. https://stackoverflow.com/a/37243211/1913361
# TODO: integrate solutions into one decorator or context manager
class SuppressStream(object):
    def __init__(self, file=None, stream=sys.stderr):
        self.orig_stream_fileno = stream.fileno()
        self.file = file

    def __enter__(self):
        self.orig_stream_dup = os.dup(self.orig_stream_fileno)
        self.devnull = open(os.devnull, 'w')
        os.dup2(self.devnull.fileno(), self.orig_stream_fileno)

    def __exit__(self, type, value, traceback):
        os.close(self.orig_stream_fileno)
        os.dup2(self.orig_stream_dup, self.orig_stream_fileno)
        os.close(self.orig_stream_dup)
        self.devnull.close()

    def write(self, x):
        # Avoid print() second call (useless \n)
        if len(x.rstrip()) > 0:
            tqdm.write(x, file=self.file)


@contextlib.contextmanager
def supress_stdout():
    save_stdout = sys.stdout
    sys.stdout = SuppressStream(sys.stdout)
    yield
    sys.stdout = save_stdout
# ------


# %%
# @supress_stdout
def slope(dem, projection, transform):
    """
    Produces a raster of slope.

    :param dem: Digital Elevation Model (DEM) as array
    :type dem: numpy.ndarray
    :param projection: Projection of the DEM
    :type projection: str
    :param transform: Affine transformation of the DEM
    :type transform: tuple
    :return: Slope image as array
    :rtype: numpy.ndarray

    This function calculates the slope of a Digital Elevation Model (DEM) using the richdem library. The slope is 
    calculated using the slope_riserun attribute of the TerrainAttribute function. The DEM is first converted to a 
    richdem.rdarray object, and then the slope is calculated using the TerrainAttribute function. The resulting slope 
    image is returned as a numpy.ndarray.
    """
    import richdem as rd
    
    with SuppressStream():
        rd_dem = rd.rdarray(dem, no_data=-9999)
        rd_dem.projection = projection
        rd_dem.geotransform = transform
        slope = rd.TerrainAttribute(rd_dem, attrib='slope_riserun')

    return np.array(slope)


# @supress_stdout
def aspect(dem, projection, transform):
    """
    Produces a raster of aspect.

    :param dem: Digital Elevation Model (DEM) as array
    :type dem: numpy.ndarray
    :param projection: Projection of the DEM
    :type projection: str
    :param transform: Affine transformation of the DEM
    :type transform: tuple
    :return: Aspect image as array
    :rtype: numpy.ndarray

    This function calculates the aspect of a Digital Elevation Model (DEM) using the richdem library. The aspect is 
    calculated using the aspect attribute of the TerrainAttribute function. The DEM is first converted to a 
    richdem.rdarray object, and then the aspect is calculated using the TerrainAttribute function. The resulting aspect 
    image is returned as a numpy.ndarray.
    """
    import richdem as rd

    # Convert ndarray to richdem.rdarray
    with SuppressStream():
        rd_dem = rd.rdarray(dem, no_data=-9999)
        rd_dem.projection = projection
        rd_dem.geotransform = transform
        aspect = rd.TerrainAttribute(rd_dem, attrib='aspect')

    return np.array(aspect)


def flow_accumulation(dem, res):
    """
    Produces a raster of flow accumulation.

    :param dem: Digital Elevation Model (DEM) as array
    :type dem: numpy.ndarray
    :param res: Spatial resolution of Digital Elevation Model (DEM)
    :type res: numeric
    :return: Flow accumulation image as array
    :rtype: numpy.ndarray

    This function calculates the flow accumulation of a Digital Elevation Model (DEM) using the pysheds library. The DEM is 
    first converted to a pysheds.view.Raster object, and then the flow accumulation is calculated using the Grid object. 
    The resulting flow accumulation image is returned as a numpy.ndarray.
    """
    from pysheds.grid import Grid
    from pysheds.view import Raster, ViewFinder

    # Convert ndarray to pysheds.view.Raster
    r_dem = Raster(dem, viewfinder=ViewFinder(shape=dem.shape))
    grid = Grid.from_raster(r_dem)
    inflated_dem = grid.resolve_flats(r_dem)
    flow_direction = grid.flowdir(inflated_dem)
    flow_accumulation = grid.accumulation(flow_direction)
    return np.array(flow_accumulation)


# %%
def classify_slope_position(tpi, slope):
    """Classifies an image of normalized Topograhic Position Index into 6 slope
    position classes:

    =======  ============
    Slope #  Description
    =======  ============
    1        Valley
    2        Lower Slope
    3        Flat Slope
    4        Middle Slope
    5        Upper Slope
    6        Ridge
    =======  ============

    Classification following Weiss, A. 2001. "Topographic Position and
    Landforms Analysis." Poster presentation, ESRI User Conference, San Diego,
    CA.  http://www.jennessent.com/downloads/tpi-poster-tnc_18x22.pdf

    :param tpi: Topographic Position Index (TPI) as array
    :type tpi: numpy.ndarray
    :param slope: Slope of terrain, in degrees
    :type slope: numpy.ndarray
    :return: Slope position image as array
    :rtype: numpy.ndarray
    """
    assert tpi.shape == slope.shape
    pos = np.empty(tpi.shape, dtype=int)

    pos[(tpi <= -1)] = 1
    pos[(tpi > -1)*(tpi < -0.5)] = 2
    pos[(tpi > -0.5)*(tpi < 0.5)*(slope <= 5)] = 3
    pos[(tpi > -0.5)*(tpi < 0.5)*(slope > 5)] = 4
    pos[(tpi > 0.5)*(tpi <= 1.0)] = 5
    pos[(tpi > 1)] = 6

    return pos


def classify_landform(tpi_near, tpi_far, slope):
    """Classifies a landscape into 10 landforms given "near" and "far" values
    of Topographic Position Index (TPI) and a slope raster.

    ==========  ======================================
    Landform #   Description
    ==========  ======================================
    1           canyons, deeply-incised streams
    2           midslope drainages, shallow valleys
    3           upland drainages, headwaters
    4           U-shape valleys
    5           plains
    6           open slopes
    7           upper slopes, mesas
    8           local ridges, hills in valleys
    9           midslope ridges, small hills in plains
    10          mountain tops, high ridges
    ==========  ======================================

    Classification following Weiss, A. 2001. "Topographic Position and
    Landforms Analysis." Poster presentation, ESRI User Conference, San Diego,
    CA.  http://www.jennessent.com/downloads/tpi-poster-tnc_18x22.pdf

    :param tpi_near: TPI values calculated using a smaller neighborhood,
        assumed to be normalized to have mean = 0 and standard deviation = 1
    :type tpi_near: numpy.ndarray
    :param tpi_far: TPI values calculated using a smaller neighborhood,
        assumed to be normalized to have mean = 0 and standard deviation = 1
    :type tpi_far: numpy.ndarray
    :param slope: Slope of terrain, in degrees
    :type slope: numpy.ndarray
    :return: Landform image as array
    :rtype: numpy.ndarray
    """
    assert tpi_near.shape == tpi_far.shape == slope.shape
    lf = np.empty(tpi_near.shape, dtype=int)

    lf[(tpi_near < 1)*(tpi_near > -1)*(tpi_far < 1)
        * (tpi_far > -1)*(slope <= 5)] = 5
    lf[(tpi_near < 1)*(tpi_near > -1)*(tpi_far < 1)
        * (tpi_far > -1)*(slope > 5)] = 6
    lf[(tpi_near < 1)*(tpi_near > -1)*(tpi_far >= 1)] = 7
    lf[(tpi_near < 1)*(tpi_near > -1)*(tpi_far <= -1)] = 4
    lf[(tpi_near <= -1)*(tpi_far < 1)*(tpi_far > -1)] = 2
    lf[(tpi_near >= 1)*(tpi_far < 1)*(tpi_far > -1)] = 9
    lf[(tpi_near <= -1)*(tpi_far >= 1)] = 3
    lf[(tpi_near <= -1)*(tpi_far <= -1)] = 1
    lf[(tpi_near >= 1)*(tpi_far >= 1)] = 10
    lf[(tpi_near >= 1)*(tpi_far <= -1)] = 8

    return lf


def center_crop_array(new_size, array):
    xpad, ypad = (np.subtract(array.shape, new_size)/2).astype(int)
    dx, dy = np.subtract(new_size, array[xpad:-xpad, ypad:-ypad].shape)
    return array[xpad:-xpad+dx, ypad:-ypad+dy]


# %%
def fetch_metadata(filename, bands, res, out_dir='.'):
    """Fetch DEM metadata from the 3Dep web service and write to disk.
        # id
        # resolution
        # properties
        # - bands
        # - datetime start (in seconds)
        # crs
        # transform
        # bounds
        # license for each collection

    :param filename: Name of the metadata file to write.
    :type filename: str
    :param bands: Dictionary of band names and descriptions.
    :type bands: dict
    :param res: Spatial resolution of the DEM.
    :type res: int or float
    :param out_dir: Directory to write the metadata file to, by default '.'.
    :type out_dir: str, optional
    :return: True if metadata file was successfully written to disk.
    :rtype: bool
    """ 
    import requests
    import json
    import calendar
    from datetime import datetime

    month_name = {month: index for index,
                  month in enumerate(calendar.month_name) if month}

    URL = 'https://elevation.nationalmap.gov/arcgis/'\
          'rest/services/3DEPElevation/ImageServer?f=pjson'
    r = requests.get(URL)
    src_metadata = r.json()

    metadata = {}
    for key in src_metadata.keys():
        if key in ['currentVersion', 'description', 'copyrightText']:
            metadata[key] = src_metadata[key]

    m, d, y = src_metadata['copyrightText']\
        .replace(',', '')\
        .replace('.', '')\
        .split(' ')[-3:]
    timestamp = datetime(int(y), month_name[m], int(d)).timestamp()

    _bands = [{'id': key, 'name': bands[key]} for key in bands.keys()]

    metadata.update(
        # id=image_id,
        name=' '.join(src_metadata['copyrightText'].split(' ')[:-3]),
        resolution=res,
        bands=_bands,
        properties={
            'system:time_start': int(timestamp * 1000),
        }
    )

    with open(os.path.join(out_dir, filename), "w") as f:
        f.write(json.dumps(metadata, indent=4))

    return True


def fetch_dems(
    cell_id: str,
    geom: gpd.GeoSeries,
    state: str,
    out_dir: str or Path,
    res:int=10,
    padding:int=1e3,
    overwrite:bool=False,
    progressbar:bool=False
):
    """Fetch DEMs for a given cell_id and geometry.

    :param cell_id: The cell ID of the geometry.
    :type cell_id: str
    :param geom: The geometry to fetch DEMs for.
    :type geom: shapely.geometry.Polygon
    :param state: The state to fetch DEMs for.
    :type state: str
    :param out_dir: The directory to write the DEMs to.
    :type out_dir: str
    :param res: The spatial resolution of the DEMs, by default 10.
    :type res: int or float, optional
    :param padding: The amount of padding to add to the geometry, by default 1e3.
    :type padding: int or float, optional
    :param overwrite: Whether to overwrite existing DEMs, by default False.
    :type overwrite: bool, optional
    :param progressbar: Whether to display a progress bar, by default False.
    :type progressbar: bool, optional
    :return: None
    :rtype: None
    """

    PROFILE = {
        'driver': 'GTiff',
        'interleave': 'band',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'compress': 'lzw',
        'nodata': -9999,
        'dtype': rasterio.float32,
        # 'count': 5 # set number of bands
    }

    itemdir = f'{cell_id}_{state}_3DEP_{res}mDEM'
    filename = f'{itemdir}-cog.tif'
    outfile = out_dir / filename

    if (not os.path.exists(outfile)) or overwrite:
        print_message(f"Processing file {itemdir}")

        # We want to request the data in a planar coordinate system
        # so we can calculate topographic metrics. This is to avoid
        # distortions due to the curvature of the earth.
        # See discussion https://gis.stackexchange.com/q/7906/72937
        p_crs = infer_utm(geom.bounds)
        p_geom = gpd.GeoSeries(geom, crs=4326).to_crs(p_crs)

        if padding > 0:
            p_geom = p_geom.buffer(padding, join_style=2)

        p_bbox = p_geom[0].bounds
        p_width = np.ceil((p_bbox[2]-p_bbox[0])/res).astype(int)
        p_height = np.ceil((p_bbox[-1]-p_bbox[1])/res).astype(int)
        p_trf = transform.from_bounds(
            *p_bbox, p_width, p_height)  # type: ignore

        # Extend the AOI with a buffer to avoid edge effects
        # when calculating topographic metrics
        buffer_size = int(round((p_width)//2/100))*100
        p_buffer = p_geom.buffer(buffer_size * res, join_style=2)
        bbox_buff = p_buffer.bounds.values[0]

        # Fetch DEM and apply a smoothing filter to mitigate stitching/edge artifacts
        try:
            dem = quad_fetch(bbox=bbox_buff, dim=3, res=res,
                            crs=p_crs.to_epsg(), noData=-9999)
        except Exception as e:
            print_message(f"Failed to fetch {itemdir}. Exception raised: {e}")
            return
        dem = filters.gaussian(dem, 3)

        # We'll need this to transform the data back to the original CRS
        crs = CRS.from_epsg(4326)
        bbox = p_geom.to_crs(crs).bounds.values[0]
        width = np.ceil(degrees_to_meters(bbox[2]-bbox[0])/res)
        height = np.ceil(degrees_to_meters(bbox[-1]-bbox[1])/res)
        trf = transform.from_bounds(*bbox, width, height)  # type: ignore

        # ---
        try:
            bands = [
                dem,
                slope(dem, p_crs, p_trf),
                aspect(dem, p_crs, p_trf),
                flow_accumulation(dem, res),
                tpi(dem, irad=15, orad=30, res=res),
                tpi(dem, irad=185, orad=200, res=res),
            ]
            bands.append(
                classify_slope_position(bands[3], bands[0])
            )
            bands.append(
                classify_landform(bands[4], bands[3], bands[0])
            )
        except Exception as e:
            print_message(f"Failed to calculate topographic metrics for {itemdir}. Exception raised: {e}")
            return
        
        # Remove buffer
        bands = [center_crop_array((p_height, p_width), x) for x in bands]
        # ---

        band_info = {
            'dem': 'Digital Elevation Model',
            'slope': 'Slope',
            'aspect': 'Aspect',
            'flowacc': 'Flow Accumulation',
            'tpi300': 'Topographic Position Index (300m)',
            'tpi2000': 'Topographic Position Index (2000m)',
            'spc300': 'Slope Position Class (300m)',
            'landform': 'Landform Class',
        }

        # fetch_metadata(itemdir, band_info, res, out_dir / itemdir)

        # Reproject, generate cog, and write the data to disk
        PROFILE.update(crs=crs, transform=trf, width=width,
                       height=height, count=len(bands))
        cog_profile = cog_profiles.get("deflate")

        with MemoryFile() as memfile:
            with memfile.open(**PROFILE) as dst:
                dst_idx = 1
                for band, data in zip(band_info.keys(), bands):
                    output = np.zeros(dst.shape, rasterio.float32)
                    reproject(
                        source=data,
                        destination=output,
                        src_transform=p_trf,
                        src_crs=p_crs,
                        dst_transform=trf,
                        dst_crs=crs,
                        resampling=Resampling.nearest
                    )
                    dst.write(output, dst_idx)
                    dst.set_band_description(dst_idx, band_info[band])

                    # Select band to generate preview
                    if dst_idx == 1:
                        cm = plt.get_cmap('gist_earth')
                        norm_out = cm(output / output.max())[:, :, :3] * 255
                        preview = Image.fromarray(
                            norm_out.astype(np.uint8)).convert('RGB')
                        preview.save(
                            out_dir / filename.replace('-cog.tif', '-preview.png'))

                    dst_idx += 1

                try:
                    fetch_metadata(filename.replace('-cog.tif', '-metadata.json'), band_info, res, out_path)
                except Exception as e:
                    print_message(f"Failed to write metadata for {itemdir}. Exception raised: {e}")
                    return

                cog_translate(
                    dst,
                    outfile,
                    cog_profile,
                    in_memory=True,
                    quiet=True
                )

    else:
        print_message(f"File {itemdir} already exists, skipping...")


# %%
if __name__ == '__main__':

    run_as = 'prod'
    res = 10
    conf = ConfigLoader(Path(__file__).parent.parent).load()

    if run_as == 'dev':
        PROJDATADIR = Path(conf.DEV_PROJDATADIR)
        GRID = Path(conf.GRID)
        threads = 4
    elif run_as == 'prod':
        PROJDATADIR = Path(conf.PROJDATADIR)
        GRID = Path(conf.GRID)
        threads = 20

    out_path = PROJDATADIR / '3dep'
    out_path.mkdir(exist_ok=True, parents=True)
    # bands = conf['items']['3dep']['bands']
    # fetch_metadata(bands, res, out_path)

    # Fetch data only for labels cellids
    labels = image_collection(PROJDATADIR, file_pattern='*.geojson')
    cellids = [int(Path(x).name.split('_')[0]) for x in labels]

    gdf = gpd.read_file(GRID)
    gdf['PRIMARY_STATE'] = gdf.PRIMARY_STATE.apply(lambda x: x.upper()[:2])
    # cellids = [264178,248505,241042,173189]
    # gdf = gdf[gdf.CELL_ID.isin(cellids)]

    # Filter out tiles without metadata
    # all_cellids = [int(Path(x).name.split('_')[0]) for x in image_collection(out_path)]
    # mtd_files = image_collection(out_path, file_pattern='*-metadata.json')
    # mtd_cellids = [int(Path(x).name.split('_')[0]) for x in mtd_files]
    # setdiff = set(all_cellids) - set(mtd_cellids)
    # gdf = gdf[gdf.CELL_ID.isin(setdiff)]

    params = [
        {
            'cell_id': row.CELL_ID,
            'geom': row.geometry,
            'state': row.PRIMARY_STATE,
            'padding': 0,
            'out_dir': out_path,
            'res': res,
            'overwrite': True
        } for row in gdf.itertuples()
    ]

    # fetch_dems(**params[0])
    multithreaded_execution(fetch_dems, params, threads)
