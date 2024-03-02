""" 
Fetch LandTrendr data from Google Earth Engine (GEE) for each tile in the USGS 7.5 min grid.
"""
# %%
import os
from pathlib import Path
import geopandas as gpd
import ee
from typing import Union

from gdstools import (
    create_directory_tree,
    GEEImageLoader, 
    get_landsat_collection, 
    multithreaded_execution,
    image_collection,
    ConfigLoader,
)


def parse_landtrendr_result(
    lt_result, current_year, flip_disturbance=False, big_fast=False, sieve=False
    ):
    """Parses a LandTrendr segmentation result, returning an image that
    identifies the years since the largest disturbance.

    :param lt_result: result of running ee.Algorithms.TemporalSegmentation.LandTrendr on an image collection
    :type lt_result: ee.Image
    :param current_year: used to calculate years since disturbance
    :type current_year: int
    :param flip_disturbance: whether to flip the sign of the change in spectral change so that disturbances are indicated by increasing reflectance
    :type flip_disturbance: bool
    :param big_fast: consider only big and fast disturbances
    :type big_fast: bool
    :param sieve: filter out disturbances that did not affect more than 11 connected pixels in the year of disturbance
    :type sieve: bool
    :return: an image with four bands:
        ysd - years since largest spectral change detected
        mag - magnitude of the change
        dur - duration of the change
        rate - rate of change
    :rtype: ee.Image
    """
    lt = lt_result.select("LandTrendr")
    is_vertex = lt.arraySlice(0, 3, 4)  # 'Is Vertex' row - yes(1)/no(0)
    verts = lt.arrayMask(is_vertex)  # vertices as boolean mask

    left, right = verts.arraySlice(1, 0, -1), verts.arraySlice(1, 1, None)
    start_yr, end_yr = left.arraySlice(0, 0, 1), right.arraySlice(0, 0, 1)
    start_val, end_val = left.arraySlice(0, 2, 3), right.arraySlice(0, 2, 3)

    ysd = start_yr.subtract(current_year - 1).multiply(-1)  # time since vertex
    dur = end_yr.subtract(start_yr)  # duration of change
    if flip_disturbance:
        mag = end_val.subtract(start_val).multiply(-1)  # magnitude of change
    else:
        mag = end_val.subtract(start_val)

    rate = mag.divide(dur)  # rate of change

    # combine segments in the timeseries
    seg_info = ee.Image.cat([ysd, mag, dur, rate]).toArray(0).mask(is_vertex.mask())

    # sort by magnitude of disturbance
    sort_by_this = seg_info.arraySlice(0, 1, 2).toArray(0)
    seg_info_sorted = seg_info.arraySort(
        sort_by_this.multiply(-1)
    )  # flip to sort in descending order
    biggest_loss = seg_info_sorted.arraySlice(1, 0, 1)

    img = ee.Image.cat(
        biggest_loss.arraySlice(0, 0, 1).arrayProject([1]).arrayFlatten([["ysd"]]),
        biggest_loss.arraySlice(0, 1, 2).arrayProject([1]).arrayFlatten([["mag"]]),
        biggest_loss.arraySlice(0, 2, 3).arrayProject([1]).arrayFlatten([["dur"]]),
        biggest_loss.arraySlice(0, 3, 4).arrayProject([1]).arrayFlatten([["rate"]]),
    )

    if big_fast:
        # get disturbances larger than 100 and less than 4 years in duration
        dist_mask = img.select(["mag"]).gt(100).And(img.select(["dur"]).lt(4))

        img = img.mask(dist_mask)

    if sieve:
        MAX_SIZE = 128  #  maximum map unit size in pixels
        # group adjacent pixels with disturbance in same year
        # create a mask identifying clumps larger than 11 pixels
        mmu_patches = (
            img.int16().select(["ysd"]).connectedPixelCount(MAX_SIZE, True).gte(11)
        )

        img = img.updateMask(mmu_patches)

    return img.round().toShort()


# %%
def get_landtrendr(
        bbox: tuple, 
        year: int, 
        out_path: Union[str, Path], 
        prefix:str=None, 
        epsg:int=4326, 
        scale:int=30, 
        overwrite:bool=False
    ):
    """Fetch LandTrendr data

    :param bbox: bounding box of the tile
    :type bbox: list
    :param year: year to fetch data for
    :type year: int
    :param out_path: path to save the data
    :type out_path: str
    :param prefix: prefix to add to the filename, defaults to None
    :type prefix: str, optional
    :param epsg: EPSG code of the projection, defaults to 4326
    :type epsg: int, optional
    :param scale: scale of the image, defaults to 30
    :type scale: int, optional
    :param overwrite: whether to overwrite existing files, defaults to False
    :type overwrite: bool, optional
    :return: None
    :rtype: None
    """ 
    # %%
    filename = f"{prefix}LandTrendr_8B_SWIR1-NBR_{year}"
    if os.path.exists(out_path / f'{filename}-cog.tif') & (not overwrite):
        print(f"File {out_path} already exists. Skipping.")
        return

    aoi = ee.Geometry.Rectangle(bbox, proj=f"EPSG:{epsg}", evenOdd=True, geodesic=False)
    swir_coll = get_landsat_collection(aoi, 1984, year, band="SWIR1")
    nbr_coll = get_landsat_collection(aoi, 1984, year, band="NBR")

    LT_PARAMS = {
        "maxSegments": 6,
        "spikeThreshold": 0.9,
        "vertexCountOvershoot": 3,
        "preventOneYearRecovery": True,
        "recoveryThreshold": 0.25,
        "pvalThreshold": 0.05,
        "bestModelProportion": 0.75,
        "minObservationsNeeded": 6,
    }

    swir_result = ee.Algorithms.TemporalSegmentation.LandTrendr(swir_coll, **LT_PARAMS)
    nbr_result = ee.Algorithms.TemporalSegmentation.LandTrendr(nbr_coll, **LT_PARAMS)

    swir_img = parse_landtrendr_result(swir_result, year).set(
        "system:time_start", swir_coll.first().get("system:time_start")
    )
    nbr_img = parse_landtrendr_result(nbr_result, year, flip_disturbance=True).set(
        "system:time_start", nbr_coll.first().get("system:time_start")
    )

    lt_img = ee.Image.cat(
        swir_img.select(["ysd"], ["ysd_swir1"]),
        swir_img.select(["mag"], ["mag_swir1"]),
        swir_img.select(["dur"], ["dur_swir1"]),
        swir_img.select(["rate"], ["rate_swir1"]),
        nbr_img.select(["ysd"], ["ysd_nbr"]),
        nbr_img.select(["mag"], ["mag_nbr"]),
        nbr_img.select(["dur"], ["dur_nbr"]),
        nbr_img.select(["rate"], ["rate_nbr"]),
    ).set("system:time_start", swir_img.get("system:time_start"))

    try:
        image = GEEImageLoader(lt_img.clip(aoi))
    except Exception as e:
        print(f"Loading image {filename} failed. Exception raised: {e}")
        return
    
    # Set image metadata and params
    image.metadata_from_collection(nbr_coll)
    image.set_params("scale", scale)
    image.set_params("crs", f"EPSG:{epsg}")
    image.set_viz_params("min", 200)
    image.set_viz_params("max", 800)
    image.set_viz_params("bands", ["ysd_swir1"])
    image.set_viz_params(
        "palette",
        [
            "#9400D3",
            "#4B0082",
            "#0000FF",
            "#00FF00",
            "#FFFF00",
            "#FF7F00",
            "#FF0000",
        ],
    )

    image.id = filename

    image.to_geotif(out_path)
    image.save_preview(out_path)
    image.save_metadata(out_path)

    return

# %%
if __name__ == "__main__":
    
    run_as = "prod"
    conf = ConfigLoader(Path(__file__).parent.parent).load()
    ltr_api = conf['items']['landtrendr']['providers']['Google']['api']
    qq_shp = gpd.read_file(conf.GRID)

    if run_as == "dev":
        PROJDATADIR = Path(conf.DEV_PROJDATADIR)
        WORKERS = 4
    elif run_as == "prod":
        PROJDATADIR = Path(conf.PROJDATADIR) / 'processed'
        WORKERS = 20
    
    # Initialize the Earth Engine module.
    # Setup your Google Earth Engine API key before running this script.
    # %%
    ee.Initialize(opt_url=ltr_api)

    # %%
    # Load the QQ grid shapefile. Fetch data only for labels cellids
    labels = image_collection(PROJDATADIR / "labels", file_pattern='*.geojson')
    cellids = [int(Path(x).name.split('_')[0]) for x in labels]
    years = set([int(Path(x).name.split('_')[1]) for x in labels])

    qq_shp['STATE'] = qq_shp.PRIMARY_STATE.apply(lambda x: x.upper()[:2])
    qq_shp = qq_shp[qq_shp.CELL_ID.isin(cellids)]

    # Overwrite years 
    years = [2021, 2022]

    for year in years:
        ltr_path = create_directory_tree(PROJDATADIR, "landtrendr", str(year))

        params = [
            {
                "bbox": row.geometry.bounds,
                "year": year,
                "out_path": ltr_path,
                "prefix": f"{row.CELL_ID}_{year}_{row.STATE}_",
            }
            for row in qq_shp.itertuples()
        ]

        # %%
        multithreaded_execution(get_landtrendr, params, WORKERS)
