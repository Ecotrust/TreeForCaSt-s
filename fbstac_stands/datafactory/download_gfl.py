# %%
from pathlib import Path
from datetime import datetime
import json

import geopandas as gpd
import ee

from gdstools import (
    create_directory_tree,
    multithreaded_execution, 
    GEEImageLoader,
    ConfigLoader,
    image_collection
)

# %%
def get_gflandsat(
    bbox,
    year,
    out_path,
    returns="image",
    prefix=None,
    season="leafon",
    epsg=4326,
    scale=30,
    overwrite=False,
):
    """
    Fetch Gap-Filled Landsat (GFL) image url from Google Earth Engine (GEE) using a bounding box.

    See https://www.ntsg.umt.edu/project/landsat/landsat-gapfill-reflect.php for more information.

    Parameters
    ----------
    month : int
        Month of year (1-12)
    year : int
        Year (e.g. 2019)
    bbox : list
        Bounding box in the form [xmin, ymin, xmax, ymax].

    Returns
    -------
    url : str
        GEE generated URL from which the raster will be downloaded.
    metadata : dict
        Image metadata.
    """
    # TODO: define method to get collection, then pass an instance of that 
    # collection to the GEEImageLoader to avoid instantiating the collection
    # every time this function is called.

    if season == "leafoff":
        start_date = f"{year - 1}-10-01"
        end_date = f"{year}-03-31"
    elif season == "leafon":
        start_date = f"{year}-04-01"
        end_date = f"{year}-09-30"
    else:
        raise ValueError(f"Invalid season: {season}")

    collection = ee.ImageCollection("projects/KalmanGFwork/GFLandsat_V1").filterDate(
        start_date, end_date
    )

    ts_start = datetime.timestamp(datetime.strptime(start_date, "%Y-%m-%d"))
    ts_end = datetime.timestamp(datetime.strptime(end_date, "%Y-%m-%d"))

    bbox = ee.Geometry.BBox(*bbox)
    image = GEEImageLoader(collection.median().clip(bbox))
    # Set image metadata and params
    image.metadata_from_collection(collection)
    image.set_property("system:time_start", ts_start * 1000)
    image.set_property("system:time_end", ts_end * 1000)
    image.set_params("scale", scale)
    image.set_params("crs", f"EPSG:{epsg}")
    image.set_params("region", bbox)
    image.set_viz_params("min", 0)
    image.set_viz_params("max", 2000)
    image.set_viz_params("bands", ["B3_mean_post", "B2_mean_post", "B1_mean_post"])
    image.id = f"{prefix}_Gap_Filled_Landsat_{season}"

    # Download cog
    # out_path = path / image.id
    # out_path.mkdir(parents=True, exist_ok=True)

    if returns == "metadata":
        return image.metadata
    else:
        image.to_geotif(out_path, overwrite=overwrite)
        image.save_preview(out_path, overwrite=overwrite)
        image.save_metadata(out_path)


if __name__ == "__main__":

    run_as = "prod"
    conf = ConfigLoader(Path(__file__).parent.parent).load()
    api_url = conf['items']['gflandsat']['api']
    
    if run_as == "dev":
        GRID = conf.GRID
        PROJDATADIR = conf.DEV_PROJDATADIR       
        WORKERS = 4
    elif run_as == "prod":
        GRID = conf.GRID
        PROJDATADIR = conf.PROJDATADIR
        WORKERS = 20

    # Initialize the Earth Engine module.
    # Setup your Google Earth Engine API key before running this script.
    # %%
    ee.Initialize(opt_url=api_url)

    # %%
    # Fetch data only for labels cellids
    labels = image_collection(PROJDATADIR, file_pattern='*.geojson')
    cellids = [int(Path(x).name.split('_')[0]) for x in labels]
    years = set([Path(x).name.split('_')[1] for x in labels])

    # Load QQ shapefile
    gdf = gpd.read_file(GRID)
    gdf['STATE'] = gdf.PRIMARY_STATE.apply(lambda x: x.upper()[:2])
    # gdf = gdf[gdf.CELL_ID.isin(cellids)]

    # Overwrite years if needed
    years = [2017, 2018, 2019, 2020, 2021]

    for year in years:

        out_path = create_directory_tree(PROJDATADIR, 'gflandsat', str(year))

        # Fetch collection metadata
        # aoi = gdf.geometry.unary_union.bounds

        # try:
        #     metadata = get_gflandsat(aoi, year=year, out_path=out_path, returns="metadata")

        # except Exception as e:
        #     print(f"Failed to fetch metadata for {year}: {e}")
        #     continue

        # with open(out_path / f"gflandsat_{year}.json", "w") as f:
        #     json.dump(metadata, f, indent=2)

        # Select a subset of qq cells to play with.
        # qq_shp = qq_shp[qq_shp.CELL_ID.isin(qq_shp.head(20).CELL_ID)].copy()
        params = [
            {
                "bbox": row.geometry.bounds,
                "year": year,
                "out_path": out_path,
                "prefix": f"{row.CELL_ID}_{year}_{row.STATE}",
                "season": "leafon",
                "overwrite": True,
            } for row in gdf.itertuples()
        ]

        multithreaded_execution(get_gflandsat, params, WORKERS)
