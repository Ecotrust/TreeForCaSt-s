# %%
from datetime import datetime
import json
import geopandas as gpd
from pathlib import Path
import rasterio
from rasterio import transform
from rasterio import MemoryFile
from rasterio.warp import reproject, Resampling
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from rasterio import windows
import numpy as np
from pyproj import CRS
from PIL import Image
from shapely.geometry import mapping

from gdstools import image_collection, degrees_to_meters

# %%
STATE = 'WA'
PROJDATADIR = Path('/mnt/data/FESDataRepo/stac_stands/processed')

def center_crop_array(new_size, array):
    xpad, ypad = (np.subtract(array.shape, new_size)/2).astype(int)
    dx, dy = np.subtract(new_size, array[xpad:-xpad, ypad:-ypad].shape)
    return array[xpad:-xpad+dx, ypad:-ypad+dy]

crs2927 = CRS.from_epsg(2927)
crs4326 = CRS.from_epsg(4326)
if STATE == 'WA':
    YEAR = '2021'
    tindex = gpd.read_file('/mnt/data/FESDataRepo/raw/noaa_naip/tileindex_WA_NAIP_2021.shp').to_crs(crs2927)
    grid = gpd.read_file('/mnt/data/FESDataRepo/stac_stands/interim/usgs_grid/USGS_CellGrid_3_75Minute_WA_epsg4326.geojson').to_crs(crs2927)
    print('Fetching NAIP imagery for Washington St')
elif STATE == 'OR':
    YEAR = '2020'
    tindex = gpd.read_file('/mnt/data/FESDataRepo/raw/noaa_naip/tile_index_OR_NAIP_2020_9504.shp').to_crs(crs2927)
    grid = gpd.read_file('/mnt/data/FESDataRepo/stac_stands/interim/usgs_grid/USGS_CellGrid_3_75Minute_OR_epsg4326.geojson').to_crs(crs2927)
    print('Fetching NAIP imagery for Oregon')

# Get the intersection of the two maps
tindex.columns = [c.upper() for c in tindex.columns if c != 'geometry'] + ['geometry']
grid_tidx = gpd.overlay(grid, tindex, how='intersection')
grid_tidx['area'] = grid_tidx.geometry.area
grid_tidx = grid_tidx.sort_values(['CELL_ID', 'area']).groupby('CELL_ID').last().reset_index()
grid = grid.merge(grid_tidx[['CELL_ID', 'URL']], on='CELL_ID')
grid = grid.to_crs(crs4326)

labels = image_collection(PROJDATADIR / "labels", file_pattern='*.geojson')
cellids = [int(Path(x).name.split('_')[0]) for x in labels]

# Skip downloaded naip
downloaded = image_collection(PROJDATADIR / f"naip/{YEAR}", file_pattern='*.tif')
downloaded_cellids = [int(Path(x).name.split('_')[0]) for x in downloaded]
cellids = list(set(cellids) - set(downloaded_cellids))

grid = grid[grid.CELL_ID.isin(cellids)]

cog_profile = cog_profiles.get("deflate")

for i, row in grid.iterrows():

    outfile = PROJDATADIR / f"naip/{YEAR}" / f"{row.CELL_ID}_{YEAR}_{STATE}_NAIP_NOAA-cog.tif"
    outfile.parent.mkdir(parents=True, exist_ok=True)

    geom = grid[grid.index == i].geometry.values[0]
    bbox = row.geometry.bounds

    print('Fetching', row.CELL_ID, 'from', row.URL)
    with rasterio.open(row.URL) as src:
        geom = gpd.GeoSeries(geom, crs=4326).to_crs(src.crs)
        xmin, ymin, xmax, ymax = geom[0].bounds
        src_w = src.shape[1]
        src_h = src.shape[0]

        prop = src_h / src_w
        p_width = ((xmax - xmin) / src.res[1])
        p_height = ((ymax - ymin) / src.res[0])

        col_off = (src_w - p_width) / 2
        row_off = (src_h - p_height) / 2

        src_transform = transform.from_bounds(xmin, ymin, xmax, ymax, p_width, p_height)
        window = windows.Window(col_off, row_off, p_width, p_height) 
        data = src.read(window=window)

        width = int(np.ceil(degrees_to_meters(bbox[2]-bbox[0])/src.res[0]))
        height = int(np.ceil(degrees_to_meters(bbox[-1]-bbox[1])/src.res[1]))

        dst_transform = transform.from_bounds(*bbox, width, height)

        PROFILE = {
            'driver': 'GTiff',
            'interleave': 'band',
            'tiled': True,
            'crs': crs4326,
            'transform': dst_transform,
            'width': width,
            'height': height,
            'blockxsize': 256,
            'blockysize': 256,
            'compress': 'lzw',
            'count': src.count,
            'dtype': rasterio.uint8,
        }

        print('Writing', outfile)
        with MemoryFile() as memfile:
            with memfile.open(**PROFILE) as dst:
                output = np.zeros((src.count, height, width), rasterio.uint8)
                reproject(
                    source=data,#np.array(bands),
                    destination=output,#rasterio.band(dst, i+1),
                    src_transform=src_transform,#.window_transform(window),
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=rasterio.crs.CRS.from_epsg(4326),
                    resampling=Resampling.bilinear
                )
                dst.write(output)

                cog_translate(
                    dst,
                    outfile,
                    cog_profile,
                    in_memory=True,
                    quiet=True
                )


                # Generate and save preview
                print('Writing preview ...')
                preview = Image.fromarray(
                    np.moveaxis(output[:3], 0, -1)).convert('RGB')
                h, w = preview.size
                # Change preview res to 30m
                new_w = int(w * src.res[0] / 30)
                new_h = int(h * src.res[1] / 30)
                preview = preview.resize((new_w, new_h))
                preview.save(outfile.parent / outfile.name.replace('-cog.tif',
                            '-preview.png'), optimize=True)


                # Generate and save metadata
                print('Writing metadata ...')
                xoff, yoff = (dst_transform.xoff, dst_transform.yoff)
                coordinates = mapping(row.geometry)['coordinates'][0][0]
                date = datetime.strptime(
                    row.URL.split('_')[-1].replace('.tif',''), "%Y%m%d")
                unixdate = int(datetime.timestamp(date)*1000)

                metadata = {
                    "type": "ImageCollection",
                    "bands": [
                        {
                        "id": "R",
                        "data_type": {
                            "type": "PixelType",
                            "precision": "double",
                            "min": 0,
                            "max": 255
                        },
                        "dimensions": [1,1],
                        "origin": [xoff, yoff],
                        "crs": "EPSG:4326",
                        "crs_transform": [1,0,0,0,1,0]
                        },
                        {
                        "id": "G",
                        "data_type": {
                            "type": "PixelType",
                            "precision": "double",
                            "min": 0,
                            "max": 255
                        },
                        "dimensions": [1,1],
                        "origin": [xoff, yoff],
                        "crs": "EPSG:4326",
                        "crs_transform": [1,0,0,0,1,0]
                        },
                        {
                        "id": "B",
                        "data_type": {
                            "type": "PixelType",
                            "precision": "double",
                            "min": 0,
                            "max": 255
                        },
                        "dimensions": [1,1],
                        "origin": [xoff, yoff],
                        "crs": "EPSG:4326",
                        "crs_transform": [1,0,0,0,1,0]
                        },
                        {
                        "id": "N",
                        "data_type": {
                            "type": "PixelType",
                            "precision": "double",
                            "min": 0,
                            "max": 255
                        },
                        "dimensions": [1,1],
                        "origin": [xoff, yoff],
                        "crs": "EPSG:4326",
                        "crs_transform": [1,0,0,0,1,0]
                        }
                    ],
                    "properties": {
                        "system:footprint": {
                        "geodesic": 'false',
                        "type": "Polygon",
                        "coordinates": [coordinates]
                        },
                        "system:time_start": unixdate,
                        "system:time_end": unixdate,
                        "description": "<p>The National Agriculture Imagery Program (NAIP) acquires aerial imagery\nduring the agricultural growing seasons in the continental U.S.</p><p>NAIP projects are contracted each year based upon available funding and the\nimagery acquisition cycle. Beginning in 2003, NAIP was acquired on\na 5-year cycle. 2008 was a transition year, and a three-year cycle began\nin 2009.</p><p>NAIP imagery is acquired at a one-meter ground sample distance (GSD) with a\nhorizontal accuracy that matches within six meters of photo-identifiable\nground control points, which are used during image inspection.</p><p>Older images were collected using 3 bands (Red, Green, and Blue: RGB), but\nnewer imagery is usually collected with an additional near-infrared band\n(RGBN). RGB asset ids begin with &#39;n<em>&#39;, NRG asset ids begin with &#39;c</em>&#39;, RGBN\nasset ids begin with &#39;m_&#39;.</p><p><b>Provider: <a href=\"https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/\">USDA Farm Production and Conservation - Business Center, Geospatial Enterprise Operations</a></b><br><p><b>Resolution</b><br>1 meter\n</p><p><b>Bands</b><table class=\"eecat\"><tr><th scope=\"col\">Name</th><th scope=\"col\">Description</th></tr><tr><td>R</td><td><p>Red</p></td></tr><tr><td>G</td><td><p>Green</p></td></tr><tr><td>B</td><td><p>Blue</p></td></tr><tr><td>N</td><td><p>Near infrared</p></td></tr></table><p><b>Terms of Use</b><br><p>Most information presented on the FSA Web site is considered public domain\ninformation. Public domain information may be freely distributed or copied,\nbut use of appropriate byline/photo/image credits is requested. For more\ninformation visit the <a href=\"https://www.fsa.usda.gov/help/policies-and-links\">FSA Policies and Links</a>\nwebsite.</p><p>Users should acknowledge USDA Farm Production and Conservation -\nBusiness Center, Geospatial Enterprise Operations when using or\ndistributing this data set.</p><p><b>Suggested citation(s)</b><ul><li><p>USDA Farm Production and Conservation - Business Center, Geospatial Enterprise Operations</p></li></ul><style>\n  table.eecat {\n  border: 1px solid black;\n  border-collapse: collapse;\n  font-size: 13px;\n  }\n  table.eecat td, tr, th {\n  text-align: left; vertical-align: top;\n  border: 1px solid gray; padding: 3px;\n  }\n  td.nobreak { white-space: nowrap; }\n</style>"
                    },
                    "id": "image"
                }

                with open(outfile.parent / outfile.name.replace('-cog.tif', '-metadata.json'), 'w') as f:
                    json.dump(metadata, f, indent=2)
