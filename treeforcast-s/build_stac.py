"""
Build the STAC catalog for Oregon BLM and Washington St DNR forest stands. 
"""

# %%
import re
import json
from datetime import datetime
from pathlib import Path

from pystac import (
    Extent,
    SpatialExtent,
    TemporalExtent,
    Item,
    Asset,
    MediaType,
    Catalog,
    Collection,
    CatalogType,
    Provider,
    Link,
)
from pystac.extensions.eo import Band, EOExtension
from pystac.extensions.label import LabelExtension, LabelType, LabelClasses
from pystac.extensions.projection import ProjectionExtension

import numpy as np
import geopandas as gpd
import rasterio
from shapely import geometry

from gdstools import ConfigLoader, image_collection, multithreaded_execution


# %%
def bbox_to_json(bbox):
    """
    Generate GeoJSON geometry from bounding box.

    :param bbox: A list of four coordinates representing the bounding box in the order
        [minx, miny, maxx, maxy].
    :type bbox: list
    :return: A GeoJSON geometry representing the bounding box.
    :rtype: dict
    """
    geom = geometry.box(*bbox, ccw=True)
    return json.loads(gpd.GeoSeries(geom).to_json())


# %%
def create_label_item(label_path: str, attr_dict: dict, crs="EPSG:4326"):
    """
    Create a label item.

    :param label_path: The path to label geojson file.
    :type label_path: str
    :param attr_dict: Dictionary with attributes for the label item. Expected attributes:
        label_properties, label_date, label_description, label_type, label_tasks,
        and label_name.
    :type attr_dict: dict
    :param crs: Coordinate Reference System (CRS) of the label data, defaults to 'EPSG:4326'.
    :type crs: str, optional
    :return: A PySTAC Item representing the label data.
    :rtype: pystac.Item
    """
    # Read label data
    label_path = Path(label_path)
    label_id = label_path.stem.replace("-cog", "")
    label_data = gpd.read_file(label_path)
    bbox = label_data.total_bounds.tolist()

    if attr_dict["label_task"] in ["classification", "segmentation"]:
        label_classes = [
            LabelClasses.create(
                classes=attr_dict["label_classes"], name=attr_dict["label_name"]
            )
        ]
    else:
        label_classes = None

    label_data = label_data.to_crs(crs)

    # label_data = json.loads(label_data.to_json())
    import shapely

    geoms = []
    for geom in label_data.geometry:
        if isinstance(geom, shapely.geometry.MultiPolygon):
            geoms.extend(geom.geoms)
        else:
            geoms.append(geom)

    mpoly = geometry.MultiPolygon(geoms)
    mpoly = gpd.GeoSeries(mpoly)
    mpoly = json.loads(mpoly.to_json())

    # Create label item
    label_item = Item(
        id=f"{label_id}",
        geometry=mpoly,
        bbox=bbox,
        datetime=attr_dict["label_date"],
        properties={},
    )

    if attr_dict["label_type"] == "vector":
        label_type = LabelType.VECTOR
    elif attr_dict["label_type"] == "raster":
        label_type = LabelType.RASTER

    label_ext = LabelExtension.ext(label_item, add_if_missing=True)
    label_ext.apply(
        label_description=attr_dict["label_description"],
        label_type=label_type,
        label_properties=attr_dict["label_properties"],
        label_tasks=attr_dict["label_task"],
        label_classes=label_classes,
    )

    # Add link to label data
    idx = label_path.as_posix().split("/").index("stands")
    subdir = "/".join(label_path.as_posix().split("/")[idx:-1])
    url = (
        "https://fbstac-stands.s3.amazonaws.com/stands/data/"
        + subdir
        + "/"
        + label_path.name
    )
    label_ext.add_geojson_labels(href=url)

    return label_item, label_ext


def create_item(
    image_path: str,
    thumb_path: str,
    metadata_path: str,
    asset_path_url: str,
):
    """
    Create a STAC item.

    :param image_path: Path to local COG image.
    :type image_path: str
    :param thumb_path: Path to local thumbnail image.
    :type thumb_path: str
    :param metadata_path: Path to local metadata file.
    :type metadata_path: str
    :param asset_path_url: URL to the asset path. This is the access point where users can download the catalog assets.
    :type asset_path_url: str
    :return: A STAC item.
    :rtype: pystac.Item
    """
    # Load image data
    image_path = Path(image_path)
    thumb_path = Path(thumb_path)
    with rasterio.open(image_path) as src:
        crs = src.crs
        bbox = list(src.bounds)

    # Load metadata
    with open(metadata_path) as f:
        metadata = json.load(f)

    # Collect image properties
    image_date = datetime.utcfromtimestamp(
        metadata["properties"]["system:time_start"] / 1000
    )
    image_id = image_path.stem.replace("-cog", "")

    image_geom = bbox_to_json(bbox)
    image_bands = metadata["bands"]

    # Create item
    item = Item(
        id=image_id,
        geometry=image_geom,
        bbox=bbox,
        datetime=image_date,
        properties={},
    )

    # Add bands and projection
    bands = [Band.create(name=b["id"], common_name=b.get("name")) for b in image_bands]
    eo = EOExtension.ext(item, add_if_missing=True)
    eo.apply(bands=bands)

    proj = ProjectionExtension.ext(item, add_if_missing=True)
    proj.apply(epsg=crs.to_epsg())

    # Add links to assets
    item.add_asset(
        "image", Asset(href=asset_path_url + image_path.name, media_type=MediaType.COG)
    )
    # item.add_asset('metadata', pystac.Asset(href=github_url +
    #                metadata_path[3:], media_type=pystac.MediaType.JSON))
    item.add_asset(
        "thumbnail",
        Asset(href=asset_path_url + thumb_path.name, media_type=MediaType.PNG),
    )

    return item


def paths_to_dict(paths_list, idx):
    """
    Create a dictionary from a list of paths.

    :param paths_list: A list of file paths.
    :type paths_list: list
    :param idx: The index of the path to use for the collection name.
    :type idx: int
    :return: A dictionary with the collection name as the key and a list of file paths as the value.
    :rtype: dict
    """
    _dict = {}
    for p in paths_list:
        plist = p.split("/")
        # expects name in format cellid_year_state_agency/dataset
        nameparts = Path(p).stem.split("_")
        collection = plist[idx + 1]
        year = nameparts[1]
        if not re.match(r"^\d+$", year):
            _dict.setdefault(collection, []).append(p)
        else:
            _dict.setdefault(collection, {}).setdefault(year, []).append(p)
    return _dict


# %%
def build_stac(rootpath: Path, run_as: str = "dev"):
    """
    Build the STAC

    :param conf: A dictionary containing configuration information for the catalog.
    :type conf: Dict[str, Any]
    :param qq_shp: A GeoDataFrame containing the quad boundaries.
    :type qq_shp: gpd.GeoDataFrame
    :return: A STAC Catalog object.
    :rtype: Catalog
    """
    # Load config file
    conf = ConfigLoader(rootpath).load()
    if run_as == "dev":
        GRID = Path(conf.GRID)
        PROJDATADIR = Path(conf.DEV_PROJDATADIR)
    else:
        GRID = conf.GRID
        PROJDATADIR = Path(conf.PROJDATADIR) / "processed"

    # Build catalog
    qq_shp = gpd.read_file(GRID)

    fbench = Catalog(
        id="treeforcast-s",
        description="A Spatio-Temporal Asset Catalog (STAC) for Modeling Forest Composition and Structure in the Pacific Northwest",
        title="TreeForCaSt-s - Modeling Forest Composition and Structure",
    )

    label_paths = image_collection(
        PROJDATADIR / "stands", file_pattern="*.geojson"
    )  # type: ignore
    image_paths = image_collection(PROJDATADIR)

    # Group labels and items by stand-agency and year
    labels_dict = paths_to_dict(label_paths, 6)
    images_dict = paths_to_dict(image_paths, 5)

    items_dict = {}
    for stand, stand_dict in labels_dict.items():

        for year, year_dict in stand_dict.items():
            items_dict.setdefault(stand, {}).setdefault(year, {"stands": year_dict})
            cellids = {Path(p).stem.split("_")[0] for p in stand_dict[year]}

            # add images
            for coll, coll_dict in images_dict.items():

                if coll == "naip":
                    print("Adding naip")

                if isinstance(coll_dict, dict):
                    min_year = min(
                        coll_dict.keys(), key=lambda x: abs(int(x) - int(year))
                    )
                    _years = [y for y in coll_dict.keys() if int(year) - int(y) <= 2]
                    cellids_min = {
                        Path(p).stem.split("_")[0]
                        for p in coll_dict[min_year]
                        if Path(p).stem.split("_")[0] in cellids
                    }

                    _year_dict = []
                    for _year in _years:
                        if _year != min_year:
                            cellids_year = {
                                Path(p).stem.split("_")[0] for p in coll_dict[_year]
                            }
                            nondups = cellids - cellids_min.intersection(cellids_year)
                            _year_dict.extend(
                                [
                                    p
                                    for p in coll_dict[_year]
                                    if Path(p).stem.split("_")[0] in nondups
                                ]
                            )
                        else:
                            _year_dict.extend(
                                [
                                    p
                                    for p in coll_dict[_year]
                                    if Path(p).stem.split("_")[0] in cellids
                                ]
                            )

                    items_dict[stand][year].setdefault("items", {}).setdefault(
                        coll, []
                    ).extend(_year_dict)

                else:
                    items_dict[stand][year].setdefault("items", {}).setdefault(
                        coll,
                        [p for p in coll_dict if Path(p).stem.split("_")[0] in cellids],
                    )

    for dataset in images_dict:

        dataset_paths = []
        if isinstance(images_dict[dataset], dict):
            for year in images_dict[dataset]:
                dataset_paths.extend(images_dict[dataset][year])
        else:
            dataset_paths = images_dict[dataset]

        # Create one collection for each dataset
        dts_info = conf["items"][dataset]
        providers = [
            Provider(
                name=p.get("name", None),
                roles=[p.get("roles", None)],
                url=p.get("url", None),
            )
            for p in dts_info["providers"].values()
        ]
        fbench_collection = Collection(
            id=f"{dataset}",
            description=dts_info["description"],
            providers=providers,
            license=dts_info["license"]["type"],
            extent={},
        )

        if dts_info["license"]["url"]:
            license_link = Link(
                rel="license", target=dts_info["license"]["url"], media_type="text/html"
            )
            fbench_collection.add_link(license_link)

        fbench.add_child(fbench_collection)

    # Create one label collection for each agency-year pair
    for agency in items_dict:
        print("Creating datasets and label collections for", agency)

        _dict = {
            "stands": [],
            "items": {},
        }
        for year in items_dict[agency]:
            _dict["stands"].extend(items_dict[agency][year]["stands"])
            for dataset in items_dict[agency][year]["items"]:
                _dict["items"].setdefault(dataset, []).extend(
                    items_dict[agency][year]["items"][dataset]
                )

        start_year = min([int(y) for y in items_dict[agency].keys()])
        end_year = max([int(y) for y in items_dict[agency].keys()])
        start_datetime = datetime(start_year, 1, 1)
        end_datetime = datetime(end_year, 12, 31)
        # _dict = items_dict[agency][year]
        cellids = [int(Path(p).stem.split("_")[0]) for p in _dict["stands"]]
        aoi = qq_shp[qq_shp.CELL_ID.isin(cellids)].total_bounds.tolist()
        label_info = conf["stands"][agency]
        label_info.update({"label_date": start_datetime})
        label_collection = Collection(
            id=f"{agency}-stands",
            description=label_info["description"],
            license=label_info["label_license"],
            providers=[
                Provider(
                    name=label_info["provider_name"],
                    roles=label_info["provider_roles"],
                    url=label_info["provider_url"],
                )
            ],
            extent=Extent(
                SpatialExtent(aoi), TemporalExtent([[start_datetime, end_datetime]])
            ),
        )

        if label_info["label_license"]["url"]:
            label_link = Link(
                rel="license",
                target=label_info["label_license"]["url"],
                media_type="text/html",
            )
            label_collection.add_link(label_link)

        # Create and add items to collections
        for dataset in _dict["items"]:
            dataset_paths = _dict["items"][dataset]
            print("\nAdding items to collection", dataset)

            def add_item(image_path, collection):
                # print(image_path)
                thumbnail_path = image_path.replace("-cog.tif", "-preview.png")
                metadata_path = image_path.replace("-cog.tif", "-metadata.json")
                idx = image_path.split("/").index(collection.id)
                subdir = "/".join(image_path.split("/")[idx:-1])
                asset_path_url = (
                    "https://fbstac-stands.s3.amazonaws.com/stands/data/" + subdir + "/"
                )
                try:
                    item = create_item(
                        image_path, thumbnail_path, metadata_path, asset_path_url
                    )

                    collection.add_item(item)
                except Exception as e:
                    print(e)

                return

            collection = fbench.get_child(dataset)

            params = [
                {
                    "image_path": image_path,
                    "collection": collection,
                }
                for image_path in dataset_paths
            ]

            multithreaded_execution(add_item, params)

            datetimes = [item.datetime for item in collection.get_all_items()]
            cellids = [int(Path(p).stem.split("_")[0]) for p in dataset_paths]
            aoi = qq_shp[qq_shp.CELL_ID.isin(cellids)].total_bounds.tolist()
            start_datetime = min(datetimes)
            end_datetime = max(datetimes)
            # for collection in fbench.get_all_collections():
            #     if collection.id == dataset:
            collection.extent = Extent(
                spatial=SpatialExtent(aoi),
                temporal=TemporalExtent([[start_datetime, end_datetime]]),
            )

        # Create labels and add references to source items.
        print("\nCreating labels and links to assets")

        def add_label_item(label_path, label_info):
            label_item, label_ext = create_label_item(label_path, label_info)

            [
                label_ext.add_source(item, assets=["image"])
                for item in fbench.get_all_items()
                if item.id.split("_")[0] == label_item.id.split("_")[0]
            ]

            label_collection.add_item(label_item)
            return

        params = [
            {"label_path": label_path, "label_info": label_info}
            for label_path in _dict["stands"]
        ]

        multithreaded_execution(add_label_item, params)

        print("\nAdding label collection to catalog")
        fbench.add_child(label_collection)

    # Validate catalog
    fbench.normalize_hrefs("fbstac")
    fbench.validate()

    return fbench


# %%
if __name__ == "__main__":

    TARGET = "/mnt/s3/fbstac-stands"
    TARGET = Path("/mnt/data/FESDataRepo/stac_stands/built_catalogs/treeforcast-s")
    TARGET.mkdir(parents=True, exist_ok=True)
    fbench = build_stac(Path(__file__).parent, run_as="prod")

    # Save catalog
    print("Saving catalog to", TARGET.as_posix())
    fbench.save(catalog_type=CatalogType.SELF_CONTAINED, dest_href=TARGET.as_posix())
