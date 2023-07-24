.. Forest Stands STAC catalog documentation master file, created by
   sphinx-quickstart on Mon Jul 17 18:45:52 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Forest Benchmarking and Modeling STAC 
=====================================

Introduction
------------

The effective implementation of a forest monitoring and assessment strategy relies on the availability of extensive databases that capture specific information related to the location and characteristics of various forest disturbances. These disturbances encompass a wide range of activities such as harvesting, insect and disease infestations, as well as other natural disturbances. By integrating and thoroughly analyzing these comprehensive datasets, researchers can significantly improve the efficacy and precision of their machine learning models employed for predicting vital forest attributes. These enhanced models contribute to a deeper understanding of forest dynamics and enable informed decision-making regarding forest management and conservation practices.

To facilitate the discovery, exploration, and analysis of forest disturbances, we provide a a Spatiotemporal Asset Catalog (STAC) for Forest Benchmarking and Modelling. A STAC is a data specification that provides a standardized way to describe geospatial datasets and assets and a consistent structure and metadata schema that allows users to search and access relevant datasets more efficiently. 

The Forest Benchmarking and Modeling STAC product consist of one catalog of Washington's DNR forest inventory stands and one catalog with forest inventory plots from the `Forest Inventory and Analysis (FIA) <https://www.fia.fs.fed.us/>`_ program. 

Data for the stands catalog is provided as 7.5 minute quarter quad tiles, as defined in the `USGS topographic grid <https://carto.nationalmap.gov/arcgis/rest/services/map_indices/MapServer/4>`_. Each tile contains a label with stand features and attributes paired with the following datesets:

* Digital Elevation Model (DEM) and topographic metrics from the 3DEP program. 
* Orthoimagery from the National Agriculture Imagery Program (NAIP)
* LandTrendr change detection estimates 
* Gap Filled Landsat imagery. 

The structure of the stands catalog is shown below.

.. code-block:: text

   Catalog (Root): fbstac_stands
      - catalog.json
      |_ Collection: dataset_id
      |     - collection.json
      |     |_ Item: QQID_DATASETCODE
      |        - qqid_datasetcode.json
      |        |_ Asset: single or multiband COG
      |        |_ Asset: Thumbnail
      |
      |_ Collection: labels_collection_id
            - collection.json 
            |_ Item (Label extension): QQID_LABELCODE
               - qqid_labelcode.json
               - links: Links to items within the same QQ tile.
                  |_ Asset: FeatureCollection with attributes for each label.

`Browse stand catalog <https://radiantearth.github.io/stac-browser/#/external/fbstac-stands.s3.us-east-1.amazonaws.com/fbstac-stands/catalog.json?.language=en>`_


.. toctree::
   :maxdepth: 3
   :caption: Contents:

   modules
   tutorials

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
