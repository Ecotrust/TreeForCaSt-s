.. Forest Stands STAC catalog documentation master file, created by
   sphinx-quickstart on Mon Jul 17 18:45:52 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

TreeForCaSt-s STAC Catalog 
==========================

Training and calibrating machine learning models requires benchmarking datasets that provide a point of reference to compare and evaluate algorithms. Benchmarking datasets minimizes variations in model performance due to differences in the type and quality of input data, data format and processing, and choice of assessment metrics, which allow modelers and developers to focus on model improvement and fine tuning. 

TreeForCaSt-s, is a proof-of-concept benchmarking dataset for modeling forest composition and structure using field inventory stands and remote sensing data. TreeForCaSt-s has the following features: 1) it is provided as a Spatio-Temporal Asset Catalog (STAC), a standard data structure to represent geospatial information. 2) It is a multi-sensor and multi-resolution dataset, 3) is open source and it is entirely based on public datasets available online. TreeForCaSt-s is intended for training multi-task machine learning models and models that are robust to missing data. 

The structure of the stands catalog is shown below.

.. code-block:: text

   Catalog (Root): treeforcast-s
      - catalog.json
      |_ Collection: dataset_id
      |     - collection.json
      |     |_ Item: QQID_DATASETCODE
      |        - qqid_datasetcode.json
      |        |_ Asset: single or multiband COG
      |        |_ Asset: Thumbnail
      |
      |_ Collection: stand_collection_id
            - collection.json 
            |_ Item (Label extension): QQID_LABELCODE
               - qqid_labelcode.json
               - links: Links to items within the same QQ tile.
                  |_ Asset: FeatureCollection with stand-level forest composition and structure attributes.

`Browse stand catalog <https://radiantearth.github.io/stac-browser/#/external/fbstac-stands.s3.us-east-1.amazonaws.com/stands/fbstac-stands/catalog.json?.language=en>`_


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
