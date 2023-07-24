"""
Validate the dataset by checking for empty images and errors while reading images.
"""

# %%
import os
from pathlib import Path
import rasterio
import pandas as pd
from forestsegnet.utils import (
    image_collection,
    ConfigLoader,
    multithreaded_execution
)

# Suppress errors
# Get cut-down GDAL that rasterio uses
from osgeo import gdal
gdal.PushErrorHandler('CPLQuietErrorHandler')

def main():
    conf = ConfigLoader(Path(__file__).parent.parent).load()
    # root = Path(conf.PROJDIR) / 'data/dev'
    root = Path(conf.PROJDATADIR) / '3dep'

    print('Validating images...')

    images = [{'img': i} for i in image_collection(root)]

    def check_empty(img):
        error_reading = 0
        is_empty = 0
        img_path = Path(img)
        try:
            with rasterio.open(img) as src:
                data = src.read()
                if data.sum() == 0:
                    is_empty = 1
        except:
            error_reading = 1

        return (img_path.parent, img_path.name, error_reading, is_empty)

    def has_metadata(img):
        img_path = Path(img)
        mdt_path = img_path.parent / img_path.name.replace('-cog.tif', '-metadata.json')
        no_metadata = []
        if not os.path.exists(mdt_path):
            return img_path.as_posix()
        return None
    
    results = multithreaded_execution(check_empty, images, 8)
    # nopath = multithreaded_execution(has_metadata, images, 8)

    df = pd.DataFrame(results, columns=['folder', 'image', 'error_reading', 'is_empty'])
    empty = df.is_empty.sum()
    errors = df.error_reading.sum()

    print(f"\nEmpty images: {empty}")
    if empty > 0:
        print(df[df.is_empty == 1].sort_values('image'))
    print(f"\nErrors while reading images: {errors}")
    if errors > 0:
        print(df[df.error_reading == 1])

if __name__ == '__main__':
    main()