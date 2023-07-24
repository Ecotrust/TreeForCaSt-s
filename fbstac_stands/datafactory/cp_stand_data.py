import os
from pathlib import Path
import shutil

from pystac import Catalog

from gdstools import ConfigLoader, image_collection, multithreaded_execution


def copy_asset(src, dst, overwrite=False):
    print(f'Copying {Path(src).name} to {dst}')

    if os.path.exists(dst) and not overwrite:
        print(f'File {dst} aready exist. Skipping ...')
        return

    Path(dst).parent.mkdir(parents=True, exist_ok=True)

    try:
        shutil.copy(src, dst)
        shutil.copy(
            src.replace('-cog.tif', '-preview.png'), 
            dst.replace('-cog.tif', '-preview.png'),
        )
    except Exception as e:
        print(f'Error copying file {src}. Exception raised: {e}')
        
    return

def main(root):
    conf = ConfigLoader(root).load()
    root_catalog = Catalog.from_file(os.path.join(conf.CATALOG_PATH, 'catalog.json'))

    # Print some basic metadata from the Catalog
    print(f"ID: {root_catalog.id}")
    print(f"Title: {root_catalog.title or 'N/A'}")
    print(f"Description: {root_catalog.description or 'N/A'}")

    items = root_catalog.get_all_items()
    cellids = set([item.id.split('_')[0] for item in items])

    images = image_collection(conf.PROJDATADIR)
    images = [img for img in images if Path(img).name.split('_')[0] in cellids]
    previews = [img.replace('-cog.tif', '-preview.png') for img in images]

    labels = image_collection(conf.PROJDATADIR + 'labels', file_pattern='*.geojson')

    root_dirname = Path(conf.PROJDATADIR).name
    idx = images[0].split('/').index(root_dirname)
    targets = ['/'.join(conf.S3BUCKET.split('/') + img.split('/')[idx + 1:]) for img in images]
    label_targets = ['/'.join(conf.S3BUCKET.split('/') + lab.split('/')[idx + 1:]) for lab in labels]

    params = [
        {
            'src': img,
            'dst': dst,
        }
        for img, dst in zip(images, targets)
    ]

    multithreaded_execution(copy_asset, params)

    params = [
        {
            'src': lab,
            'dst': dst,
        }
        for lab, dst in zip(labels, label_targets)
    ]

    multithreaded_execution(copy_asset, params)

    return

if __name__ == '__main__':
    
    root = Path(__file__).parent.parent
    main(root)
