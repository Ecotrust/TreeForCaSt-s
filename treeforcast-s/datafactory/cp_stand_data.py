import os
from pathlib import Path
import shutil

from pystac import Catalog

from gdstools import ConfigLoader, multithreaded_execution


def copy_asset(src, dst, overwrite=False):
    print(f'Copying {Path(src).name} to {dst}')

    if os.path.exists(dst) and not overwrite:
        print(f'File {dst} aready exist. Skipping ...')
        return

    Path(dst).parent.mkdir(parents=True, exist_ok=True)

    try:
        shutil.copy(src, dst)
    except Exception as e:
        print(f'Error copying file {src}. Exception raised: {e}')
        
    return

def main(root):
    conf = ConfigLoader(root).load()
    root_catalog = Catalog.from_file(os.path.join(conf.CATALOG_PATH, 'catalog.json'))
    prjdir = Path(conf.PROJDATADIR)

    # Print some basic metadata from the Catalog
    print(f"ID: {root_catalog.id}")
    print(f"Title: {root_catalog.title or 'N/A'}")
    print(f"Description: {root_catalog.description or 'N/A'}")

    # items = root_catalog.get_child(collection_name).get_all_items()
    items = [collection.get_all_items() for collection in root_catalog.get_all_collections()]
    assets = [item.get_assets() for collection in items for item in collection]
    hrefs = [[a[k].href for k in a.keys()] for a in assets]
    hrefs = [h for sublist in hrefs for h in sublist] 

    root_href = 'https://fbstac-stands.s3.amazonaws.com'
    sources = [img.replace(root_href + '/data', prjdir.as_posix()) for img in hrefs]
    targets = [img.replace(root_href, conf.CATALOG_PATH) for img in hrefs]

    params = [
        {
            'src': src,
            'dst': dst,
        }
        for src, dst in zip(sources, targets)
    ]

    multithreaded_execution(copy_asset, params)

    return

if __name__ == '__main__':
    
    root = Path(__file__).parent.parent
    main(root)
