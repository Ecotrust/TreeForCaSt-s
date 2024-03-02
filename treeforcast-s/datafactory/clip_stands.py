"""
Extract stand maps for each qq tile.

1. Read stand feature collection and grid.
2. Overly each stand map with usgs_grid.
3. Group by tile.
4. Save each tile as a GeoJSON file.
"""

# %%
import os
from pathlib import Path

import geopandas as gpd
from gdstools import (
    ConfigLoader, 
    multithreaded_execution as mtexe,
    image_collection
)

def main():
    # %%
    conf = ConfigLoader(Path(__file__).parent.parent).load()
    GRID = conf.GRID
    STANDMAPSDIR = Path(conf.PROJDATADIR).parent / 'interim/stands'
    OUTPATH = Path(conf.PROJDATADIR)

    def save_tile(gdf, filepath, cols, overwrite=False):
        if os.path.exists(filepath) and (not overwrite):
            return
        
        if cols is None:
            cols = gdf.columns

        gdf[cols].to_file(filepath, driver='GeoJSON')
        return

    standmaps = image_collection(STANDMAPSDIR, file_pattern="*.geojson")
    # standmaps.pop(0)

    grid = gpd.read_file(GRID).to_crs(crs=3857)
    grid.insert(4, 'ST', grid.PRIMARY_STATE.apply(lambda x: x.upper()[:2]))
    grid.insert(5, 'cell_area', grid.geometry.area)

    for stand in standmaps:
        stand = Path(stand)
        print("Clipping stand map:", stand.name)

        YEAR = stand.stem.split('_')[-1]
        AGENCY = stand.stem.split('_')[0]
        STATE = stand.parent.parent.stem

        foi = gpd.read_file(stand).to_crs(crs=3857)
        
        # Rename grid columns to avoid conflicts with stand map columns
        # renamed = [col + '_1' if col in foi.columns else col for col in grid.columns]
        # grid.columns = renamed
        cols_bkup = foi.columns.tolist()

        if grid.columns[0] in foi.columns:
            grid.rename(columns={grid.columns[0]: grid.columns[0] + '_1'}, inplace=True)

        foi_ovrly = gpd.overlay(foi, grid)
        idx = foi_ovrly.columns.tolist().index(grid.columns[0])
        replace_cols = dict(zip(foi_ovrly.columns.tolist()[:idx], cols_bkup))
        foi_ovrly.rename(columns=replace_cols, inplace=True)
        keep_cols = foi_ovrly.columns.tolist()[:idx-2] + ['ST','geometry']

        # Group by CELL_ID attribute
        grouped = foi_ovrly.groupby('CELL_ID')
        df_list = [
            group for _, group in grouped 
            if group.geometry.area.sum() >= (group.cell_area.iloc[0]) * 0.3
        ]

        labels_path = Path(OUTPATH, 'labels', STATE, AGENCY, YEAR)
        labels_path.mkdir(parents=True, exist_ok=True)

        params = [
            {
                'gdf': df.to_crs(crs=4326),
                'filepath': labels_path / f'{df.CELL_ID.iloc[0]}_{YEAR}_{df.ST.iloc[0].lower()}_{AGENCY}_stands.geojson',
                'cols': keep_cols
            }
            for df in df_list
        ] 

        # %%
        mtexe(save_tile, params)

if __name__ == '__main__':
    main()