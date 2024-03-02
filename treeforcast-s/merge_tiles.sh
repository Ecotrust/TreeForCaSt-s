#!/bin/bash

# This script merges multiple NAIP tiles into a single GeoTIFF file.
# Usage: ./merge_tiles.sh <input_dir> <output_dir>

for d in $1/*;
    do if [ -d $d ]; 
        then echo "Merging tiles in $d"; 

        # Create output directory if it doesn't exist
        tiles_dir=$(basename $d)
        mkdir -p $2/$tiles_dir;

        # Copy metadata and preview files
        cp $d/*.json $2/$tiles_dir/;
        cp $d/*.png $2/$tiles_dir/;

        # Retrieve file name from first tile
        cell=$(basename $d | cut -d'_' -f 1);
        for f in $d/${cell}_*_1.tif;
            do readarray -d "_" -t lst <<< $(basename $f .tif); 
            fname=${lst[0]}_${lst[1]}_${lst[2]}_${lst[3]}_${lst[4]};
        done;

        # Merge tiles
        gdalwarp $d/${cell}_*.tif $2/${tiles_dir}/${fname}-cog.tif; 
    fi
done

# # Collect cell ids
# cells=( $(
#     for f in $1/*.tif; 
#         do basename $f .tif | cut -d'_' -f 1; 
#     done | sort -u 
#     ) 
# )

# # Merge files with same cell id
# for cell in ${cells[@]};
#     # Retrieve file name from first tile
#     do for f in $1/${cell}_*_1.tif;
#         do readarray -d "_" -t splitted <<< $(basename $f .tif); 
#         fname=${splitted[0]}_${splitted[1]}_${splitted[2]}_${splitted[3]};
#     done
#     # Generate mosaic
#     gdalwarp $1/${cell}_*.tif $2/${fname}.tif; 
# done

