#!/bin/bash

python canopy_kmeans.py -n 4 -c "1,1;2,6;6,2" -t1 4 -t2 2  & sleep 1
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost

