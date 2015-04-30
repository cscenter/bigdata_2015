#!/bin/bash

python mr_kmeans.py -n 4 -c '1,1;2,6;6,2' -t1 3 -t2 1 & sleep 1
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
