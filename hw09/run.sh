#!/bin/bash

python mr_kmeans.py -n 4 -c "1,1;3,1;1,3" -t2 2.5 -t1 5 & sleep 1
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
python mincemeat.py localhost
