#!/bin/bash

python mr_lsh.py & sleep 1
python mincemeat.py localhost
python mincemeat.py localhost

