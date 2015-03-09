#!/bin/sh

kill $(ps aux | grep 'python server.py' | awk '{print $2}')

rm -r files data data2 
mkdir data data2

echo "launching master"
python server.py --role master --port 8000 & sleep 3

echo "launching node 1"
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8001 --data data & sleep 3

echo "launching node 2"
python server.py --role chunkserver --master localhost:8000 --chunkserver localhost --port 8002 --data data2 & sleep 3
