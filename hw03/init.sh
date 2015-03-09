#!/bin/sh

kill $(ps aux | grep 'python mr_posting_lists.py' | awk '{print $2}')

rm -rf tmp/
mkdir tmp
mkdir tmp/plist

python mr_posting_lists.py & sleep 1 && python mincemeat.py localhost -v

echo Press any key to run second pass
read DUMMY

python mincemeat.py localhost -v
