#!/bin/bash
TIMEOUT="0.3s"
NUM="3"

if [ "x$1" != "x" ]
then
  NUM="$1"
fi

python mr_lsh.py &
sleep "$TIMEOUT"

for i in $(seq "$NUM")
do
  python mincemeat.py localhost
done

# kill -9 $(ps aux | grep "python mr_lsh.py" | grep -v grep | awk '{print $2}')
