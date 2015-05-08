#!/bin/bash

ERR="/dev/null"
#ERR="/dev/stderr"
python raft.py -p 8001 -l 1:L,1:O,1:R,4:E,4:M,5:I,5:P,6:S,6:U -t 6 2>$ERR &
python raft.py -p 8002 -l 1:L,1:O,1:R,4:E -t 4 2>$ERR &
python raft.py -p 8003 -l 1:L,1:O,1:R,4:E,4:M,5:I,5:P,6:S,6:U,6:S,6:D -t 6 2>$ERR &
python raft.py -p 8004 -l 1:L,1:O,1:R,4:E,4:M,5:I,5:P,6:S,6:U,6:S,7:D,7:O -t 7 2>$ERR &
python raft.py -p 8005 -l 1:L,1:O,1:R,4:E,4:M,4:E,4:T -t 4 2>$ERR &
python raft.py -p 8006 -l 1:L,1:O,1:R,2:S,2:I,2:T,3:D,3:U,3:R,3:U,3:M -t 3 2>$ERR &
python raft.py -p 8007 -l "" -t 1 2>$ERR &
python raft.py -p 8008 -l 1:L,1:O,1:R,4:E,4:M,5:I,5:P,7:X,7:X -t 7 &


sleep 2s
python raft.py -p 8000 -l 1:L,1:O,1:R,4:E,4:M,5:I,5:P,6:S,6:U,8:M -t 8 -f 8001,8002,8003,8004,8005,8006,8007,8008
kill $(jobs -p)


