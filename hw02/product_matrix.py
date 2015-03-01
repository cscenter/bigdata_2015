# encoding: utf-8
# -*- coding: utf-8 -*-
from dfs_client import *
import mincemeat

def mapfn(k, v):
    matrix1_rows = 3
    matrix1_cols = 4
    matrix2_rows = 4
    matrix2_cols = 6

    m = matrix1_rows
    n = matrix1_cols
    q = matrix2_cols
    print('map')
    # for l in get_file_content(v):
    for l in open(v):
        print(l)
        print('yeah')
        (key, value) = l.split(' ')
        print(key, value)
        yield key, value


def reducefn(k, vs):
    result = 0
    for v in vs:
        result += int(v)
    return result



s = mincemeat.Server()


print('started')
precalc_files = 'precalc_results'
print('file loaded')
# s.map_input = mincemeat.MapInputDFSFileName(precalc_files)
s.map_input = mincemeat.MapInputLocalFile(precalc_files)
s.mapfn = mapfn
s.reducefn = reducefn


results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )

