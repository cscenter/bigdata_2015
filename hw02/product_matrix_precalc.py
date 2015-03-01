# encoding: utf-8
# -*- coding: utf-8 -*-
from dfs_client import *
import mincemeat

matrix1_rows = 3
matrix1_cols = 4

matrix2_rows = 4
matrix2_cols = 6

def mapfn(k, v):
    matrix1_rows = 3
    matrix1_cols = 4
    matrix2_rows = 4
    matrix2_cols = 6

    m = matrix1_rows
    n = matrix1_cols
    q = matrix2_cols
    matrix_num = None
    for l in get_file_content(v):
        if matrix_num is None:
            matrix_num, start, end = l.split(' ', 2)
            current_row = int(start)
            current_col = 1
            continue
        for value in l.split(" "):
            if int(matrix_num) is 1:
                if current_col > matrix1_cols:
                    current_col = 1
                    current_row += 1

                for j in xrange(1, q + 1):
                    yield (str(current_row) + '|' + str(j) + '|' + str(current_col) ), value #  i, j, номер в последовательности

                current_col += 1

            if int(matrix_num) is 2:
                if current_col > matrix2_cols:
                    current_col = 1
                    current_row += 1

                for i in xrange(1, m + 1):
                    yield (str(i) + '|' + str(current_col) + '|' + str(current_row) ), value #  i, j, номер в последовательности


                current_col += 1





def reducefn(k, vs):
    # print('reduce ', k, vs)
    (i, j, index) = k.split('|')
    # [a, b] = vs
    result = 1
    for v in vs:
        result *= int(v)

    result = (i + '|' + j), result
    return result



s = mincemeat.Server()


matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)
print(matrix_files)


s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
# for key, value in sorted(results.items()):
#     print("%s: %s" % (key, value) )

file = open("precalc_results", "w")
for key, value in results.items():
    print(value)
    new_key, new_value = value
    file.write("%s %s\n" % (new_key, new_value))