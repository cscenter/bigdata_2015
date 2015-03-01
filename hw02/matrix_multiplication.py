# encoding: utf-8
from dfs_client import *
import mincemeat


# in:  key-value pair, where both key and value are the same filename
# out: key-value pair, where key is (i, j) - coordinates of an element in result matrix,
# and value is the value of that element
def mapfn(k, v):
    size = {'1': {'rows': 3, 'cols': 4}, '2': {'rows': 4, 'cols': 6}}
    matrix_num = None
    for line in get_file_content(v):
        if matrix_num is None:
            matrix_num, start, end = line.split()
            start = int(start)
            row = start
            chunk_number = 0
            continue
        values = [int(v) for v in line.split()]
        if chunk_number == 0 and row == start:  # second line has been read
            r = len(values)
            chunks_per_line = size[matrix_num]['cols'] / r
        elif chunk_number == chunks_per_line:
            chunk_number = 0
            row += 1
        chunk_number += 1
        if matrix_num == '1':
            for v in values:
                for k in xrange(1, size['2']['cols'] + 1):
                    yield ((row, k), v)
        elif matrix_num == '2':
            col = r * (chunk_number - 1)
            for v in values:
                col += 1
                for k in xrange(1, size['1']['rows'] + 1):
                    yield ((k, col), v)


# Note that the elements of the first matrix are exactly in a first half of the 'vs' list due to matrix files
# order in 'matrix_files'. Similarly, the elements of the second matrix are in a second half.
# So we can just slice the list and calculate a dot product.
def reducefn(k, vs):
    mid = len(vs) / 2
    return sum([x * y for (x, y) in zip(vs[:mid + 1], vs[mid:])])


matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)

s = mincemeat.Server()
s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
result_matrix = sorted(results.items())
size = {'1': {'rows': 3, 'cols': 4}, '2': {'rows': 4, 'cols': 6}}  # sorry for duplicating
row_len = size['2']['cols']

if result_matrix[-1][0][0] != size['1']['rows'] or result_matrix[-1][0][1] != size['2']['cols']:
    raise Exception('matrices sizes are invalid')

with open('result.dat', 'w') as out:
    for key, value in result_matrix:
        out.write('{} '.format(value) if key[1] < row_len else '{}\n'.format(value))

# Result matrix is written on local disc, in file called 'result.dat', one row per line.
# This MapReduce is better to run on N * M machines, where N and M are sizes of result matrix
# (hence, N * M is number of reducers).