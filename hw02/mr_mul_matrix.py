# encoding: utf-8
from dfs_client import *
import mincemeat
import collections

####
#
#   ОЦЕНКА:
#
#   Количество маперов: не больше K. Где K - количество шардов
#                       на которые разбиты наши данные.
#
#   Количество редьюсеров: не больше N * M. Где MxN - размерность
#                          результирующей матрицы.
#
#   Если машин будет больше - они будут простаивать.
#
####

def mapfn(k, v):
    """
    Map all input values as key -> value
    key - (i, j) - position in output matrix
    value - (column/row, value) - where column/row needs for
            reducer to understand which values should be multiplied between each other
            and value - is just original value for one position in input matrix
    """

    # constants start
    # Should be local. In other case mincemeat won't see them
    M = 3
    K = 4
    N = 6

    FIRST_MATRIX = 1
    SECOND_MATRIX = 2
    # constants end

    matrix_num = None
    current_row = None
    current_col = 0

    for l in get_file_content(v):
        if matrix_num is None:
            matrix_num, start, end = l.split(" ", 2)

            matrix_num = int(matrix_num)
            current_row = int(start) - 1

            continue

        values = [int(v) for v in l.split(" ")]
        for v in values:
            if (matrix_num == FIRST_MATRIX and current_col == K) or (matrix_num == SECOND_MATRIX and current_col == N):
                current_col = 0
                current_row += 1

            if matrix_num == FIRST_MATRIX:
                for i in range(N):
                    # this value will be used for
                    # output matrix (current_row, i) position calculation
                    yield (current_row, i), (current_col, v)
            else:
                for i in range(M):
                    # this value will be used for
                    # output matrix (i, current_col) position calculation
                    yield (i, current_col), (current_row, v)

            current_col += 1

def reducefn(k, vs):
    """
    Calculate result for one position in output matrix
    """
    map = collections.defaultdict(lambda: [])
    for v in vs:
        map[v[0]].append(v[1])

    summary = 0
    for v in map.values():
        summary += (v[0] * v[1])

    return summary

s = mincemeat.Server()

# читаем список файлов, из которых состоят матрицы
matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)

# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )