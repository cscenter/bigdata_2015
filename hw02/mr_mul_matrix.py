# encoding: utf-8
import logging
import sys

from dfs_client import *
import mincemeat
import print_matrix


def mapfn(dummy, v):
    """
        for each (i, j) of A (LxM) emits key=(i,k) value=A[i,j] in 1..N
        for each (j, k) of B (MxN) emits key=(i,k) value=B[j,k] in 1..L
    :param dummy:
    :param v:
    :return:
    """
    matrix_id = None
    # hardcoded dims
    L = 3
    M = 4
    N = 6
    for line in get_file_content(v):
        if matrix_id is None:
            matrix_id, start_row, dummy = line.split(" ", 2)
            row = int(start_row)
            col = 1
            continue
        values = [int(v) for v in line.split(" ")]
        if matrix_id == '1':
            for A_ij in values:
                for k in range(1, N + 1):
                    logging.info("Emitting reduce_key=(%s,%s), value=%s for A" % (row, k, A_ij))
                    yield (row, k), A_ij
                col += 1
                if col > M:
                    row += 1
                    col = 1
        else:
            for B_jk in values:
                for i in range(1, L + 1):
                    logging.info("Emitting reduce_key=(%s,%s), value=%s for B" % (i, col, B_jk))
                    yield (i, col), B_jk
                col += 1
                if col > N:
                    row += 1
                    col = 1


# key = (i,k)
# value = sum_j (A[i,k] * B[j, k])
def reducefn(dummy, vs):
    # hardcoded dims
    L = 3
    M = 4
    N = 6
    # dot product
    return sum([A_ij * B_jk for (A_ij, B_jk) in zip(vs[:M + 1], vs[M:])])


def printInput():
    print_matrix.print_matrix("/matrix1", 3, 4)
    print_matrix.print_matrix("/matrix2", 4, 6)


def printOutput(results):
    curr_row = 1
    for k, v in sorted(results.items()):
        if k[0] != curr_row:
            print('')
            curr_row = k[0]
        sys.stdout.write('%s\t' % v)


s = mincemeat.Server()

matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)

printInput()

s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")

printOutput(results)