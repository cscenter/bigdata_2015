# encoding: utf-8
from dfs_client import *
import mincemeat


def mapfn(k, v):
  L = 3
  M = 4
  N = 6
  reduce_key = None
  readed_el = 0
  for l in get_file_content(v):
    if reduce_key is None:
      matrix_num, start, end = l.split(" ", 2)
      reduce_key = int(matrix_num)
      i = jj = int(start) - 1
      k = j = 0
      continue
    values = [int(v) for v in l.split(" ")]
    readed_el += len(values)
    if reduce_key == 1:
      for value in values:
        for k in range(N):
          yield (i, k), ('A', i, j, value)
        j = (j + 1) % M
      i = int(start) - 1 + readed_el / M
    elif reduce_key == 2:
      for value in values:
        for i in range(L):
          yield (i, k), ('B', jj, k, value)
        k = (k + 1) % N
      jj = int(start) - 1 + readed_el / N
    else:
        raise Exception("Unexpected reduce_key {}".format(reduce_key))


def reducefn(k, vs):
    # key is (i, k)
    # values is a list of ('A', i, j, a_ij) and ('B', j, k, b_jk)
    hash_A = {j: a_ij for (x, i, j, a_ij) in vs if x == 'A'}
    hash_B = {j: b_jk for (x, j, k, b_jk) in vs if x == 'B'}
    result = 0
    M = 4
    for j in range(M):
      result += hash_A[j] * hash_B[j]
    return (k, result)


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