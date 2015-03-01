# encoding: utf-8
import mincemeat
from dfs_client import *
from functools import reduce


# Состовляет ключи вида (строка_результата, столбец_результата, число),
# где число показывает, на какой элемент соседней матрицы должен быть домножен элемент.
def mapfn(k, v):
  cols1 = 4
  raws1 = 3
  cols2 = 6
  raws2 = 4

  reduce_key = None
  for l in get_file_content(v):
    if reduce_key is None:
      matrix_num, start, end = l.split(" ", 2)
      num = int(matrix_num)
      reduce_key = matrix_num
      col = 1
      raw = int(start)
      cols = cols1 if num == 1 else cols2
      raws = raws1 if num == 1 else raws2
      ncols = cols1 if num == 2 else cols2
      nraws = raws1 if num == 2 else raws2
      continue

    for val in l.split(" "):
      if num == 1:
        for i in range(1, ncols + 1):
          yield "+".join((str(raw), str(i), str(col))), int(val)

      if num ==2:
        for i in range(1, nraws + 1):
          yield "+".join((str(i), str(col), str(raw))), int(val)

      col += 1
      if col > cols:
        col = 1
        raw += 1


# Умножает элементы.
def reducefn(k, vs):
    result = reduce(lambda x, y: x * y, vs, 1)
    return result


# Запуск сервера. 
s = mincemeat.Server() 

matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)
    
s.map_input = mincemeat.MapInputDFSFileName(matrix_files) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 

# Запуск промежуточных результатов кудато-то.
f = open("tmp.txt", "w")
for key, value in sorted(results.items()):
    # print("%s %s" % (key, value) )
    f.write("%s %s\n" % (key, value))



