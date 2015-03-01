# encoding: utf-8
from dfs_client import *
import mincemeat

# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений
def mapfn(k, v):
  reduce_key = None
  for l in get_file_content(v):
    if reduce_key is None:
      matrix_num, start, end = l.split(" ", 2)
      reduce_key = matrix_num
      continue
    values = [int(v) for v in l.split(" ")]
    yield reduce_key, sum(values)

# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
    result = sum(vs)
    return result

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
