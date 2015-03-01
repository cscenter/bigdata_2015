# encoding: utf-8
from dfs_client import *
import mincemeat
# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений
def mapfn(k, v):
  reduce_key = None
  iter = 0
  pos = 1
  #Hardcoded matrix sizes
  M=3
  N=6
  K=4
  for l in get_file_content(v):
    if reduce_key is None:
      matrix_num, start, end = l.split(" ", 2)
      iter += int(start)
      matrix_num = int(matrix_num)
      reduce_key = matrix_num
      continue
    for v in l.strip().split():
        #print (matrix_num, iter, pos, "\n")
        if matrix_num == 1:
            i = iter
            j = pos
            yield str(j), (1, i, v)
        if matrix_num == 2:
            j = iter
            k = pos
            yield str(j), (2, k, v)

        pos += 1
    if (matrix_num == 1 and pos > K) or (matrix_num == 2 and pos > N):
        pos = 1
        iter += 1


# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
    #print(vs)
    first = []
    second = []
    for v in vs:
        if v[0] == 1:
            first.append(v)
        else:
            second.append(v)
    res = ""
    for f in first:
        for s in second:
            key = k
            v = (f[1], s[1], int(f[2])*int(s[2]))
            res += str(key) + " " + " ".join((str(x) for x in v)) + '\n'
    #print(res)
    write_file(k, res)
    return k

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
res = ""
for key, value in sorted(results.items()):
    res += key + "\n"

write_file("step1", res)
