# encoding: utf-8
from dfs_client import *
import mincemeat

# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений
# M1(m,k) and M2(k,n)
def mapfn(k, v):
    M = 3
    K = 4
    N = 6

    matrix_id = None
    for l in get_file_content(v):
        if matrix_id is None:
            matrix_id, first, last = map(int, l.strip().split())
            if matrix_id == 1:
                m = first
                k = 1
            if matrix_id == 2:
                k = first
                n = 1
            continue

        data = map(int, l.strip().split())
        for num in data:
            if matrix_id == 1:
                for n in range(1, N+1):
                    yield ((m,n), (k, num))
                k += 1
                if k == K + 1:
                    m += 1
                    k = 1
            if matrix_id == 2:
                for m in range(1, M+1):
                    yield ((m,n), (k, num))
                n += 1
                if n == N + 1:
                    k += 1
                    n = 1


# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
    result = 0
    d = {}
    for (i, num) in vs:
        if not i in d:
            d[i] = num
        else:
            result += d[i] * num
    
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

with open('data/result.dat', 'w') as out:
  for key, value in results.items():
    out.write("%s: %s\n" % (key, value))
