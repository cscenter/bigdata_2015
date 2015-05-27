# encoding: utf-8
from dfs_client import *
import mincemeat
# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений
def mapfn(k, v):
  #Hardcoded matrix sizes
  M=3
  N=6
  K=4
  for l in get_file_content(v):
    j, i, k, mn = l.split()
    yield "{} {}".format(i, k), int(mn)



# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
    return sum(vs)

s = mincemeat.Server()
step1_files = [l.strip() for l in get_file_content("step1")]


# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputDFSFileName(step1_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")

with open("result.txt", "w") as otp:
    pk = 1
    for key, value in sorted(results.items()):
        i, j = (int(x) for x in key.split())
        if pk != i:
            otp.write("\n")
        otp.write(str(value))
        otp.write(" ")
        pk = i
