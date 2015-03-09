# encoding: utf-8

from dfs_client import *
import mincemeat
import sys


def mapfn(k, v):
  k = 4
  n = 6
  m = 3
  
  
  #cобираем информацию с первой строки
  meta = True
  for l in get_file_content(v):
    if meta:
      matrix_num, start, end = [int(i) for i in l.strip().split(' ')]
      rows = end - start +1
      meta = False
      i = start
      j = 1
      
      if matrix_num == 1:
        j_max = k
      if matrix_num == 2:
        j_max = n        
      continue
   
    values = [int(v) for v in l.split(" ")]
    
    #сохраняем значения матриц в соответсвующей ячейке результирующей матрицы
    for v in values:
      if matrix_num == 1:
        for cell in range(n):
          reduce_key = (str(i)+" "+str(cell+1))
          reduce_value = [int(j), int(v)]
          yield (reduce_key, reduce_value)
             
      elif matrix_num == 2:
        for cell in range(m):
          reduce_key = (str(cell+1)+" "+ str(j))
          reduce_value = [int(i), int(v)]
          yield (reduce_key, reduce_value)
      
      if j < j_max:
        j += 1
      elif j == j_max:
        j = 1 
        i += 1
     
        
def reducefn(k, vs):
  svs = sorted(vs)
  result = 0
  for x in range(0, len(svs)-1, 2):
    result += svs[x][1]*svs[x+1][1]
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
i = 1
for key, value in sorted(results.items()):
   if int(key.split(' ')[0]) == i:
    print value, " ",
   else:
    i += 1
    print '\n', value, ' ',