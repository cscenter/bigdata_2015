# encoding: utf-8
from dfs_client import *
import mincemeat


def mapfn(k, v):
  k = 4
  n = 6
  m = 3

  num_line = 1
  for line in get_file_content(v):
    data = line.split()
    if num_line == 1:
      num_line = 2
      num_matrix = data[0]
      num_matrix_line = int(data[1])
      counter = 1

    else:
      for num in data:
        if num_matrix == '1':
          for val in range(1,(n+1)):
            reduce_key = ' '.join([str(num_matrix_line), str(val)])
            reduce_value = [int(counter), int(num)]
            yield (reduce_key, reduce_value)
          if counter < k:
            counter += 1
          else:
            counter = 1 
            num_matrix_line += 1

        elif num_matrix == '2':
          for val in range(1,(m+1)):
            reduce_key = ' '.join([str(val), str(counter)])
            reduce_value = [int(num_matrix_line), int(num)]
            yield (reduce_key, reduce_value)
          if counter < n:
            counter += 1
          else:
            counter = 1 
            num_matrix_line += 1      

    
def reducefn(k, vs):
  result = 0
  vs = sorted(vs)
  for x in range(0, len(vs)-1, 2):
    result += vs[x][1]*vs[x+1][1]
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

new_line = '1'
last_line = '1'
for k, v in sorted(results.items()):
  new_line = k.split()[0]
  if new_line == last_line:
    print v, ' ', 
  else:
    print '\n', v, ' ',
  last_line = new_line
