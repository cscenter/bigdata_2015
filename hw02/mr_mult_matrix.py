# encoding: utf-8
from dfs_client import *
import mincemeat



# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений
def mapfn(k, v):
    
  r = 2 #ОЛОЛОЛООЛОЛОЛОЛОЛОЛОЛОЛОЛОЛОООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООООО
  n = 3
  K = 4
  m = 6
  value = 0
  for cur_col_in_result in range(1, m + 1):    
      row = 1
      col = 1
      head_title = False
      for l in get_file_content(k):
          if head_title == False:
              matrix_num, row, end = l.split(" ", 2)
              row = int(row)
              head_title = True
              continue

          row2 = 1
          col2 = 1
          head_title2 = False  
          for l2 in get_file_content(v):
              if head_title2 == False:
                  matrix_num2, row2, end2 = l2.split(" ", 2)
                  row2 = int(row2)
                  head_title2 = True
                  continue
              if cur_col_in_result >= col2 and cur_col_in_result <= col2 + r - 1:                  
                  if row2 >= col and row2 <= col + r - 1:
                      value += int(l.split(" ")[(row2 - 1) % r]) * int(l2.split(" ")[(cur_col_in_result - 1) % r])
              
              col2 += r
              if col2 > m:
                 row2 += 1
                 col2 = 1
          col += r
          if col > K:
              col = 1
              yield '(' + str(row) + ',' + str(cur_col_in_result) + ')', value
            #  yield str(row), value
              value = 0
              row += 1
#    values = [int(v) for v in l.split(" ")]
 #   yield reduce_key, sum(values)

# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
 #   print(k)
 #   print(vs)
    result = sum(vs)
    return result
    
s = mincemeat.Server() 

# читаем список файлов, из которых состоят матрицы
matrix_files_1 = [l for l in get_file_content("/matrix1")]
matrix_files_2 = [l for l in get_file_content("/matrix2")]
    
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputDFSFileNameByMatrix1Matrix2(matrix_files_1, matrix_files_2) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )
