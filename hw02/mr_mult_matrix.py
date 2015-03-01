# encoding: utf-8
from dfs_client import *
import mincemeat
import json

# Мапперов количество файлов хранящих первую матрицу * кол-во вторую 
# M * N различных ключей, столько и нужно мапперов
#
#
#


# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений
#  n = 3

def mapfn(k, v):
#  for i in range(1, 11111111):
 #     b = i
 
 # value = 0    
  r = 2
  K = 4
  m = 6
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
              if row2 >= col and row2 <= col + r - 1:
                  for cols in range(col2, col2 + r):
             #        print('(' + str(row) + ',' + str(cols) + ')' + ' ' +  str(int(l.split(" ")[(row2 - 1) % r]) * int(l2.split(" ")[(cols - 1) % r])) )
                     yield '[' + str(row) + ',' + str(cols) + ']', int(l.split(" ")[(row2 - 1) % r]) * int(l2.split(" ")[(cols - 1) % r]) 
              col2 += r
              if col2 > m:
                 row2 += 1
                 col2 = 1
          col += r
          if col > K:
              col = 1
          #    yield '(' + str(row) + ',' + str(cur_col_in_result) + ')', value
            #  yield str(row), value
            #  value = 0
              row += 1
#    values = [int(v) for v in l.split(" ")]
 #   yield reduce_key, sum(values)

# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
 #   print(k)
 #   print(vs)
 #   print(k + ' ' + str(vs))
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
#sum = 0
#for key, value in sorted(results.items()):
#    print("%s: %s" % (key, value) )
#    sum += value
#print(sum)        

r = 2
n = 3
m = 6
count_rows_in_each_matrix_dat = 2
count_rows_in_each_chunks = 2
#matrix_file = open('matrix.dat', 'w')
cur_row = 1
cur_col = 1
matrix_dat = 'matrix_1.dat'
matrix_dat_num = 1
chunk_num = 1
chunk_file = open('chunks/chunk_1_1', 'w')
chunk_file.write(str(cur_row) + ' ' + str(cur_row + count_rows_in_each_matrix_dat - 1) + '\n')
for key, value in sorted(results.items()):  
    chunk_file.write(str(value))
    if cur_col % r == 0:
        chunk_file.write('\n')
    else:
        chunk_file.write(' ')
    cur_col += 1
    if cur_col > m:
        cur_col = 1
        cur_row += 1
    if cur_row > n:    
        break
    if (cur_row - 1) % count_rows_in_each_matrix_dat == 0 and cur_col == 1:
        matrix_dat_num += 1
        matrix_dat = 'matrix_%d.dat' % matrix_dat_num
        chunk_num = 1 
    if (cur_row - 1) % count_rows_in_each_chunks == 0 and cur_col == 1:
        if (cur_row - 1) % count_rows_in_each_matrix_dat != 0:
            chunk_num += 1        
        chunk_file = open('chunks/chunk_%d_%d' % (matrix_dat_num, chunk_num), 'w')
        if chunk_num == 1:        
            chunk_file.write(str(cur_row) + ' ' + str(min(n, cur_row + count_rows_in_each_matrix_dat - 1)) + '\n')
    print(str(cur_row) + ' ' + str(cur_col) + str(matrix_dat) + ' ' + str('chunk_%d_%d' % (matrix_dat_num, chunk_num)))    








