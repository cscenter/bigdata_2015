# encoding: utf-8
from dfs_client import *
import mincemeat
import json
import os

# Количество мапперов равно количеству файлов хранящих первую матрицу умноженных на кол-во файлов хранящих вторую матрицу (их декартово произведение) 
# M * N различных ключей, столько и нужно редьюсеров, соотвественно столько машин, не больше, иначе будут машины без дела

def mapfn(k, v): 
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
                #     print('(' + str(row) + ',' + str(cols) + ')' + ' ' +  str(int(l.split(" ")[(row2 - 1) % r]) * int(l2.split(" ")[(cols - 1) % r])) )
                     yield '[' + str(row) + ',' + str(cols) + ']', (int(l.split(" ")[(row2 - 1) % r]) * int(l2.split(" ")[(cols - 1) % r]))                   
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

def collectfn(k, data):
  #  print(k + ' ' + str(data))
    return sum(data)

# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
 #   print(k)
 #   print(vs)
   # print(k + ' ' + str(vs))
    result = sum(vs)
    return result
    
s = mincemeat.Server() 

# читаем список файлов, из которых состоят матрицы
matrix_files_1 = [l for l in get_file_content("/matrix1")]
matrix_files_2 = [l for l in get_file_content("/matrix2")]
    
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputDFSFileNameByMatrix1Matrix2(matrix_files_1, matrix_files_2) 
s.mapfn = mapfn
s.collectfn = collectfn
s.reducefn = reducefn

results = s.run_server(password="") 
#sum = 0
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )
#    sum += value
#print(sum)        



#здравый смысл подсказал что раз мы читали по r блоков, то и хранить хорошо бы по r блоков 
r = 2
n = 3
m = 6
count_rows_in_each_matrix_dat = 2
count_rows_in_each_chunks = 2
cur_row = 1
cur_col = 1
matrix_dat = 'matrix_1.dat'
matrix_dat_num = 1
chunk_num = 1
if os.path.exists('chunks') == False:
    os.mkdir('chunks')
chunk_file = open('chunks/chunk_1_1', 'w')
chunk_file.write(str(cur_row) + ' ' + str(cur_row + count_rows_in_each_matrix_dat - 1) + '\n')
for_json_file = []
chunks = ['chunk_1_1']
matrixs = ['matrix_1.dat']
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
        for_json_file.append({"chunks": chunks, "name": '/' +matrix_dat}) 
        matrix_dat_num += 1
        matrix_dat = 'matrix_%d.dat' % matrix_dat_num
        matrixs.append(matrix_dat)
        chunk_num = 1  

        chunks = ['chunk_%d_%d' % (matrix_dat_num, chunk_num)]
    if (cur_row - 1) % count_rows_in_each_chunks == 0 and cur_col == 1:
        if (cur_row - 1) % count_rows_in_each_matrix_dat != 0:
            chunk_num += 1        
            chunks.append('chunk_%d_%d' % (matrix_dat_num, chunk_num))
        chunk_file = open('chunks/chunk_%d_%d' % (matrix_dat_num, chunk_num), 'w')
        if chunk_num == 1:        
            chunk_file.write(str(cur_row) + ' ' + str(min(n, cur_row + count_rows_in_each_matrix_dat - 1)) + '\n')  

chunk_file = open('chunks/matrix', 'w')
for m in matrixs:
    chunk_file.write('/' + m + '\n')
for_json_file.append({"chunks": chunks, "name": '/' + matrix_dat})
matrix_vector = ['/matrix']
for_json_file.append({"chunks": 'matrix', "name": matrix_vector}) 
json_file = open('matrix_files', 'w')
json.dump(for_json_file, json_file, indent = 2)





