# encoding: utf-8
import hashlib
import datetime
from dfs_client import *
import mincemeat

# маппер ожидает на входе получать ключ и значение, равные имени файла
# для каждого элемента генерируются ключи, имеющие номер в результирующей матрице и текущее значение
def mapfn(k, v):
  m1_rows = 3
  m1_cols = 4
  m2_cols = 6

  matrix_num = None
  start = None
  line_num = 0
  for l in get_file_content(v):
    if matrix_num is None:
      matrix_num, start, _ = l.split(" ", 2)
      start = int(start)
      continue

    values = [int(v) for v in l.split(" ")]
    r = len(values)
    for i in range(r):
      shift = r * line_num + i
      if matrix_num == '1':
        x = start + shift / m1_cols - 1
        y = shift % m1_cols
        for col in range(m2_cols):
          yield str((x, col)), (matrix_num, y, int(values[i]))
      elif matrix_num == '2':
        x = start + shift / m2_cols - 1
        y = shift % m2_cols
        for row in range(m1_rows):
          yield str((row, y)), (matrix_num, x, int(values[i]))
      else:
        raise Exception("Matricies numbers should be equal 1 or 2")

    line_num += 1


# На входе получается список для некоторого ключа (x, k)
# Значения списка: (left_matrix, y, l[x, y]) и (right_matrix, y, right[y, k])
# Редьюсер соединяет перемножает значения в списке, которые имеют одинаковый 'y',
# после чего суммирует результат
def reducefn(k, vs):
  left = filter(lambda v: v[0] == '1', vs)
  right = filter(lambda v: v[0] == '2', vs)
  left.sort(key=lambda x: x[1])
  right.sort(key=lambda x: x[1])
  return sum(left[i][2] * right[i][2] for i in range(len(left)))


def save_result(results):
  filename = str(datetime.datetime.now().time()) + ".txt"
  f = open(filename, 'w')
  for key, value in sorted(results.items()):
    k = key.strip("()").split(", ")
    if k[0] != '0' and k[1] == '0':
      f.write("\n")
    f.write(str(value) + ' ')
  f.close()
  return filename


s1 = mincemeat.Server()

# читаем список файлов, из которых состоят матрицы
matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)
    
# и подаем этот список на вход мапперам
s1.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s1.mapfn = mapfn
s1.reducefn = reducefn

results = s1.run_server(password="")
filename = save_result(results)
print("Results saved to file: ", filename)