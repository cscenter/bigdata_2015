# encoding: utf-8
# -*- coding: utf-8 -*-

# условимся что размерности матриц M на N, и N на Q
# этот MapReduce должен выполняться первым, маппер принимает на вход файлы матриц
# для каждой ячейки из исходной матрицы выдаёт ключ(координаты ячейки результирующей матрицы, на которую влияет, а так же индекс в сумме произведения строки из первой матрицы
# и столбца из второй матрицы) и значение ячейки
# в reduce мы перемножаем значения двух ячеек исходных матриц
# в результате мы имеем множество перемноженных ячеек из разных матриц, так же знаем координаты ячеек результирующей матрицы, в которой их надо просуммировать
# мапперов может быть не более чем  M + N (т.к. минимальное содержание в чанке - 1 строка матрицы)
# reduceров может быть не более чем M * Q * N (M Q размерность результирующей матрицы, для каждой ячейки сгенерируется по 2 * N значений, на каждую пару значений один редьюсер)
# в результате имеем M * Q * N значений, которые подадим второму MapReduceру (product_matrix.py)
from dfs_client import *
import mincemeat

matrix1_rows = 3
matrix1_cols = 4

matrix2_rows = 4
matrix2_cols = 6

def mapfn(k, v):
    matrix1_rows = 3
    matrix1_cols = 4
    matrix2_rows = 4
    matrix2_cols = 6

    m = matrix1_rows
    n = matrix1_cols
    q = matrix2_cols
    matrix_num = None
    for l in get_file_content(v):
        if matrix_num is None:
            matrix_num, start, end = l.split(' ', 2)
            current_row = int(start)
            current_col = 1
            continue
        for value in l.split(" "):
            if int(matrix_num) is 1:

                if current_col > matrix1_cols:
                    current_col = 1
                    current_row += 1

                for j in xrange(1, q + 1):
                    yield (str(current_row) + '|' + str(j) + '|' + str(current_col) ), value #  координаты в результирующей матрице и индекс

                current_col += 1

            if int(matrix_num) is 2:
                if current_col > matrix2_cols:
                    current_col = 1
                    current_row += 1

                for i in xrange(1, m + 1):
                    yield (str(i) + '|' + str(current_col) + '|' + str(current_row) ), value #  коориднаты в результирующей матрице и индекс


                current_col += 1




# обрабатывает и пишет в файл, возвращает имя файла
def reducefn(k, vs):
    (i, j, index) = k.split('|')
    new_key = (i + '|' + j)
    new_value = 1
    for v in vs:
        new_value *= int(v)

    result = new_key, new_value
    write_file(k, str(new_key) + ' ' + str(new_value) + '\n')
    return k



s = mincemeat.Server()


matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)
print(matrix_files)


s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
# пишем в файл все файлы для следующего MapReducа
precalc_result_files = ''
for key, value in results.items():
    precalc_result_files = precalc_result_files + str(key) + '\n'

write_file('precalc_result_files', precalc_result_files)
