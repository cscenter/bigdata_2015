# encoding: utf-8
# -*- coding: utf-8 -*-
from dfs_client import *
import mincemeat

# второй MapReduce
# по сути ничего не делает, направляет всё в reduce
# M * Q * N мапперов
def mapfn(k, v):

    for l in get_file_content(v):
        (key, value) = l.split(' ')
        yield key, value

# здесь суммируем все подготовленные значения для соответствующих ячеек (ключ - координаты ячейки, значения в сумме дают необходимый результат для ячейки)
# и пишем в файл, возвращаем имя файла
# в каждом файле будет запись формата "i|j v" где i,j - координаты матрицы, v - значение в ячейке с координатами i, j
# количество reduceров не более чем M * Q
def reducefn(k, vs):
    result = 0
    for v in vs:
        result += int(v)
    write_file(k, str(result) + '\n')
    return k



s = mincemeat.Server()


precalc_result_files = [l for l in get_file_content('precalc_result_files')]

s.map_input = mincemeat.MapInputDFSFileName(precalc_result_files)
s.mapfn = mapfn
s.reducefn = reducefn


results = s.run_server(password="")
# пишем в файл все файлы описывающие матрицу
result_matrix_files = ''
for key, value in results.items():
    result_matrix_files += str(key) + '\n'
write_file('result_matrix_files', result_matrix_files)

