# encoding: utf-8
from dfs_client import *
import mincemeat

#Для максимальной производительности можно использовать до F1 + F2 машин
#Где F1 - кол-во файлов первой матрицы, F2 - второй
#Результат умножения записывается в файл result.dat построчно

# маппер принимает (file, file)
# выдает пары (key, value), где key = (i, j) пара чисел,
# соответствующая ячейке результирующей матрицы
# value = (matrix_num, k, value)
# где k - число, связывающее попарно элементы которые нужно перемножить
def mapfn(k, v):
    left_matrix_num = 1     #константы для номеров матриц
    right_matrix_num = 2
    matrix_num = None       #номер текущей матрицы
    start = None            #начало строк в текущем файле
    end = None              #конец строк в текущем файле
    m = 3       #константы для размерностей матриц
    k = 4       # m x k, k x n
    n = 6
    for l in get_file_content(v):
        if matrix_num is None:
            matrix_num, start, end = l.split(" ", 2)
            matrix_num = int(matrix_num)
            start = int(start)
            end = int(end)
            curr_i = start #координаты обрабатываемого элемента матрицы
            curr_j = 1
            continue
        values = [int(v) for v in l.split(" ")]
        if matrix_num == left_matrix_num:
            for elem_ij in values:
                for j in range(1, n + 1):
                    yield (curr_i, j), (matrix_num, curr_j, elem_ij)
                curr_j += 1
            if curr_j > k:          #для перевода строки в матрице
                curr_i += 1
                curr_j = 1
        else:
            for elem_ij in values:
                for i in range(1, m + 1):
                    yield (i, curr_j), (matrix_num, curr_i, elem_ij)
                curr_j += 1
            if curr_j > n:
                curr_i += 1
                curr_j = 1
    if curr_i != end + 1 or curr_j != 1:   #проверка констант размерности матриц
        raise Exception("Bad size of matrix number = {}".format(matrix_num))


# редьюсер вычисляет элемент (i, j) результирующей матрицы с ключом k = (i,j)
def reducefn(k, vs):
    left_matrix_num = 1     #константы для номеров матриц
    right_matrix_num = 2
    (i, j) = k
    #разбиваем на два списка (значения левой матрицы и правой)
    list1 = [(v[1], v[2]) for v in vs if v[0] == left_matrix_num] 
    list2 = [(a,b) for (matrix_num, a, b) in vs if matrix_num == right_matrix_num]
    list1.sort() #сортируем для попарного перемножения
    list2.sort()
    result = sum([v[0][1] * v[1][1] for v in zip(list1, list2)])
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

out_put_f = open('result.dat', 'w')
j = 1     # текущая строка результирующей матрицы
for key, value in sorted(results.items()):
    if key[0] != j:       #если перешли на новую строку
        out_put_f.write('\n')
        j = key[0]
    out_put_f.write("{} ".format(value))
out_put_f.close()