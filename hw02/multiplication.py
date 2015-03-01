# encoding: utf-8

from dfs_client import *
import mincemeat
#Количество мапперов, которые нам понадобятся равно количеству различных пар файлов, первый из которых
#хранит данные первой матрицы, а второй хранит данные второй матрицы.
#Количество редьюсеров равно количество элементов в результирующей матрице, то есть n*m. Нужно не более n*m машин, иначе они будут простаивать

def mapfn(v1, v2):
#размерности матриц ( k - количество столбцов первой матрицы и строк второй матрицы,
# m - количество столбцов второй матрицы)  и допустимой длины строки в чанке(r)
    r = 2
    k = 4
    m = 6
    ind_1 = False
    num_of_col_1 = 1
    num_of_row_1 = 1
    #читаем блоки длины r из чанков файла, содержащего информацию о первой матрице
    for l1 in get_file_content(v1):
        #проверка перехода в новый чанк
        if ind_1 == False:
            matrix_num1, start1, end1 = l1.split(" ", 2)
            num_of_row_1 = int(start1)
            ind_1 = True
            continue
        num_of_col_2 = 1
        num_of_row_2 = 1
        ind_2 = False
        #читаем блоки длины r из чанков файла,содержащего информацию о второй матрице
        for l2 in get_file_content(v2):
            if ind_2 == False:
                matrix_num2, start2, end2 = l2.split(" ", 2)
                num_of_row_2 = int(start2)
                ind_2 = True
                continue
            #проходимся по столбцам второй матрицы
            for col_2 in range(num_of_col_2, num_of_col_2 + r):
                for i in range(0, r):
                    if num_of_row_2 == num_of_col_1 + i:
                        #создаём ключ, содержащий в себе координаты элемента в результирующей матрице
                        key = '[' + str(num_of_row_1) + ',' + str(col_2) + ']'
                        #создаём значение, равное произведению двух чисел, это произведение будет являться слагаемым в суммировании
                        #элементов для вычисления элемента результирующей матрицы
                        value = int(l1.split(" ")[(num_of_row_2 - 1) % r]) * int(l2.split(" ")[(col_2 - 1) % r])
                        yield key, value
            num_of_col_2 += r
            if num_of_col_2 > m:
                num_of_col_2 = 1
                num_of_row_2 += 1
        num_of_col_1 += r
        if num_of_col_1 > k:
            num_of_col_1 = 1
            num_of_row_1 += 1


def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server()
# читаем список файлов, из которых состоят матрицы
matrix_files1 = [l for l in get_file_content("/matrix1")]
matrix_files2 = [l for l in get_file_content("/matrix2")]
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MyMapInputDFSFileName(matrix_files1, matrix_files2)
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )
    name_of_file = "%s_%s" % (key[1], key[3])
    write_file(name_of_file, str(value))













