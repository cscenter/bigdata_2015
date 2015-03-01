# encoding: utf-8
from dfs_client import *
import mincemeat




# маппер читает файл, содержащий часть матрицы
# выдает пары (key, value), где key -- пара чисел,
# соответствующая какой-то ячейке финальной матрицы
# value -- числа, из которых будет складываться значение
# в этой ячейке, в следующем формате: (k, matrix_num, value)
# то есть на самом деле мы будем отправлять на обработку
# каждому reducer-у набор чисел, которые нужно будет
# попарно перемножить и просуммировать результаты
# NB: пришлось закомментировать строку в mincemeat, заносящую
# в лог информацию о вызове reducefn, не получалось применить encode
# к таким key
def mapfn(k, v):
    # константы, задающие размерность матриц
    M = 3
    K = 4
    N = 6
    matrix_num = None
    for l in get_file_content(v):
        if matrix_num is None:
            # читаем первую строку файла
            matrix_num, start, end = l.split(" ", 2)
            matrix_num = int(matrix_num)
            start = int(start)
            end = int(end)
            if matrix_num == 1:
                m = start
                k = 1
            elif matrix_num == 2:
                k = start
                n = 1
            else:
                raise Exception("Unknown matrix number = {}!".format(matrix_num))
            continue

        # читаем значения в матрице
        values = [int(v) for v in l.split(" ")]

        for v in values:
            # для каждого значения генерируем N либо M результатов, по одному
            # для каждого из чисел, с которым ему предстоит перемножиться
            if matrix_num == 1:
                for n in range(1, N + 1):
                    yield (m, n), (k, 1, v)
                k += 1
                if k == K + 1:
                    m += 1
                    k = 1
            else:
                for m in range(1, M + 1):
                    yield (m, n), (k, 2, v)
                n += 1
                if n == N + 1:
                    k += 1
                    n = 1
    if matrix_num == 1 and (m != end + 1 or k != 1):
        raise Exception("Wrong input format")
    if matrix_num == 2 and (k != end + 1 or n != 1):
        raise Exception("Wrong input format")

# в reducefn происходит суммирование:
# M3[i, n] = sum k = 1 to K (M1[i, k] * M2[k, n])
def reducefn(k, vs):
    sum = 0
    a = None
    for v in sorted(vs):
        if v[1] == 1:
            a = v[2]
        else:
            sum += a * v[2]
    return sum


s = mincemeat.Server()

# читаем список файлов, из которых состоят матрицы
matrix_files = [f for f in get_file_content("/matrix1")]
for f in get_file_content("/matrix2"):
    matrix_files.append(f)

#print(matrix_files)

s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")

result_file = open('result.dat', 'w')
cur_row = 1     # чтобы делать переносы строки в файле
m = max(results.items())[0]
result_file.write("{} {}\n".format(m[0], m[1]))
for key, value in sorted(results.items()):
    if cur_row != key[0]:
        result_file.write('\n')
        cur_row = key[0]
    result_file.write("{} ".format(value))
result_file.close()

# результат записывается в файл result.dat, каждая строка матрицы в отдельной строке,
# значения разделены пробелами, в первой строке два числа -- размерность матрицы
#
# mapper-ы выдают 2 * M * N * K пар ключ-значение, по 2 * k на каждый ключ
# т.о. reducer-ов будет M * N, mapper-ов столько, в скольких файлах хранятся матрицы
#
# количество машин -- не меньше 1, не больше M * N, больше бессмысленно
