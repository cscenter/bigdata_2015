# encoding: utf-8
from collections import Counter
import math
import cPickle as pickle
from dfs_client import *
import mincemeat

__author__ = 'Flok'


# Захардкоженные переменные
I = 3
J = 4
K = 6

# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память)
# выплевывает номер матрицы и сумму значений
def mapfn(k, v):
    left_file, right_file = pickle.loads(v)
    start, finish, fileIO = get_file_descriptor(left_file)
    start2, finish2, _ = get_file_descriptor(right_file)

    for row in range(start, finish + 1):
        col = 0 # Число считанных элементов
        # смещение массива в памяти относительно строки в матрице.
        mem_offset = 0

        # промотаем столбцы левой матрицы, для которых у нас нет множителей
        while col < start2:
            numbers, col = read_line(fileIO, col)
            mem_offset = col - len(numbers)
        # для каждого элемента строки правой матрицы производим умножение
        for k in range(K):
            ans = 0
            for j in range(start2 - 1, finish2):
                left = numbers[j - mem_offset]
                right = get_matrix_elem(right_file, j - start2 + 1, k)
                ans += left * right
                # если мы дошли до края памяти, но не до края матрицы
                # читаем новую порцию элементов строки левой матрицы
                if j == col - 1 and col < J:
                    numbers, col = read_line(fileIO, col)
                    mem_offset = col - len(numbers)
            yield pickle.dumps((row, k + 1)), ans
        # снова проматываем столбцы до конца, для которых у нас нет множителей
        while col != J:
            _, col = read_line(fileIO, col)


def read_line(fileIO, col):
    numbers = [int(v) for v in fileIO.next().split()]
    col += len(numbers)
    return numbers, col

# в файле, содержащим часть матрицы, ищет элемент по смещению row, col
def get_matrix_elem(file, row, col):
    start, finish, fileIO = get_file_descriptor(file)
    seek_row(row - 1, J, fileIO)
    curr_col = 0
    for l in fileIO:
        numbers = l.split()
        curr_col += len(numbers)
        if curr_col > col:
            return int(numbers[len(numbers) - curr_col + col])


# читает файл с частью матрицы, возвращает информацию из первой строки
# и ссылку на поток
def get_file_descriptor(file_name):
    fileIO = get_file_content(file_name)
    start, finish = (int(v) for v in fileIO.next().split()[1:])
    return start, finish, fileIO


# работает как seek, только не по символам, а по рядам.
def seek_row(row, columns, fileIO):
    for i in range(row):
        numbers = []
        while len(numbers) != columns:
            numbers.extend(fileIO.next().split())


# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
    result = sum(vs)
    return result



#на вход идёт имя файла матрицы (например, matrix1)
#возвращает список пар (id матрицы, .dat файл с частью матрицы)
def get_job_parts(filename):
    chunks = None
    for f in dfs.files():
        if f.name == filename:
            chunks = f.chunks
            break
    clocs = {}
    for c in dfs.chunk_locations():
        if c.id in chunks:
            clocs[c.id] = c.chunkserver
    #читаем разбиение матрицы на части, готовим список новых файлов
    #в которых эти части лежат
    files = []
    for chunk, serv in clocs.items():
        files.extend(x[:-1] for x in dfs.get_chunk_data(serv, chunk))
    return files


#составляет список всех возможных пар файлов .dat
def divide_tasks(left_matrix, right_matrix):
    left_matrix_files = get_job_parts(left_matrix)
    right_matrix_files = get_job_parts(right_matrix)
    for left_file in left_matrix_files:
        for right_file in right_matrix_files:
            yield (left_file, right_file)

if __name__ == "__main__":
    s = mincemeat.Server()

    matrix_files = [pickle.dumps(v) for v in divide_tasks("/matrix1", "/matrix2")]
    s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
    s.mapfn = mapfn
    s.reducefn = reducefn

    results = s.run_server(password="")
    for key, value in sorted(results.items()):
        print("%s: %s" % (pickle.loads(key), value) )
