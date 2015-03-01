# encoding: utf-8

from dfs_client import *
import mincemeat
import codecs
import os

'''
Требуемое количество машин должно быть равно количеству элементов
в результирующей матрице, т.е. если матрицы размеров MxK и KxN, 
то нужное количество – M*N
'''

def mapfn(k, v):
    '''
    Идея в следующем: генерируем ключи в виде пары (i,j)
    Причём в случае первой матрице каждое значение нужно будет
    выдать с ключами (row, i) i-раз, где i=0..n-1, а для второй матрицы
    ключи (i, col) тоже i раз, где i=0..m-1
    '''
    # размерности матриц mxk, kxn
    m, k, n = 3, 4, 6
    matrix_num = None
    for l in get_file_content(v):
        rows, cols = 0, 0
        if matrix_num == None:
            matrix_num, start, end = map(int, l.split(" ", 2))
            # Определяем с какой-стороки матрицы начинаем работать
            row = start - 1
            col = 0
            continue
        for v in map(int, l.split(" ")):
            if matrix_num == 1:
                for i in xrange(n):
                    # yield (row, i), v
                    # mincemeat в качестве ключа может принимать 
                    # только строковый тип
                    yield ("%d %d" % (row, i)), v
                col += 1
                if col >= k:
                    col = 0
                    row += 1
            elif matrix_num == 2:
                for i in xrange(m):
                    # yield (i, col), v
                    yield ("%d %d" % (i, col)), v
                col += 1
                if col >= n:
                    col = 0
                    row += 1


def reducefn(k, vs):
    i,j = map(int, k.split(" "))
    # первая половина массива vs - относится к первой матрице
    m1_values = vs[:len(vs) / 2]
    # вторая половина к второй матрице
    m2_values = vs[len(vs) / 2:]
    return sum([m1v * m2v for (m1v, m2v) in zip(m1_values, m2_values)])


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

# for key, value in sorted(results.items()):
#     print("%s: %s" % (key, value) )

# сохраняем результат каждой задачи свертки в файл
# имя файла соответсвует ключу
for key, value in results.items():
    output_dir = "results"
    filename = "%s/%s" % (output_dir, key.replace(" ", "_"))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with codecs.open(filename, 'w', 'utf-8') as output:
        output.write(str(value))
    