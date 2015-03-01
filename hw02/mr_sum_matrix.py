# encoding: utf-8
from dfs_client import *
import mincemeat

#маппер ожидает на входе получать ключ и значение, равные имени
# файла, считывает кусок строки матрицы и для каждого элемента матрицы возвращает множество, такое, что
# если A * B, то для кажого A[i][j] ((i, p), (i, j, A[i][j])) p in 1..n, для каждого B[i][j] ((p, j), (i,j, B[i][j])) для 
# k in 1..k.
def mapfunc(k, v):
    k, m, n = 3, 4, 6 #размеры матриц, первая k*m, вторая m*n
    left = matrix_number = lenOfStr = t = r = -1
    i = 1
    for line in get_file_content(v):
        if left == -1: #обработка самой первой строки в файле
            matrix_number, left = [int(x)  for x in line.split(" ")][:2]         
            
            if matrix_number == 1:
                lenOfStr = m # длина строки матрицы
                t = n 
            else:
                lenOfStr = n
                t = k
            continue

        values = [int(x) for x in line.split(' ')]
        if r == -1:
            r = len(values) #длина куска, который помещается в файл
               
        for x in range(r):
            for j in range(1, t + 1):
                if matrix_number == 1:
                    yield str((left, j)), ((left, ((i-1) % (lenOfStr / r))*r + x + 1), values[x])
                else:
                    yield str((j, ((i-1) % (lenOfStr / r))*r + x + 1)), ((left,((i-1) % (lenOfStr / r))*r + x + 1), values[x])
        if i % (lenOfStr / r) == 0:
            left = left + 1 #для следущего i, кусок матрицы длины r принадлежит следующей строке
        i = i + 1
            
#редьюсер принимает значения с ключом - координаты ячейки матрицы-результата, у нас есть множество элементов первой и 
#второй матрицы с исходными координатами мы должны посчитать sumj (A[i][j] * B[j][k]). У нас все отсортировано - сначала
#идут элементы первой матрицы потом второй и осталось пройти от середины к краям перемножая.
def reducefunc(k, vs):
    print vs
    result = 0
    n = len(vs) / 2
    for j in range(n):
        result = result + vs[n - j - 1][1] * vs[n + j][1]
    return result
    
    
s = mincemeat.Server() 

# читаем список файлов, из которых состоят матрицы
matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)


# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputDFSFileName(matrix_files) 
s.mapfn = mapfunc
s.reducefn = reducefunc

results = s.run_server(password="") 
result_file = open('res.dat', 'w')#результат умножения матриц в файле res.dat
sizes = max(results.items())[0].strip(')').strip('(').split(',')
result_file.write("{} {}\n".format(sizes[0], sizes[1]))
current_row = 1
for key, value in sorted(results.items()):
    row_num = int(key.strip('(').split(',')[0])
    if current_row != row_num:
        result_file.write('\n')
        current_row = row_num
    result_file.write("{} ".format(value))
result_file.close()
