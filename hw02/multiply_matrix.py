# encoding: utf-8
from dfs_client import *
import mincemeat

# маппер ожидает на входе получать ключ и значение, равные имени
# файла, и для каждой строки (больше не помещается в память) 
# выплевывает номер матрицы и сумму значений

def mapfn(key, value):
    M = 3
    K = 4
    N = 6

    first_line_fl = True
    expected_values = 0
    counter = 0
    i = None
    j = 1
    j_border = None
    for line in get_file_content(value):
        if first_line_fl:
            try:
                matrix_num_str, start_str, end_str = line.strip().split(" ", 2)
                matrix_num = int(matrix_num_str)
                start = int(start_str)
                end = int(end_str)
            except:
                raise Exception(
                    "Malformed description string. Expected format:\"{int} {int} {int}\", but found: %s" % line)
            if start > end:
                raise Exception("End value must be more or equals then start. start:%s,end%s" % (start, end))
            if matrix_num != 1 and matrix_num != 2:
                raise Exception("Unexpected matrix number: %s" % matrix_num)
            if matrix_num == 1:
                expected_values = (end - start + 1) * K
                j_border = K
            else:
                expected_values = (end - start + 1) * N
                j_border = N

            first_line_fl = False
            i = start
        else:
            for str_val in line.strip().split(" "):
                cell_val = int(str_val)
                counter += 1
                if counter > expected_values:
                    raise Exception("File is too large. Expected %s values" % (expected_values, counter))

                if matrix_num == 1:
                    for k1 in xrange(1, N + 1):
                        reduce_key = "%s %s" % (i, k1)
                        reduce_value = "%s %s %s" % (matrix_num, j, cell_val)
                        yield (reduce_key, reduce_value)
                else:
                    for k2 in xrange(1, M + 1):
                        reduce_key = "%s %s" % (k2, j)
                        reduce_value = "%s %s %s" % (matrix_num, i, cell_val)
                        yield (reduce_key, reduce_value)

                j += 1
                if j > j_border:
                    j = 1
                    i += 1

    if counter != expected_values:
        raise Exception("Unexpected file size. Expected %s values but read %" % (
            expected_values, counter))


def reducefn(reduce_key, reduce_values):
    # M = 3
    K = 4
    # N = 6
    x_1 = {}
    x_2 = {}
    for reduce_value in reduce_values:
        matrix_num_str, i_str, v_str = reduce_value.split(" ", 3)
        matrix_num = int(matrix_num_str)
        i = int(i_str)
        v = int(v_str)
        if matrix_num == 1:
            x_1[i] = v
        else:
            x_2[i] = v
    sum = 0
    for index in xrange(1, K + 1):
        sum += x_1[index] * x_2[index]
    return reduce_key, sum


def local_handle(filname1, filname2, M, K, N):
    matrix_files = [l for l in get_file_content(filname1)]
    for l in get_file_content(filname2):
        matrix_files.append(l)
    # print("matrix_files:", matrix_files)
    map_result = {}
    for l in matrix_files:
        r = mapfn(None, l)
        for k, v in r:
            if k not in map_result:
                map_result[k] = [v]
            else:
                map_result[k].append(v)
    result = {}
    for reduce_key, reduce_values in map_result.iteritems():
        i, j = parse_matrix_index(reduce_key)
        if i not in result:
            result[i] = {}
        row = result[i]
        _, cell_result = reducefn(reduce_key, reduce_values)
        row[j] = cell_result
    print_matrix_from_result(result, M, N)


def remote_handle(filname1, filname2, M, K, N):
    s = mincemeat.Server()
    matrix_files = [l for l in get_file_content(filname1)]
    for l in get_file_content(filname2):
        matrix_files.append(l)
    s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
    s.mapfn = mapfn
    s.reducefn = reducefn

    results = s.run_server(password="")
    matrix = {}
    for key, value in results.items():
        _, v = value
        i, j = parse_matrix_index(key)
        if i not in matrix:
            matrix[i] = {}
        row = matrix[i]
        row[j] = v
    print_matrix_from_result(matrix, M, N)


def parse_matrix_index(reduce_key):
    i_str, j_str = reduce_key.split(" ", 2)
    i = int(i_str)
    j = int(j_str)
    return i, j


def print_matrix_from_result(result, M, N):
    for i in xrange(1, M + 1):
        line = ""
        if i in result:
            row = result[i]
            for j in xrange(1, N + 1):
                if j in row:
                    line = line + " " + str(row[j])
        print(line)


# local_handle("/matrix1", "/matrix2", 3, 4, 6)
# local_handle("/matrix3", "/matrix4", 3, 4, 6)
# remote_handle("/matrix1", "/matrix2", 3, 4, 6)
remote_handle("/matrix3", "/matrix4", 3, 4, 6)
