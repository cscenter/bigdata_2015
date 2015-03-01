#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function

import dfs_client
import itertools
import mincemeat


def filter_lines(it):
    return (x for x in (y.rstrip() for y in it) if x)


def get_chunk_header(it):
    return (int(x) for x in next(it).split())


def get_matrix(matrix):
    files = list(dfs_client.get_file_content(matrix))
    total_rows = 0
    for file in files:
        it = filter_lines(dfs_client.get_file_content(file))
        num, start, end = get_chunk_header(it)
        rows = end - start + 1
        total_rows += rows
        r = len(next(it).split())
        cols = r
        for line in it:
            cols += r
        cols /= rows
    return total_rows, cols, r, files


def mapfn(key, value):
    import json

    num, i, j, n, r = key

    partition_size = r / 2

    if num == 1:
        for k in xrange(n):
            yield json.dumps((i, k, j / partition_size)), (num, j, value)
    elif num == 2:
        for k in xrange(n):
            yield json.dumps((k, j, i / partition_size)), (num, i, value)


def reducefn(key, values):
    import errno
    import json
    import os

    key = json.loads(key)
    i, j, partition = key

    values.sort(key=lambda x: (x[0], x[1]))
    s = len(values)
    ps = values[:(s / 2)]
    qs = values[(s / 2):]

    result = 0
    for k in xrange(s / 2):
        result += ps[k][2] * qs[k][2]

    result_filename = 'target/res_%d_%d_%d.txt' % (i, j, partition)
    try:
        os.makedirs('target')
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    with open(result_filename, 'w') as f:
        f.write(str(result) + '\n')

    return (i, j, partition), result


def iter_matrix(files, m, l, n, r, identifier):
    for file in files:
        it = filter_lines(dfs_client.get_file_content(file))
        _, start, end = get_chunk_header(it)
        i = start - 1
        j = 0
        for line in it:
            data = [int(x) for x in line.split()]
            for k in xrange(len(data)):
                yield (identifier, i, j, n, r), data[k]
                j += 1
            if j >= l:
                j = 0
                i += 1


class MapInputMatricesMultiply(mincemeat.MapInput):

    def __init__(self, *args):
        self.generator = self.get_generator(*args)

    def get_generator(self, files1, files2, m, l, n, r):
        for x in iter_matrix(files1, m, l, n, r, 1):
            yield x
        for x in iter_matrix(files2, l, n, m, r, 2):
            yield x

    def next(self):
        return next(self.generator)


def main():
    m, l, r, files1 = get_matrix('/matrix1')
    l, n, r, files2 = get_matrix('/matrix2')

    s = mincemeat.Server()

    s.map_input = MapInputMatricesMultiply(files1, files2, m, l, n, r)
    s.mapfn = mapfn
    s.reducefn = reducefn
    # print_matrix('/matrix1', l)

    results = s.run_server(password='')
    print(results)


if __name__ == '__main__':
    main()
