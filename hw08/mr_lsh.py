#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division

import mincemeat

# первый map-reduce соберет для каждого документа часть вектора признаков
# по r в каждом бине
# в качестве хеша используется простой lcg (h_k = 19 * h_(k-1) + x_k % buckets)


def mapfn1(k, v):
    import json
    doc_id = k
    doc_vector, params = v
    for i, vect in enumerate(doc_vector):
        partition = i // params['r']
        yield json.dumps((partition, doc_id)), (vect, params['k'])


def reducefn1(k, vs):
    import json
    partition, doc_id = json.loads(k)
    hash = 17
    for v, buckets in vs:
        hash = (19 * hash + v) % buckets
    return (partition, hash), doc_id


s = mincemeat.Server()

input1 = {}
input1['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input1['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input1['doc3'] =[48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 50, 77, 30, 32]
input1['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89, 63, 66, 96, 78, 19, 39, 53, 83, 20]

# пример с лекции
# input1 = {}
# input1['docs1'] = [2, 0, 1, 3, 1, 3, 1, 0, 1]
# input1['docs2'] = [1, 3, 2, 2, 4, 1, 1, 2, 3]
# input1['docs3'] = [4, 0, 3, 1, 0, 2, 4, 4, 0]
# input1['docs4'] = [2, 0, 1, 3, 0, 3, 1, 2, 3]


# параметры r и k задаются в качестве keyword аргементов для MapInput'а
# если требуется b бинов, то нужно передать следующее r:
#
# b = 10
# n = len(next(iter(input1)))
# # вместо len в случае если все данные не помещаются в память
# # можно просто посчитать тривиальным циклом
# r = n // b

s.map_input = mincemeat.ParamsDictMapInput(input1, r=2, k=2 ** 32)
s.mapfn = mapfn1
s.reducefn = reducefn1

results1 = s.run_server(password='')

# for key, value in sorted(results1.items()):
#     import json
#     print('%s: %s' % (json.loads(key), value))


# второй map-reduce собирает одинаковые хеши в одних бинах и возвращает документы
# попвашие в эти бины и их количество


def mapfn2(k, v):
    import json
    yield json.dumps(k), v[0]


def reducefn2(k, vs):
    return tuple(vs), len(vs)


input2 = results1.values()

s.map_input = mincemeat.TupleListMapInput(input2)
s.mapfn = mapfn2
s.reducefn = reducefn2

results2 = s.run_server(password='')

# for key, value in sorted(results2.items()):
#     import json
#     print('%s: %s' % (json.loads(key), value))


# уберем повторные записи
# выведем все пары (отсортированные), которые алгоритм определил кандидатами


def mapfn3(k, v):
    import json
    for i in xrange(v[0] - 1):
        for j in xrange(i + 1, v[0]):
            yield json.dumps(sorted((k[i], k[j]))), None


def reducefn3(k, vs):
    import json
    return json.loads(k)


input3 = results2.values()

s.map_input = mincemeat.TupleListMapInput(input3)
s.mapfn = mapfn3
s.reducefn = reducefn3

results3 = s.run_server(password='')

# for key, value in sorted(results3.items()):
#     print('%s: %s' % (key, value))


res_pairs = results3.values()

# на каждой строке список документов-кандидатов
for d in sorted(res_pairs):
    print(', '.join(d))
