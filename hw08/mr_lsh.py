# encoding: utf-8

import mincemeat

# 1/b ^ 1/r = 1/2 и b * r = n   =>   b = n / r  и  2 ^ r = n/r 
# Lambert W Function
# r = W(n log 2) / log 2
# W(x) при x стремящемся к бесконечности log x - log log x:
# r = (log(n log 2) - log(log(n log 2)) / log 2

# k <= количество полос * количество документов

# выполняется два mapreduce:
# первый: разбает вектора на b полос, 
# второй: возвращает "корзины" с идентификаторами документов, имеющих одинаковые вектора 


def mapfn1(docid, docvector):
    from math import ceil, log

    a1 = log(len(docvector) * log(2))
    a2 = log(log(len(docvector) * log(2)))
    r = ceil((a1 - a2) / log(2))
    b = ceil(len(docvector) / r)
    for strip in range(int(b)):
        yield str(strip), (docid, docvector[int(r) * strip : int(r) * (strip + 1)])

def reducefn1(k, vs):
    return vs


def mapfn2(strip, doc):
    from collections import defaultdict

    hash_value = defaultdict(list)
    for docid, docvector in doc:
        hash_value[hash(str(docvector))].append(docid)
    for h in hash_value:
        if len(hash_value[h]) > 1:
            bucket = sorted(hash_value[h])
            for i, b1 in enumerate(bucket):
                for b2 in bucket[i + 1:]:
                    yield b1, b2

def reducefn2(k, vs):
    return [(k, v) for v in set(vs)]


s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]

s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn1
s.reducefn = reducefn1

results1 = s.run_server(password="") 

s.map_input = mincemeat.DictMapInput(results1)
s.mapfn = mapfn2
s.reducefn = reducefn2

results2 = s.run_server(password="")

for key, value in sorted(results2.items()):
    for v1, v2 in sorted(value):
        print v1, v2
