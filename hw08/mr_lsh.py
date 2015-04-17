# encoding: utf-8
import mincemeat

def mapfn1(doc_id, doc_vector_with_r): #выплевываем пару (номер полосы, элементы вектора этой полосы)
    doc_vector, r = doc_vector_with_r
    n = len(doc_vector)
    band_id = 1
    for band_num in range(0, n, r):
        yield str(band_id), (doc_id, doc_vector[band_num : band_num + r])   
        band_id += 1

def reducefn1(band_id, vs): 
    return vs
    
def mapfn2(band_id, band_vector): # для каждой полосы получаем все возможные хэши и составляем список пар
    from collections import defaultdict
    hashes = defaultdict(list)
    for doc_id, doc_vector in band_vector:
        hashes[hash(str(doc_vector))].append(doc_id) 
    pairs = set()    
    for h in hashes.iteritems():
        if len(h[1]) > 1:
            for first_doc in h[1]:
                for second_doc in h[1]:
                    if first_doc != second_doc and (second_doc, first_doc) not in pairs:
                        pairs.add((first_doc, second_doc))   
    for p in pairs:
        yield str(band_id), p #нет смысла использовать один ключ и пихать всю информацию в редьюс
                              #поэтому ключи разные, хотя в редьюсе проще было отфильтровать все все все пары   
    
def reducefn2(band_id, vs):
    return vs

s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]

n = 1
for i in input0.keys():
    n = len(input0[i])
    break

threshold = 0.75 # s похожесть
from math import log 
r = (log(n * log(1. / threshold)) - log(log(n * log(1. / threshold)))) / log(1. / threshold)
r = int(r) # (1/b)^(1/r) = threshold, Mining of Massive Datasets, 91 стр

input0_with_r = {k: (v, r) for k, v in input0.iteritems()} # передаём информацию мапперу о r
s.map_input = mincemeat.DictMapInput(input0_with_r) 
s.mapfn = mapfn1
s.reducefn = reducefn1

input2 = s.run_server(password="") 

s2 = mincemeat.Server()
s2.map_input = mincemeat.DictMapInput(input2)
s2.mapfn = mapfn2
s2.reducefn = reducefn2

ans = s2.run_server(password="")
pairs = set()
for key, value in sorted(ans.iteritems()):
    for pair in value:
        if (pair[1], pair[0]) not in pairs:
            pairs.add(pair)
for p in pairs:
    print p            
