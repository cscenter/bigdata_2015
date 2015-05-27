	# encoding: utf-8
import mincemeat
#Из курса mmds http://infolab.stanford.edu/~ullman/mmds/ch3.pdf: threshhold ~ (1/b)^(1/r) = s, r*b = n. Решение можно
#возьмем s = 0.8 - то есть ожидается схожесть в вероятностью 80%
#  тогда решение для b можно приблизить величиной 0.223n/ln(0.223n) с точностью до величины порядка ln(ln(n))

#первая итерация map-reduce разбивает на полосы, а вторая берет полиномиальный хэш и переводит его в упорядоченные пары
def mapfn(docid, docvector):
  import math
  b = math.ceil(0.223*len(docvector)/math.log(0.223*len(docvector)))
  r = math.ceil(len(docvector)/b)
  for i in range(int(b)):
      yield i, (docvector[i*int(r):min((i+1)*int(r), len(docvector))], docid)



def reducefn(k, vs):
  return vs



def mapfn2(layer, arrays):
    def hash(a, layer):
        p = 31
        q = 100000
        res = 1
        for v in a:
            res = ((res*p)+v)%q
        return res+(layer+1)*q

    hashes = dict()
    for array, doc in arrays:
        v = hash(array, layer)
        if v in hashes:
            hashes[v].append(doc)
        else:
            hashes[v] = [doc]

    for key in hashes:
        for i in range(len(hashes[key])):
            for j in range(i+1, len(hashes[key])):
                yield min(hashes[key][i], hashes[key][j]),max(hashes[key][i], hashes[key][j]) #упорядычиваем чтобы не было дубликатов.
                #ключом является один элемент а не пара, чтобы оптимизировать  reduce

def reducefn2(k, vs):
    return [(x, k) for x in vs]

s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]



s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
s.map_input = mincemeat.DictMapInput(results)
s.mapfn = mapfn2
s.reducefn = reducefn2
results = s.run_server(password="")
for v in results.values():
    for (a, b) in v:
        print("{} is similar to {}".format(a, b))
