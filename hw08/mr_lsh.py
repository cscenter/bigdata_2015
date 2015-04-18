# encoding: utf-8
import mincemeat

# Приближенное значение порога можно вычислить по формуле threshold = (1/b) ^ (1/r), а n = r * b
# Возьмём порог s=0.7, тогда решая уравнение (1/b)^(1/r) = 7/10
# получим r * ln(10/7) = W(n * ln(10/7))    W(x) - Lambert W-function приближенно ln x - ln ln x

# Первый mapreduce разбивает на rows столбцов каждый вектор
# Второй раскидывает идентификаторы документов по корзинам

def mapfn(docid, docvector):
    from math import log, ceil
    n = len(docvector)
    x = n * log(10.0/7.0)
    rows = (log(x) - log(log(x))) / log(10.0/7.0)
    rows = int(ceil(rows))
    bands = int(ceil(n / rows))

    for b in range(bands):
        yield docid, (docvector[rows * b : rows * (b + 1)])

def reducefn(k, vs):
    return vs

def mapfn2(docid, vectors):
    import hashlib, json
    bands = len(vectors)
    for v in vectors:
        backet = 0
        for value in v:
            m = hashlib.md5()
            bts = str(value).encode("utf-8")
            m.update(bts)
            digest = m.hexdigest()
            backet += int(digest, 16) % bands
        yield str(backet), docid
    

def reducefn2(k, vs):
    # убираем дубликаты
    return set(vs)

s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]

s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn
s.reducefn = reducefn

result1 = s.run_server(password="") 

# for key, value in sorted(result1.items()):
#     print("%s: %s" % (key, value) )

s.map_input = mincemeat.DictMapInput(result1) 
s.mapfn = mapfn2
s.reducefn = reducefn2

result2 = s.run_server(password="")

for key, value in sorted(result2.items()):
    print("%s: %s" % (key, value) )
