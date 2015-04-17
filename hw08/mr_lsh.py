# encoding: utf-8


import mincemeat

# требуется задать константы R и B

def mapfn1(docid, docvector):
    import math
    R = 6
    for i in range(int(math.ceil(float(len(docvector)) / R))):
        yield docid, docvector[i * R : (i + 1) * R]


def reducefn1(k, vs):
    return vs


# после первого mr мы имеем словарь {(docid, stripid) : [strip_values]}
# закидываем (docid, stripid) в тот бакет, который вернул нам md5.
def mapfn2(docinfo, docvector):
    from operator import mod
    K = 1000

    def poly_hash(docvector):
        import hashlib
        m = hashlib.md5()
        str = "".join(chr(x) for x in docvector)
        m.update(str)
        return mod(int(m.hexdigest(), 16), K)

    yield str(poly_hash(docvector)), docinfo

# третий mr просто делает результаты чуть красивее,
# убирая дубликаты из разных корзин и дубликаты с реверсным порядком.
def reducefn2(k, vs):
    candidates = []
    for doc1, strip1 in vs:
        for doc2, strip2 in vs:
            if doc1 == doc2:
                continue
            if strip1 != strip2:
                continue
            if (doc2, doc1) not in candidates:
                candidates.append((doc1, doc2))
    return candidates

def mapfn3(bucket, candidates):
    for pair in candidates:
        if pair[1] < pair[0]:
            pair = (pair[1], pair[0])
        yield pair

def reducefn3(doc, docs):
    docs = set(docs)
    return docs

s = mincemeat.Server()

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89, 63, 66, 96, 78, 19, 39, 53, 83, 20]

s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="")
"""
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )
"""

# нумеруем подотрезки, добавляем эти номера в ключ
# т.е. превращаем {docid: [[strip_vals]]} в {(docid, stripid): [strip_vals]}
input1 = {}
for res in results.items():
    for i in range(len(res[1])):
        input1[(res[0], i)] = res[1][i]

s.map_input = mincemeat.DictMapInput(input1)
s.mapfn = mapfn2
s.reducefn = reducefn2
results = s.run_server(password="")

s.map_input = mincemeat.DictMapInput(results)
s.mapfn = mapfn3
s.reducefn = reducefn3
results = s.run_server(password="")


res = []
for key, value in sorted(results.items()):
    for p in value:
        res.append((key, p))
print(res)