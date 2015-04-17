# encoding: utf-8
import mincemeat




def mapfn1(docid, docvector):
    from math import ceil

    def calc_r(vector_size):
    # Всё взято из mining of massive datasets, видимо лекция так же в освном базируется на
    # материале из этой книги :)

    # Choose a threshold t that defines how similar documents have to be in
    # order for them to be regarded as a desired “similar pair.” Pick a number
    # of bands b and a number of rows r such that br = n, and the threshold
    # t is approximately (1/b) ^ (1/r)
    # If avoidance of false negatives is important,
    # you may wish to select b and r to produce a threshold lower than t; if
    # speed is important and you wish to limit false positives, select b and r to
    # produce a higher threshold

    # фиксируем threshold, высчитываем r
        from math import log, ceil

        threshold = 0.75
        return int(ceil(threshold * log(vector_size)))


    strip_ind = 0

    vector_size = len(docvector)
    r = calc_r(vector_size)  # длина вектора в полосе
    b = int(ceil(vector_size * 1.0) / r) # количество полос
    for strip_ind in xrange(b):
        yield str(strip_ind), (docid, docvector[strip_ind * r: (strip_ind + 1) * r])


def reducefn1(k, vs):
    return vs


def mapfn2(strip_ind, v):
    from collections import defaultdict

# можно написать фабрику хэш функций, при необходимости увеличения коллизий можно определённый
# процент элементов вектора, как-то распределённых по вектору, не учитывать в подсчёте хэш функции.
# Здесь для простоты этого делать не будем.
    def hash1(l):
        h = 0
        for i in l:
            h = (h * 59 + i + int(strip_ind)) % 369731
        return h

    def hash2(l):
        h = 0
        for i in l:
            h = (h * 79 + i + int(strip_ind)) % 369709
        return h

    buckets = defaultdict(list)


    for docid, docvector in v:
        buckets[hash1(docvector)].append(docid)
        buckets[hash2(docvector)].append(docid)

#ищем пару похожих, лексиграфически упорядоченную
    for bucket in buckets.values():
        if (len(bucket) > 1):
            for i in xrange(len(bucket)):
                for j in xrange(i + 1, len(bucket)):
                    yield min(bucket[i], bucket[j]), max(bucket[i], bucket[j])

#расчитываем что похожих не так много, чтобы не влезало в память
def reducefn2(d1, similar_docs):
    return [(d1, d) for d in set(similar_docs)]


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

s.map_input = mincemeat.DictMapInput(results)
s.mapfn = mapfn2
s.reducefn = reducefn2

answ = s.run_server(password='')

for k, similar_docs in answ.items():
    for d1, d2 in similar_docs:
        print d1 + " similar on " + d2;
