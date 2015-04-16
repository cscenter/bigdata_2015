# encoding: utf-8
import mincemeat

# будет применена последовательность из нескольких mar-reduce
# первый этап: разбиение векторов документов на b полос, возвращает b списков, в каждом по вектору от каждого документа
# второй: сравнение хешей получившихся векторов, возвращает "корзины" с id документов, чьи вектора совпали
#
#

# пусть n -- длина вектора характеристик, b -- количество полос, r -- количество строк в полосе
# (1/b) ** (1/r) = 1/2 (threshold) и (b * r) = n   =>   b = 2 ^ r  и  r * 2 ^ r = n ,  это Lambert W Function,
# но мы будем считать натуральный логарифм, получается похоже:
# http://www.wolframalpha.com/input/?i=plot+W%28x%29%2C+W%281000%29%2Flog%28500%29+log%28x%2F2%29%2C+x+%3D+1+to+1000
#
# количество корзин k <= количество полос * количество документов.
# равенство соблюдается, только если все хеши на всех полосах окажутся различными
#

def mapfn1(docid, docvector):
    from math import log, ceil

    r = int(ceil(0.845 * log(len(docvector) / 2)))      # для небольших векторов характеристик получается маловато,
                                                        # хотя в теории вроде должно быть так
    b = int(ceil(1. * len(docvector) / r))

    for band in range(b):
        yield band, (docid, docvector[r * band : r * (band + 1)])


def reducefn1(k, vs):
    return vs

s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32, 10]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32, 10]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 50, 77, 30, 32, 10]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89, 63, 66, 96, 78, 19, 39, 53, 83, 20, 10]


input1 = {}
for doc in ['doc1', 'doc2', 'doc3', 'doc4']:
    input1[doc] = 10 * input0[doc]
input1['doc5'] = 5 * (input0['doc1'] + input0['doc4'])
input1['doc6'] = 5 * (input0['doc4'] + input0['doc2'])


s.map_input = mincemeat.DictMapInput(input1)
s.mapfn = mapfn1
s.reducefn = reducefn1

# получаем список полос
results1 = s.run_server(password="")


def mapfn2(bandid, bands):
    # нужно сравнить находящиеся на одном уровне полосы разных документов и сообщить, если будут совпадения
    from collections import defaultdict
    from random import randint

    r = len(bands[0][1])
    rand = [randint(0, 1000) / 1000. for _ in range(r)]     # случайные значения для хеш-функции
    def hash(l):
        f = sum(l[i] * rand[i] for i in range(r))
        return f - int(f) + bandid      # дробная часть + номер полосы
        # номер полосы добавляем, чтобы хеши были в разных интервалах для разных полос, хотя это не используется

    ht = defaultdict(list)
    for doc, values in bands:
        h = hash(values)
        ht[h].append(doc)
    for bucket in ht.values():
        if len(bucket) > 1:
            # есть повторения, возвращаем пары похожих
            # сортируем, чтобы первый элемент пары был всегда меньше второго -- так меньше мороки потом
            bucket = sorted(bucket)
            for i in range(len(bucket)):
                for j in range(i + 1, len(bucket)):
                    yield bucket[i], bucket[j]

def reducefn2(d1, vs):
    return [(d1, d2) for d2 in set(vs)]


s.map_input = mincemeat.DictMapInput(results1)
s.mapfn = mapfn2
s.reducefn = reducefn2

# получаем списки похожих документов
results2 = s.run_server(password="")

for p in sorted(results2.values()):
    for k, ls in sorted(p):
        print("({0}, {1})".format(k, ls))

