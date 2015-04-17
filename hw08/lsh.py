# encoding: utf-8
import mincemeat

# Нужно как-то разбить документы на полоски, желательно одинаковой длины. И так как я дальше
# #буду использовать обычный полимиальный хэш
# для меня важно, чтобы длина полосок у потенциально схожих документов была одинакова, иначе будет беда.
# нучжно придумать какую-то целочисленную функцию для длины полосок крайне плавно меняющуюся на бесконечности.
# Пришло в голову два очевидных решения - корень какой-то степени и логарифм.
# С корнем - s-curve мне не понравилась => испоьзую логарифм


def map1(docid, doc):
    import math

    n = len(doc)
    r = int(math.log(n))
    b = (n + r - 1) / r

    for strip in xrange(b):
        yield strip, (docid, doc[strip * r : r * (strip + 1)])


def reduce1(k, vs):
    #print 'ok'
    return vs

# Во втором мапе считаю хэши

def map2(strip, strips):

    from random import choice

    p = choice([2, 5, 13, 37, 57, 101, 7, 29, 47])
    pw = [1]
    sz = 0
    base = 2**31 - 1
    from collections import defaultdict

    hashResult = defaultdict(list)

    for (docid, doc) in strips:
        n = len(doc)
        ret = 0
        for j in xrange(n):
            if j > sz:
                sz += 1
                pw.append(pw[-1] * p)
            ret = (ret + doc[j] * pw[j]) & base

        hashResult[ret].append(docid)


    for candidat in hashResult.values():
        # кто-то с кем-то совпал
        if len(candidat) > 1:
            for x1 in candidat:
                for x2 in candidat:
                    if x1 < x2:
                        yield x1, x2


def reduce2(firstId, listId):
    return [x for x in set(listId)]


s = mincemeat.Server()

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]
input0['doc5'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]
input0['doc6'] = [1, 5, 10, 2, 4, 1, 2, 3, 4, 5, 6, 7, 0, 10, 2, 3, 4, 5, 6]
input0['doc7'] = [1, 5, 2, 2, 4, 1, 2, 3, 4, 5, 6, 7, 0, 10, 2, 3, 4, 5, 6]
input0['doc8'] = [1, 3, 4, 2, 4, 1, 2, 3, 4, 5, 6, 7, 0, 10, 2, 3, 4, 5, 6, 8]
input0['doc9'] = [1, 5, 10, 2, 4, 1, 2, 3, 4, 5, 6, 7, 0, 10, 2, 3, 4, 5]


s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = map1
s.reducefn = reduce1


results = s.run_server(password="")


s.map_input = mincemeat.DictMapInput(results)
s.mapfn = map2
s.reducefn = reduce2

results = s.run_server(password="")


for key, value in sorted(results.items()):
    for x in value:
        print("%s and %s" % (key, x) )
