# encoding: utf-8
import mincemeat


def map_1(doc_id, vector_and_bands):
    doc_vector, bands = vector_and_bands
    band_id = 1
    for i in xrange(0, len(doc_vector), bands):
        yield band_id, (doc_id, doc_vector[i:i+bands])
        band_id += 1


def reduce_1(band_id, vs):
    return vs


def map_2(band_id, docs):

    vector_hash = lambda vector: hash(str(vector))

    from collections import defaultdict
    hashes = defaultdict(list)

    for doc_id, doc_vector in docs:
        hashes[vector_hash(doc_vector)].append(doc_id)

    from itertools import combinations
    candidates = (pair for _, doc_ids in hashes.iteritems() if len(doc_ids) > 1
                  for pair in combinations(doc_ids, 2))

    for first, second in candidates:
        yield (first, second) if first < second else (second, first)


def reduce_2(doc, docs):
    return set(docs)  # may be too expensive


def rows_in_band(n):
    from math import log, ceil

    log_2 = log(2)
    n_log_2 = n * log_2
    log_n_log_2 = log(n_log_2)
    r = ceil((log_n_log_2 - log(log_n_log_2)) / log_2)

    return int(r)

s1 = mincemeat.Server()

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89, 63, 66, 96, 78, 19, 39, 53, 83, 20]
r = rows_in_band(len(input0['doc1']))
input0 = {k: (v, r) for k, v in input0.iteritems()}

s1.map_input = mincemeat.DictMapInput(input0)
s1.mapfn = map_1
s1.reducefn = reduce_1

input1 = s1.run_server(password='')

s2 = mincemeat.Server()
s2.map_input = mincemeat.DictMapInput(input1)
s2.mapfn = map_2
s2.reducefn = reduce_2

result = s2.run_server(password='')
for key, value in sorted(result.iteritems()):
    for x in value:
        print("%s and %s" % (key, x))