# encoding: utf-8
import mincemeat
import argparse
import logging

import mincemeat
def mapreduce(mapfn, refucefn, data):
    s = mincemeat.Server()
    s.map_input = mincemeat.DictMapInput(data)
    s.mapfn = mapfn
    s.reducefn = refucefn
    results = s.run_server(password="")
    return results

logging.basicConfig(level=logging.INFO)

import math
def dist(p1, p2):
    return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

# value: (T2,T1) + points
# returns a possible canopies selection for current input by T2 criterion
def mapfn_find_candidates(k, vs):
    import math
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)
    T2, T1 = vs[0]
    canter_candidates = [vs[1]]
    yield T2, vs[1]
    #todo:ramdom shuffle
    import random
    #items = random.shuffle(vs[1:])
    items = vs[1:]
    for i in items:
        if all([dist(i, c) >= T2 for c in canter_candidates]):
            canter_candidates.append(i)
            yield T2, i
        else:
            logging.info("(map) %f %f is not valid for canopy" % i)

# reduces possible canopies selection by T2 criterion globally
def reducefn_find_candidates(k, vs):
    import math
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    T2 = k
    import random
    candidates = [random.choice(vs)]

    for i in vs:
        if i not in candidates and all((dist(i, c) >= T2 for c in candidates)):
            candidates.append(i)
        else:
            logging.info("(reduce) %f %f is not valid for canopy" % i)

    return candidates

def mapfn_select_canopies(k, vs):
    import math
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    found_canopies = vs[0]
    T1 = vs[1][1]
    for p in vs[2:]:
        valid = [c for c in found_canopies if dist(c, p) <= T1]
        yield k, (p, valid)

def reducefn_select_canopies(k, vs):
    return vs


def mapfn_k_means(k, vs):
    import math
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    centroids = vs[0]
    canopies = vs[1]

    for canopies in canopies:
        canopy_center, canopy_childs = canopies
        min_dist = 100500
        min_c = -1
        for c in centroids:
            if dist(canopy_center, c[0]) < min_dist:
                min_dist = dist(canopy_center, c[0])
                min_c = c[0]
        yield min_c, canopies


def reducefn_k_means(k, vs):
    new_cx = float(sum([x[0] for x, y in vs])) / len(vs)
    new_cy = float(sum([x[1] for x, y in vs])) / len(vs)
    return new_cx, new_cy

parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required = True)
parser.add_argument("-t2", help="T2", required=True, type=int)
parser.add_argument("-t1", help="T1", required=True, type=int)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]
T2 = args.t2
T1 = args.t1
SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]

# finding canopies in two steps

input_find_candidates = {}
input_find_candidates["set1"] = [(T2, T1)] + SHARD1
input_find_candidates["set2"] = [(T2, T1)] + SHARD2
candidates = mapreduce(mapfn_find_candidates, reducefn_find_candidates, input_find_candidates)[T2]
logging.info("RESULTS candidates=%s" % candidates)

input_select_canopies = {i: [candidates] + shard for i, shard in input_find_candidates.iteritems()}
canopies = mapreduce(mapfn_select_canopies, reducefn_select_canopies, input_select_canopies)
logging.info("RESULTS canopies=%s" % canopies)

# k-means
for i in xrange(args.n):
    centroids = [(c, {i for i in candidates if dist(c, i) <= T1}) for c in centroids]
    input_kmeans = {i: (centroids, canopies) for i, canopies in canopies.iteritems()}
    results = mapreduce(mapfn_k_means, reducefn_k_means, input_kmeans)
    centroids = results.values()

# output
#todo:dry
centroids = centroids = [(c, {center for center in candidates if dist(c, center) <= T1}) for c in centroids]
input_kmeans = {i: (centroids, canopies) for i, canopies in canopies.iteritems()}
results = mapreduce(mapfn_k_means, reducefn_select_canopies, input_kmeans)
centroids = results.values()

logging.info("===============================")

for center, childs in results.iteritems():
    logging.info(("(%f %f): " % center) + " " + ("%s" % [p for p, _ in childs]))
