# encoding: utf-8
import mincemeat
import argparse


# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn1(k, items):
    import math

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    cur_centroids = items[0]
    del items[0]
    for i in items:
        min_dist = 100
        min_c = -1
        for c in cur_centroids:
            if dist(i, c) < min_dist:
                min_c = c
                min_dist = dist(i, c)
        yield "%f %f" % min_c, "%f %f" % i


# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn1(k, vs):
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return new_cx, new_cy


def reducefn2(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required=True, type=int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required=True)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]

SHARD1 = [(0, 0), (0, 3), (1, 0), (1, 1), (1, 5), (1, 6), (2, 1), (2, 2), (2, 6)]
SHARD2 = [(4, 4), (3, 6), (5, 2), (5, 3), (6, 1), (6, 2)]
for i in xrange(1, args.n):
    s = mincemeat.Server()

    input0 = {'set1': [centroids] + SHARD1, 'set2': [centroids] + SHARD2}
    s.map_input = mincemeat.DictMapInput(input0)
    s.mapfn = mapfn1
    s.reducefn = reducefn1

    results = s.run_server(password="")
    centroids = [c for c in results.itervalues()]
    print centroids

# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server()
input0 = {'set1': [centroids] + SHARD1, 'set2': [centroids] + SHARD2}
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value))

