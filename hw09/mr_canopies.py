# encoding: utf-8
import mincemeat
import argparse
import math


# mapfn0 и reducefn0 находят центры зонтиков
# mapfn1 и reducefn1 для каждой точки формируют список зонтиков, в которые она попадает
# после этого каждую точку мы рассматриваем сразу с этим списком
# mapfn2 и reducefn2 представляют собой итерацию k-means, единственное отличие -- если у точек нет
# общих зонтиков, мы расстояние между ними считаем бесконечно большим


# получает список элементов + пару (T1, T2)
# возвращает центры зонтиков
def mapfn0(k, items):
    import random
    import math

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    T1, T2 = items[0]
    del items[0]
    free_points = items[:]
    cur_canopies = set()
    while len(free_points) > 0:
        cur_center = random.choice(free_points)
        free_points.remove(cur_center)
        cur_canopies.add(cur_center)

        for p in free_points:
            d = dist(p, cur_center)
            if d < T1:
                if d < T2:
                    free_points.remove(p)
    for center in cur_canopies:
        yield "canopy_centers", center

# просто возвращает получившиеся зонтики (без повторов)
def reducefn0(k, vs):
    return [v for v in set(vs)]


# для каждой точки выдает номера зонтиков, в которые она попадает
def mapfn1(k, items):
    import math

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    T1, T2 = items[0]
    del items[0]
    canopy_centers = items[0]
    del items[0]

    for p in items:
        for i, c in enumerate(canopy_centers):
            d = dist(p, c)
            if d < T1:
                yield p, i

# просто возвращает получившиеся зонтики в виде множества
def reducefn1(k, vs):
    return set(vs)


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required = True)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]

SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]

# находим зонтики
T1, T2 = 3.0, 2.0
s = mincemeat.Server()

input0 = {}
input0['set1'] = [(T1, T2)] + SHARD1
input0['set2'] = [(T1, T2)] + SHARD2
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn0
s.reducefn = reducefn0

results = s.run_server(password="")
canopy_centers = results["canopy_centers"]
print("canopy centers: {0}".format(canopy_centers))


# нужно для каждой точки определить множество зонтиков, в которое она попадает
s = mincemeat.Server()

input0 = {}
input0['set1'] = [(T1, T2)] + [canopy_centers] + SHARD1
input0['set2'] = [(T1, T2)] + [canopy_centers] + SHARD2
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn1

# results[p] -- множество зонтиков, в которые попадает p
results = s.run_server(password="")

##############
# фаза k-means
##############

# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn3(k, items):
    import math

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    cur_centroids = items[0]
    del items[0]
    for i in items:
        min_dist = 100
        min_c = []
        for c in cur_centroids:
            # проверяем, есть ли у них общие зонтики
            if len(i[1].intersection(c[1])) == 0:
                continue

            # считаем dist только если пересечение непусто
            if dist(i[0], c[0]) < min_dist:
                min_c = c
                min_dist = dist(i[0], c[0])
        yield "%f %f" % min_c[0], "%f %f" % i[0]


# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn3(k, vs):
        new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
        new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
        return (new_cx, new_cy)

def reducefn4(k, vs):
        return vs


# теперь каждая точка представляет собой координаты плюс список зонтиков, в которые она попадает
SHARD1 = [(p, results[p]) for p in SHARD1]
SHARD2 = [(p, results[p]) for p in SHARD2]

def dist(p1, p2):
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

centroids = [(centr, set([i for i, can in enumerate(canopy_centers) if dist(centr, can) < T1])) for centr in centroids]

# запускаем k-means
for i in xrange(1,args.n):
    s = mincemeat.Server()

    input0 = {}
    input0['set1'] = [centroids] + SHARD1
    input0['set2'] = [centroids] + SHARD2
    s.map_input = mincemeat.DictMapInput(input0)
    s.mapfn = mapfn3
    s.reducefn = reducefn3

    results = s.run_server(password="")
    centroids = [c for c in results.itervalues()]
    print centroids
    centroids = [(centr, set([i for i, can in enumerate(canopy_centers) if dist(centr, can) < T1])) for centr in centroids]

# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server()
input0 = {}
input0['set1'] = [centroids] + SHARD1
input0['set2'] = [centroids] + SHARD2
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn3
s.reducefn = reducefn4
results = s.run_server(password="")
for key, value in sorted(results.items()):
        print("%s: %s" % (key, value) )
