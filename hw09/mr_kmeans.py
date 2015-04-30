# encoding: utf-8
from __future__ import print_function, division
import mincemeat
import argparse

# для простоты kmeans и canopy clustering будут иметь одинаковые функции расстояния (евклидово)
# также для хранения списка зонтиков будет использоваться временный файл на DFS, но для упрощения
# в данной реализации будет применен обычный список, которые будет целиком передоваться между
# маппером и редьюсером (хотя на самом деле должно передаваться только имя файла на DFS)


def mapfn0(k, v):
    import math
    import json

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    (items, params) = v

    # этот список на самом деле на DFS; добавление -- append в файл
    canopies = []

    for point in items:
        emit = True
        for canopy in canopies:
            if dist(point, canopy) < params['t2']:
                emit = False
                break
        if emit:
            canopies.append(point)
            yield json.dumps(('centroids', params)), point


def reducefn0(k, vs):
    import math
    import json

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    (_, params) = json.loads(k)

    result = []

    for point in vs:
        emit = True
        for canopy in result:
            if dist(point, canopy) < params['t2']:
                emit = False
                break
        if emit:
            result.append(point)

    return result


# этот mapreduce относит точки к соответствующим кластерам
def mapfn1(k, v):
    import math

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    (items, params) = v

    cur_centroids = items[0]

    for point in items[1:]:
        for canopy in cur_centroids:
            if dist(point, canopy) < params['t1']:
                yield k, (point, canopy)


def reducefn1(k, vs):
    return vs


# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид


def mapfn2(k, items):
    import math

    min_dist_default = 100

    def dist(p1c, p2c):
        # if different clusters: return min_dist_default + 1
        (p1, c1) = p1c
        (p2, c2) = p2c
        if c1 != c2:
            return min_dist_default + 1
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    cur_centroids = items[0]

    for i in items[1:]:
        min_dist = min_dist_default
        min_c = -1
        for c in cur_centroids:
            if dist(i, c) < min_dist:
                min_c = c
                min_dist = dist(i, c)
        if min_c != -1:
            yield ('%f %f' % min_c[0]) + ';' + ('%f %f' % min_c[1]), i[0]


# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn2(k, vs):
    canopy = tuple(tuple(float(x) for x in y.split(' ')) for y in k.split(';'))[1]
    new_cx = sum(v[0] for v in vs) / len(vs)
    new_cy = sum(v[1] for v in vs) / len(vs)
    return (new_cx, new_cy), canopy


def reducefn3(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument('-n', help='Iterations count', required=True, type=int)

args = parser.parse_args()

# количество итераций принимается параметром

SHARD1 = [(0, 0), (0, 3), (1, 0), (1, 1), (1, 5), (1, 6), (2, 1), (2, 2), (2, 6)]
SHARD2 = [(4, 4), (3, 6), (5, 2), (5, 3), (6, 1), (6, 2)]

s = mincemeat.Server()

input0 = {}
input0['set1'] = SHARD1
input0['set2'] = SHARD2
s.map_input = mincemeat.DictParamsMapInput(input0, t1=2.0, t2=3.5)
s.mapfn = mapfn0
s.reducefn = reducefn0

# в качестве начальных центроидов берутся зонтичные центроиды
centroids = s.run_server(password='').values()[0]
print('canopies:', centroids)

# теперь отнесем каждую точку к соответствующему
input1 = {}
input1['set1'] = [centroids] + SHARD1
input1['set2'] = [centroids] + SHARD2
s.map_input = mincemeat.DictParamsMapInput(input1, t1=2.0, t2=3.5)
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password='')
for key, value in sorted(results.items()):
    print('%s: %s' % (key, value))
SHARD1NEW = results['set1']
SHARD2NEW = results['set2']

# положим каждый центроид в свой же зонтик
centroids = [(c, c) for c in centroids]

for i in xrange(1, args.n):
    s = mincemeat.Server()

    input2 = {}
    input2['set1'] = [centroids] + SHARD1NEW
    input2['set2'] = [centroids] + SHARD2NEW
    s.map_input = mincemeat.DictMapInput(input2)
    s.mapfn = mapfn2
    s.reducefn = reducefn2

    results = s.run_server(password='')
    centroids = list(results.values())
    print('centroid:', centroids)

# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server()
input3 = {}
input3['set1'] = [centroids] + SHARD1NEW
input3['set2'] = [centroids] + SHARD2NEW
s.map_input = mincemeat.DictMapInput(input3)
s.mapfn = mapfn2
s.reducefn = reducefn3
results = s.run_server(password='')

# тут печатаем результирующие кластеры
for key, value in sorted(results.items()):
    print('> %s: %s' % (key, value))
