# encoding: utf-8
import argparse
import math
import mincemeat

# первый mapper.
# на вход получает массив точек.
# на выходе даёт {центр зонта: ([точки ближе T2], [точки ближе T1])}.
def create_canopies(k, args):
    Ts, points = args
    T1, T2 = Ts

    import math
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    while points:
        from random import choice
        canopy = choice(points)
        points_to_delete = set()
        candidates = [] # dist < T1
        own_points = [] # dist < T2
        for point in points:
            distance = dist(canopy, point)
            if distance < T1:
                if distance < T2:
                    own_points.append(point)
                else:
                    candidates.append(point)
                points_to_delete.add(point)

        points = list(point for point in points if point not in points_to_delete)
        yield "%f,%f" % canopy, (own_points, candidates)


# первый reducer.
# никак не изменяет входящие данные. Так как точка обрабатывалась только один раз, она либо могла стать центром зонта,
# либо могла не стать. Те, которые стали, попали в reducer. Так как стали только один раз, получили список из 1 элемента.
# его же и возвращаем.
def id_canopies(canopy, points):
    return points[0]


# второй mapper.
# получает на вход {центр зонта: ([точки ближе T2], [ ближе T1])}.
# возвращает {точка: (Мой зонт, Возможный мой зонт)}.
def mark_point(canopy, points):
    own, candidates = points
    canopy = tuple(float(x) for x in canopy.split(','))
    for point in own:
        yield "%f,%f" % point, (canopy, None)

    for point in candidates:
        yield "%f,%f" % point, (None, canopy)


# второй reducer
# получает {точка: [(Мой зонт, Возможный мой зонт)]}.
# возвращает
# {точка: (мой первый гарантированный зонт, None)}
# если мы имеем хотя бы один "гарантированный" зонт, либо
# {точка: (None, [Мои возможные зонты])}
# в противном случае.
def reduce_marked(point, canopies):
    own = []
    candidate = set()
    for canopy in canopies:
        if canopy[0]:
            own.append(canopy[0])
        else:
            candidate.add(canopy[1])
    if own:
        return own[0], None
    else:
        return  None, candidate



# формируем кластеры как в исходном примере.
# исключение - переработан dist
# а за точкой тянутся её старые списки [<T2], [<T1]
def form_clusters(k, items):
    import math
    MAX_DIST = 100

    def dist(point, centroid):
        if point[1][0] and centroid[1][0] and point[1][0] != centroid[1][0]:
            return MAX_DIST
        if not point[1][0] and centroid[1][0] and centroid[1][0] not in point[1][1]:
            return MAX_DIST
        if not centroid[1][0] and point[1][0] and point[1][0] not in centroid[1][1]:
            return MAX_DIST
        if not point[1][0] and not centroid[1][0] and not set(point[1][1]).intersection(set(centroid[1][1])):
            return MAX_DIST
        return math.sqrt((point[0][0] - centroid[0][0])**2 + (point[0][1] - centroid[0][1])**2)

    cur_centroids, points = items
    for i in xrange(len(points)):
        points[i] = (tuple(float(x) for x in points[i][0].split(',')), points[i][1])
        min_dist = MAX_DIST
        min_c = -1
        for c in cur_centroids:
            d = dist(points[i], c)
            if d < min_dist:
                min_c = c
                min_dist = d
        yield "%f %f" % min_c[0], points[i]


# вычисляет новые центроиды как в исходном примере
def reduce_new_centroids(k, vs):
    new_cx = float(sum([float(v[0][0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v[0][1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)

def reducefn(k, vs):
    return vs

def dist(p1, p2):
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

def link_centroids_with_canopies(centroids, canopies):
    new_centroids = []
    for c in centroids:
        candidates = []
        found_main = False
        for canopy in canopies:
            d = dist(c, canopy)
            if d < radius[1]:
                new_centroids.append((c, (canopy, None)))
                found_main = True
                break
            elif d < radius[0]:
                candidates.append(canopy)
        if not found_main and candidates:
            new_centroids.append((c, (None, candidates)))
        elif not found_main:
            new_centroids.append((c, (None, None)))
    return new_centroids

SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]



parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
args = parser.parse_args()

s = mincemeat.Server()
radius = (1.5, 1.0) # T1, T2
input0 = {}
input0['set1'] = (radius, SHARD1)
input0['set2'] = (radius, SHARD2)
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = create_canopies
s.reducefn = id_canopies

results = s.run_server(password="",  port=8082)
canopies = [c for c in results]




s.map_input = mincemeat.DictMapInput(results)
s.mapfn = mark_point
s.reducefn = reduce_marked

results = s.run_server(password="")

input1 = {}
#после последнего reduce у нас все точки собрались в один result
#пытаемся снова симулировать разбиение на шарды
results = list(results.items())
for i in range(0, len(results), 10):
    input1['set%d' % i] = list()
    for point in results[i: i+10]:
        input1['set%d' % i].append(point)

temp_c = []
for can in canopies:
    temp_c.append(tuple(float(x) for x in can.split(',')))
canopies = temp_c

centroids = link_centroids_with_canopies(canopies[:], canopies)

input = {}
for k, v in input1.items():
    input[k] = (centroids, v)

for i in xrange(1,args.n):
    s.map_input = mincemeat.DictMapInput(input)
    s.mapfn = form_clusters
    s.reducefn = reduce_new_centroids
    results = s.run_server(password="")

    centroids = link_centroids_with_canopies(results.values(), canopies)
    for k, v in input1.items():
        input[k] = (centroids, v)

s.map_input = mincemeat.DictMapInput(input)
s.mapfn = form_clusters
s.reducefn = reducefn
results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, [v[0] for v in value]))
    print()