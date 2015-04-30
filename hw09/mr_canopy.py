# encoding: utf-8
import mincemeat
import argparse


def mapfn1(k, points):
    """
    Первый этап MapReduce выбирает центры зонтиков.
    Mapper просто пробегает по точкам шарда и проверяет, может ли текущая точка стать центром нового зонтика (т.е.
     проверка на то, что точка не входит ни в один Т2 радиус уже имеющихся центров). Если может, то добавляем её в список
     центров.
    Reducer у нас один, и делает он то же самое, что и mapper (т.е. по сути удаляет дубликаты).
    Обратите внимание, что при достаточно близких Т1 и Т2 reducer может удалить лишние дубликаты,
     и каким-то точкам не достанется зонтик. Поэтому рекомендую выбирать Т1 > 1.25 T2 (очень примерно)
    Еще замечание: все промежуточные результаты записываются на DFS, но у нас игрушечный пример,
     поэтому все происходит в RAM.
    """
    # squared euclidean metric, for better performance
    dist = lambda x, y: sum(map(lambda x_i, y_i: (x_i - y_i) ** 2, x, y))

    t2, t1 = points[0]  # inner and outer thresholds
    del points[0]

    canopy_centers = [points[0]]
    yield t2, points[0]
    # always returns t2 as key to share it with reducer
    # we need only one reducer in this MR step, so it's not the worst idea

    for point in points:
        if all([dist(point, c) >= t2 for c in canopy_centers]):
            canopy_centers.append(point)
            yield t2, point


def reducefn1(t2, canopy_centers):
    dist = lambda x, y: sum(map(lambda x_i, y_i: (x_i - y_i) ** 2, x, y))

    unique_canopy_centers = [canopy_centers[0]]

    for point in canopy_centers:
        if all((dist(point, c) >= t2 for c in unique_canopy_centers)):
            unique_canopy_centers.append(point)

    return unique_canopy_centers


def mapfn2(k, points):
    """
    Второй этап MapReduce помечает точки центрами зонтиков.
    Mapper делает довольно очевидную вещь: для каждой точки вычисляет дистанции между ней
     и центрами зонтиков, и приписывает зонтик к точки если дистанция не больше T1.
    """
    dist = lambda x, y: sum(map(lambda x_i, y_i: (x_i - y_i) ** 2, x, y))

    canopy_centers = points[0]
    t2, t1 = points[1]  # inner and outer thresholds
    del points[0:2]

    for point in points:
        yield k, (point, {center for center in canopy_centers if dist(center, point) <= t1})


def reducefn2(k, vs):
    return vs


def mapfn3(k, items):
    """
    Самый обычный k-means. Главное отличие от аналогичного из mr_kmeans.py, конечно же, в использовании зонтиков.
    Вместе с точками на вход mapper получает центроиды. И центроиды, и точки помечены соответствующими им центрами
     зонтиков. Для того, чтобы считать расстояние от точки до центроиды, нужно убедиться, что у точки есть общий зонтик
     хотя бы с одним из центроидов.
    """
    dist = lambda x, y: sum(map(lambda x_i, y_i: (x_i - y_i) ** 2, x, y))

    current_centroids, points_with_canopies = items

    inf = 1e5

    for point_with_canopies in points_with_canopies:
        point, point_canopies = point_with_canopies
        if any((point_canopies & centroid_canopies for _, centroid_canopies in current_centroids)):
            min_dist, closest_centroid = min(map(
                lambda x: (dist(x[0], point), x[0]) if point_canopies & x[1] else (inf, x[0]), current_centroids))
            yield closest_centroid, point_with_canopies


def reducefn3(k, vs):
    new_cx = float(sum([x[0] for x, y in vs])) / len(vs)
    new_cy = float(sum([x[1] for x, y in vs])) / len(vs)
    return new_cx, new_cy


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required=True, type=int)
parser.add_argument("-i", help="T2, inner threshold", required=True, type=int)
parser.add_argument("-o", help="T1, outer threshold", required=True, type=int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required=True)

args = parser.parse_args()

SHARD1 = [(0, 0), (0, 3), (1, 0), (1, 1), (1, 5), (1, 6), (2, 1), (2, 2), (2, 6)]
SHARD2 = [(4, 4), (3, 6), (5, 2), (5, 3), (6, 1), (6, 2)]

centroids = [(float(center.split(",")[0]), float(center.split(",")[1])) for center in args.c.split(";")]
K = len(centroids)
t2 = args.i ** 2  # squared because of squared euclidean metric
t1 = args.o ** 2
thresholds = (t2, t1)

""" First Canopy MR """

input0 = {'set1': [thresholds] + SHARD1, 'set2': [thresholds] + SHARD2}

s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn1
results = s.run_server(password="")

canopy_centers = results[thresholds[0]]

""" Second Canopy MR """

input1 = {k: [canopy_centers] + v for k, v in input0.iteritems()}

s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input1)
s.mapfn = mapfn2
s.reducefn = reducefn2
results1 = s.run_server(password="")

""" KMeans """


def print_dict(d):
    for k, v in d.iteritems():
        print k, ' : ', v
    print ''


dist = lambda x, y: sum(map(lambda x_i, y_i: (x_i - y_i) ** 2, x, y))

for i in xrange(args.n):
    centroids = [(centroid, {center for center in canopy_centers if dist(centroid, center) <= t1})
                 for centroid in centroids]

    input2 = {point: (centroids, point_with_canopies) for point, point_with_canopies in results1.iteritems()}

    s = mincemeat.Server()
    s.map_input = mincemeat.DictMapInput(input2)
    s.mapfn = mapfn3
    s.reducefn = reducefn3
    results = s.run_server(password="")

    centroids = results.values()

centroids = [(centroid, {center for center in canopy_centers if dist(centroid, center) <= t1})
             for centroid in centroids]

input2 = {point: (centroids, point_with_canopies) for point, point_with_canopies in results1.iteritems()}

s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input2)
s.mapfn = mapfn3
s.reducefn = reducefn2
results = s.run_server(password="")

for center, item in results.iteritems():
    print center, ' : ', [point for point, _ in item]