# encoding: utf-8
import mincemeat
import argparse
import util
import math

# Алгоритм состоит из трех фаз: зонтичной кластеризации, маркировки данных по зонтичным кластерам и k-means по
# зонтичным коастерам.


# Фаза зонтичной кластеризации

# Алгоритм работает схождим образом как маппер и как редьюсер.
# Функции получает ключ в виде пары T1,T2 - настройки порогов для формирования зонтичных покрытий и список элементы
# которого точки данных.
#
# Как маппер, отдает центры зонтичных кластеров полученных из переданных данных от одног из шардов кластера.
# Центрами зонтичных кластеров выбираются точки, которые находятся на растоянии большем T2 от любых других центров таких
# покрытий.
# Как редьюсер собирает центры из всех щардов и делает такую же зонтичную кламтеризацию. Для удаления перекрывающихся
# классов.
#
# Стоит иметь ввиду у алгоритма есть слабое место. При втором проходе, при объединении кластеров могут быть утеряны
# крайние точки одного из кластеров которые не попадают буольше не в один другой. В зависимости от данных можно
# попробовать лучше рандомизировать порядок выбора и объединения центральных точек или уменьшить предельный интервал
# обьединения на втором этапе. Таким образом можно добится уменьшения вероятности потери точек.
# Однако при наличии больших массивов данных стоит иметь ввиду, что такие крайние точки  могут вообще быть выбросами,
# и в таком случае их потеря не повредит качеству кластеризации.
#
def mapfn1(k, items):
    import math
    import util

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    def to_point(s):
        return util.str_to_arg(" ", s, "float float")

    if k:
        n, t1, t2 = util.str_to_arg(" ", k, "str int int")
        canopies = []
        if items:
            for p_str in items:
                p = to_point(p_str)
                in_strict_bound = False
                for c in canopies:
                    if dist(c, p) <= t2:
                        in_strict_bound = True
                        break
                if not in_strict_bound:
                    canopies.append(p)
                    yield "c %d %d" % (t1, t2), p_str
        else:
            raise Exception("Empty items")
    else:
        raise Exception("Empty key")


def reducefn1(k, items):
    import math
    import util

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    def to_point(s):
        return util.str_to_arg(" ", s, "float float")

    if k:
        n, t1, t2 = util.str_to_arg(" ", k, "str int int")
        canopies = []
        canopie_strs = []
        if items:
            for p_str in items:
                p = to_point(p_str)
                in_strict_bound = False
                for c in canopies:
                    if dist(c, p) <= t2:
                        in_strict_bound = True
                        break
                if not in_strict_bound:
                    canopies.append(p)
                    canopie_strs.append(p_str)
        else:
            raise Exception("Empty items")
        return canopie_strs
    else:
        raise Exception("Empty key")


# Фаза маркировки

# Маппер получает первым аргументом строку элементов разделенных ";" первый элемент является парой значений T1 T2 далее
# идут центры зонтичной кластеризации, вторым аргументом список точек шарда. Далее происходит
# маркировка точек данных по вхождению в зонтичные кластеры.
def mapfn2(k, items):
    import math
    import util

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    def to_point(s):
        return util.str_to_arg(" ", s, "float float")

    if k:
        canopy_strs = k.split(";")
        n, t1, t2 = util.str_to_arg(" ", canopy_strs[0], "str int int")
        del canopy_strs[0]
        canopy_pairs = [(idx, to_point(vc)) for idx, vc in enumerate(canopy_strs)]
        if items:
            for p_str in items:
                p = to_point(p_str)
                for idx, c in canopy_pairs:
                    if dist(c, p) <= t1:
                        yield p_str, str(idx)
        else:
            raise Exception("Empty items")
    else:
        raise Exception("Empty key")


def reducefn2(k, vs):
    return vs


# Фаза k-means

# Маппер получает список, в котором первым элементом записан список уентров зонтичных кластеров, затем список центроидов
# а последущими элементами являются точки исходного набора данных с соответсвующими им идентификаторами центров
# зонтичных кластеров.
# Маппер производит пары (c, d) где c -- ближайший к точке центроид d -- точка данных. Пары производятся только для тех
# точек и центройдов которые принадлежат одному и томуже зонтичному кластеру.
def mapfn3(k, items):
    import math
    import util

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    if k:
        n, t1, t2 = util.str_to_arg(" ", k, "str int int")

        canopy_points = items[0]
        del items[0]
        centroids = items[0]
        del items[0]

        centroids_by_location_map_of_sets = {}
        for centroid in centroids:
            for idx, c in enumerate(canopy_points):
                if dist(c, centroid) <= t1:
                    if idx not in centroids_by_location_map_of_sets:
                        centroids_by_location_map_of_sets[idx] = {centroid}
                    else:
                        centroids_by_location_map_of_sets[idx].add(centroid)

        for point, locations_str in items:
            nearest_centroids = set()
            for v in locations_str:
                key = int(v)
                if key in centroids_by_location_map_of_sets:
                    nearest_centroids |= centroids_by_location_map_of_sets[key]

            min_dist = 100
            min_c = -1
            for c in nearest_centroids:
                if dist(point, c) < min_dist:
                    min_c = c
                    min_dist = dist(point, c)
            yield "%f %f" % min_c, "%f %f" % point
    else:
        raise Exception("Empty key")


# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn3(k, vs):
    # print("s:" + str(len(vs)))
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)


def reducefn4(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required=True, type=int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required=True)
parser.add_argument("-t1", help="T1 threshold", required=True, type=float)
parser.add_argument("-t2", help="T2 threshold", required=True, type=float)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]
arg_t1 = int(args.t1)
arg_t2 = int(args.t2)
arg_n = int(args.n)

if arg_t2 > arg_t1:
    raise Exception("T1 must be more then T2")

SHARD1 = [(0, 0), (0, 3), (1, 0), (1, 1), (1, 5), (1, 6), (2, 1), (2, 2), (2, 6)]
SHARD2 = [(4, 4), (3, 6), (5, 2), (5, 3), (6, 1), (6, 2)]

# Поиск центров зонтичных покрытий
s = mincemeat.Server()

# Просто единый формат данных
key_input = util.arg_to_str(" ", arg_t1, arg_t2)
SHARD1_STR = ["%f %f" % v for v in SHARD1]
SHARD2_STR = ["%f %f" % v for v in SHARD2]

input1 = {}
input1["s1 " + key_input] = SHARD1_STR
input1["s2 " + key_input] = SHARD2_STR

s.map_input = mincemeat.DictMapInput(input1)
s.mapfn = mapfn1
s.reducefn = reducefn1
results = s.run_server(password="")

# print("Canopy:")
# for key, value in sorted(results.items()):
# print("%s: %s" % (key, value))

canopies = results.values()[0]
canopies_str = ";".join(canopies)
input2 = {}
input2["s1 " + key_input + ";" + canopies_str] = SHARD1_STR
input2["s2 " + key_input + ";" + canopies_str] = SHARD2_STR

s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input2)
s.mapfn = mapfn2
s.reducefn = reducefn2
results = s.run_server(password="")

# Небольшая предобработка

def to_point(s):
    return util.str_to_arg(" ", s, "float float")


canopy_points = [to_point(c) for c in canopies]

SHARD1 = []
SHARD2 = []
fl = True
for key, values in results.items():
    if fl:
        SHARD1.append((to_point(key), values))
    else:
        SHARD2.append((to_point(key), values))
    fl = not fl

# Фаза k-means

for i in xrange(1, arg_n):
    s = mincemeat.Server()
    input3 = {}
    input3["s1 " + key_input] = [canopy_points] + [centroids] + SHARD1
    input3["s2 " + key_input] = [canopy_points] + [centroids] + SHARD2
    s.map_input = mincemeat.DictMapInput(input3)
    s.mapfn = mapfn3
    s.reducefn = reducefn3

    results = s.run_server(password="")
    centroids = [c for c in results.itervalues()]
    # print centroids

# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server()
input3 = {}
input3["s1 " + key_input] = [canopy_points] + [centroids] + SHARD1
input3["s2 " + key_input] = [canopy_points] + [centroids] + SHARD2
s.map_input = mincemeat.DictMapInput(input3)
s.mapfn = mapfn3
s.reducefn = reducefn4
results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s" % key)