# encoding: utf-8
import mincemeat
import argparse
import util

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
        canopie_strs = k.split(";")
        n, t1, t2 = util.str_to_arg(" ", canopie_strs[0], "str int int")
        del canopie_strs[0]
        canopies_pairs = [(to_point(vc), vc) for vc in canopie_strs]
        if items:
            for p_str in items:
                p = to_point(p_str)
                for c, c_str in canopies_pairs:
                    if dist(c, p) <= t1:
                        print(c_str, p_str)
                        yield c_str, p_str
        else:
            raise Exception("Empty items")
    else:
        raise Exception("Empty key")


def reducefn2(k, vs):
    return vs


# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn11(k, items):
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
def reducefn11(k, vs):
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)


def reducefn2(k, vs):
    return vs


# parser = argparse.ArgumentParser()
# parser.add_argument("-n", help="Iterations count", required=True, type=int)
# parser.add_argument("-t1", help="T1 threshold ", required=True, type=float)
# parser.add_argument("-t2", help="T1 threshold ", required=True, type=float)
# parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required=True)
#
# args = parser.parse_args()
#
# # Начальные центроиды и количество итераций принимаются параметрами
# centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]
# arg_t1 = int(args.t1)
# arg_t2 = int(args.t2)

arg_t1 = 4
arg_t2 = 2

SHARD1 = [(0, 0), (0, 3), (1, 0), (1, 1), (1, 5), (1, 6), (2, 1), (2, 2), (2, 6)]
SHARD2 = [(4, 4), (3, 6), (5, 2), (5, 3), (6, 1), (6, 2)]

# Поиск центров зонтичных покрытий
s = mincemeat.Server()

# Просто единый формат данных, чтобы не писать отдельныфе функции с хитрым протаскиванием аргументов
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
print("Canopy:")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value))

canpies = ";".join(results.values()[0])
input2 = {}
input2["s1 " + key_input + ";" + canpies] = SHARD1_STR
input2["s2 " + key_input + ";" + canpies] = SHARD2_STR
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input2)
s.mapfn = mapfn2
s.reducefn = reducefn2
results = s.run_server(password="")

print("Canopy clusters:")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value))