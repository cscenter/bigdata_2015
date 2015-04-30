# encoding: utf-8
import mincemeat
import argparse
# при больших T1 мы не получаем никаких выйгрышей от зонтичной кластеризации
# при маленьких T1 мы мы можем получить "изолированные" зонтики, в которые не войдёт ни один центроид
# при больших T2 у нас получится мало зонтиков это не очень хорошо
# при маленьких T2 у нас получится очень много зонтиков, что расточительно

# этот map-reduce служит для нахождения центров зонтиков
def mapfn0(k, items):
    import math
    import random
    T1 = 10
    T2 = 1
    class canopy:
        def __init__(self, initial_point):
            self.points = []
            self.points.append(initial_point)

        def get_centroid(self):
            centroid_x = float(sum([float(point[0]) for point in self.points])) / len(self.points)
            centroid_y = float(sum([float(point[1]) for point in self.points])) / len(self.points)
            return (centroid_x, centroid_y)
        def add_point(self, new_point):
            self.points.append(new_point)
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)
    used_num_of_points = []
    while len(used_num_of_points) < len(items):
        num_of_point = random.randint(0, len(items) - 1)
        if num_of_point not in used_num_of_points:
            used_num_of_points.append(num_of_point)
            my_canopy = canopy(items[num_of_point])
            for j in range(0, len(items)):
                if j not in used_num_of_points:
                    if dist(items[j], items[num_of_point]) < T1:
                        my_canopy.add_point(items[j])
                        if dist(items[j], items[num_of_point]) < T2:
                            used_num_of_points.append(j)
            my_centroid = my_canopy.get_centroid()
            yield "centroid_of_canopy", "%f %f" % my_centroid

def reducefn0(k, vs):
    import math
    import random
    T1 = 10
    T2 = 1
    output_centroids = []
    class canopy:
        def __init__(self, initial_point):
            self.points = []
            self.points.append(initial_point)

        def get_centroid(self):
            centroid_x = float(sum([float(point[0]) for point in self.points])) / len(self.points)
            centroid_y = float(sum([float(point[1]) for point in self.points])) / len(self.points)
            return (centroid_x, centroid_y)
        def add_point(self, new_point):
            self.points.append(new_point)
    def dist(p1, p2):
        return math.sqrt((float(p1[0]) - float(p2[0]))**2 + (float(p1[1]) - float(p2[1]))**2)
    used_num_of_points = []
    while len(used_num_of_points) < len(vs):
        num_of_point = random.randint(0, len(vs) - 1)
        if num_of_point not in used_num_of_points:
            used_num_of_points.append(num_of_point)
            my_canopy = canopy(vs[num_of_point].split())
            for j in range(0, len(vs)):
                if j not in used_num_of_points:
                    if dist(vs[j].split(), vs[num_of_point].split()) < T1:
                        my_canopy.add_point(vs[j].split())
                        if dist(vs[j].split(), vs[num_of_point].split()) < T2:
                            used_num_of_points.append(j)
            my_centroid = my_canopy.get_centroid()
            output_centroids.append(my_centroid)
    return output_centroids
# этот map-reduce служит для построения зонтиков
# каждый получившийся зонтик при желании можно хранить в отдельном шарде
def mapfn0_1(k, items):
    import math
    T1 = 10
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    canopy_centroids = items[0]
    del items[0]
    for can_centr in canopy_centroids:
        for j in items:
            if dist(j, can_centr) <= T1:
                yield "%f %f" % can_centr, "%f %f" % j

def reducefn0_1(k, vs):
    return vs

# этот map-reduce производит лист пар (точка - ближайший) к ней центроид
def mapfn1_1(k, items):
    import math
    T1 = 10
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)
    centroids = items[0]
    for centr in centroids:
        if dist(centr, (float(k.split()[0]), float(k.split()[1]))) < T1:
            for j in items[1]:
                yield j, "%f %f" % centr

def reducefn1_1(k, vs):
    import math
    T1 = 10
    def dist(p1, p2):
        return math.sqrt((float(p1[0]) - float(p2[0]))**2 + (float(p1[1]) - float(p2[1]))**2)
    nearest_v = vs[0]
    nearest_dist = dist(k.split(), vs[0].split())
    for v in vs:
        if dist(k.split(), v.split()) < nearest_dist:
            nearest_v = v
            nearest_dist = dist(k.split(), v.split())
    return nearest_v
# этот map-reduce , используя результат предыдущего map-reduce, оптимизирует наши центроиды
def mapfn1_2(k, items):
        yield items, k
def reducefn1_2(k, vs):
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)


def reducefn2(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required=True, type=int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,2;2,6;6,2", required=True, type=str)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]
print 'initial centroids'
print centroids

SHARD1 = [(0, 0), (0, 3), (1, 0), (1, 1), (1, 5), (1, 6), (2, 1), (2, 2), (2, 6)]
SHARD2 = [(4, 4), (3, 6), (5, 2), (5, 3), (6, 1), (6, 2)]

#находим центры зонтиков
s0 = mincemeat.Server()
input0 = {}
input0['set1'] = SHARD1
input0['set2'] = SHARD2
s0.map_input = mincemeat.DictMapInput(input0)
s0.mapfn = mapfn0
s0.reducefn = reducefn0
results = s0.run_server(password="")
canopy_centroids = results.items()[0][1]

#строим зонтики
s0_1 = mincemeat.Server()
input0 = {}
input0['set1'] = [canopy_centroids] + SHARD1
input0['set2'] = [canopy_centroids] + SHARD2
s0_1.map_input = mincemeat.DictMapInput(input0)
s0_1.mapfn = mapfn0_1
s0_1.reducefn = reducefn0_1
results = s0_1.run_server(password="")
canopies = results

# оптимизируем центроиды
for i in xrange(1, args.n):
    s1 = mincemeat.Server()
    input0 = {}
    for key, value in sorted(canopies.items()):
        input0[key] = [centroids] + [value]
    s1.map_input = mincemeat.DictMapInput(input0)
    s1.mapfn = mapfn1_1
    s1.reducefn = reducefn1_1
    results = s1.run_server(password="")
    s2 = mincemeat.Server()
    s2.map_input = mincemeat.DictMapInput(results)
    s2.mapfn = mapfn1_2
    s2.reducefn = reducefn1_2
    results = s2.run_server(password="")
    centroids = [c for c in results.itervalues()]
    print 'centroids'
    print centroids

# кластеризуем
s1 = mincemeat.Server()
input0 = {}
for key, value in sorted(canopies.items()):
    input0[key] = [centroids] + [value]
s1.map_input = mincemeat.DictMapInput(input0)
s1.mapfn = mapfn1_1
s1.reducefn = reducefn1_1
results = s1.run_server(password="")
s2 = mincemeat.Server()
s2.map_input = mincemeat.DictMapInput(results)
s2.mapfn = mapfn1_2
s2.reducefn = reducefn2
results = s2.run_server(password="")
print 'result'
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value))