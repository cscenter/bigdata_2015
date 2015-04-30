# encoding: utf-8
import mincemeat
import argparse

def mapfn1(k, items):
    T1 = items[0][0]
    T2 = items[0][1]

    import math    
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    points = [i for i in items[1]]
    points_len = len(points)
    canopy_points = []
    while points_len != 0:
        from random import randint
        c = points[randint(0, points_len - 1)]
        current_conapy = []
        current_conapy.append(c)
        points.remove(c)
        points_len -= 1
        elements_to_delete = []
        for i in range(0, points_len):
            d = dist(c, points[i])
            if d < T1:
                current_conapy.append(points[i])
                if d < T2: 
                    elements_to_delete.append(points[i])
        points_len -= len(elements_to_delete)
        for i in elements_to_delete:
            points.remove(i)        
        canopy_points.append(current_conapy)    
    
    for l in canopy_points:
        x = 0
        y = 0
        for l2 in l:
            x += l2[0]
            y += l2[1]    
        yield str(T1) + ',' +  str(T2), (float(x) / len(l), float(y) / len(l))

def reducefn1(k, items):
    T1, T2 = k.split(',')
    T1 = int(T1)
    T2 = int(T2)

    import math    
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    points = [i for i in items]
    points_len = len(points)
    canopy_points = []
    while points_len != 0:
        from random import randint
        c = points[randint(0, points_len - 1)]
        current_conapy = []
        current_conapy.append(c)
        points.remove(c)
        points_len -= 1
        elements_to_delete = []
        for i in range(0, points_len):
            d = dist(c, points[i])
            if d < T1:
                current_conapy.append(points[i])
                if d < T2: 
                    elements_to_delete.append(points[i])
        points_len -= len(elements_to_delete)
        for i in elements_to_delete:
            points.remove(i)        
        canopy_points.append(current_conapy)    
    
    all_canopy = []
    for l in canopy_points:
        x = 0
        y = 0
        for l2 in l:
            x += l2[0]
            y += l2[1]
        all_canopy.append((float(x) / len(l), float(y) / len(l)))
    return all_canopy    

def mapfn2(k, items):
    import math    
    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    T1 = items[0][0]
    T2 = items[0][1]
    T1 = int(T1)
    T2 = int(T2)
    canopies = items[1]
    points = items[2]    
    for canopy in canopies:
        for p in points:
            if dist(canopy, p) < T1:
                yield "%f %f" % canopy, p

def reducefn2(k, items):
    q = k.split()
    return items
    
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
    min_c = -1
    for c in cur_centroids:
      if dist(i, c) < min_dist:
        min_c = c
        min_dist = dist(i, c)
    yield "%f %f" % min_c, "%f %f" % i

# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn3(k, vs):
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)

def reducefn4(k, vs):
    return vs

parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required = True)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]

SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]

T1 = 3
T2 = 2
input0 = {}
input0['set1'] = [[T1, T2]] + [SHARD1]
input0['set2'] = [[T1, T2]] + [SHARD2]
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn1
canopy_coordinates = s.run_server(password="")
print canopy_coordinates
print ''

input0 = {}
input0['set1'] = [[T1, T2]] + [canopy_coordinates[str(T1) + ',' + str(T2)]] + [SHARD1] 
input0['set2'] = [[T1, T2]] + [canopy_coordinates[str(T1) + ',' + str(T2)]] + [SHARD2]
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn2
s.reducefn = reducefn2
results = s.run_server(password="")
print results

'''
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

'''