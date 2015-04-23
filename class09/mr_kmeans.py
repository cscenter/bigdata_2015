# encoding: utf-8
import mincemeat
import argparse

# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn1(k, items):
  cur_centroids = items[0]
  del items[0]
  for i in items:
    min_dist = 100
    min_c = -1
    for c in cur_centroids:
      c = float(c)
      if abs(i - c) < min_dist:
        min_c = c
        min_dist = abs(i-c)
    yield str(min_c), str(i)

# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn1(k, vs):
    print k
    print vs
    old_c = float(k)
    new_c = float(sum([int(v) for v in vs])) / len(vs)
    return new_c

def reducefn2(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Comma-separated list of initial centroids")

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [int(c.strip()) for c in args.c.split(",")]

SHARD1 = [10, 20, 25, 27, 27, 32, 41, 49, 55, 72]
SHARD2 = [15, 16, 30, 35, 43, 44, 53, 67, 80, 81]
for i in xrange(1,args.n):
  s = mincemeat.Server() 

  # На каждой 
  input0 = {}
  input0['set1'] = [centroids] + SHARD1
  input0['set2'] = [centroids] + SHARD2
  s.map_input = mincemeat.DictMapInput(input0) 
  s.mapfn = mapfn1
  s.reducefn = reducefn1

  results = s.run_server(password="") 
  centroids = [c for c in results.itervalues()]

# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server() 
input0 = {}
input0['set1'] = [centroids] + SHARD1
input0['set2'] = [centroids] + SHARD2
s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="") 
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )

