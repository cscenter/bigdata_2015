# encoding: utf-8
import mincemeat
import argparse

#Строим canopy centers:
#mapper строит центры зонтиков в пределах доступных ему данных и выдает все на один reducer
#reducer убирает сильно перекрывающиеся центроиды
#
def mapcnp1(k, items):
  if not k or not items:
      raise Exception("Invalid mapper input")
  import math

  def dist(p1, p2):
  	return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

  T1, T2 = items[0]
  del items[0]

  canopy_centers = []
  for p in items:
      f = False
      for canopy in canopy_centers:
          if dist(p, canopy) < T2:
              f = True
              break
      if f:
          continue
      canopy_centers.append(p)
  for canopy in canopy_centers:
      yield "{};{}".format(T1, T2), "{};{}".format(canopy[0], canopy[1])


def reducecnp1(k, vs):
    import math

    def dist(p1, p2):
  	  return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    T1, T2 = (float(x) for x in k.split(";"))
    final_canopy_centers = []
    canopy_list = [list(float(x) for x in s.split(";")) for s in vs]
    for canopy in canopy_list:
        f = False
        for final_canopy in final_canopy_centers:
            if dist(canopy, final_canopy) < T2/2:
              f = True
              break
        if f:
            continue
        final_canopy_centers.append(canopy)
    return final_canopy_centers


#получаем на вход списко центров canopy и точки, выдаем обратно зонты сопоставленные каждой точке
def mapcnp2(k, items):
    import math

    def dist(p1, p2):
  	  return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    T1, T2 = items[0]
    del items[0]
    canopy_list = items[0]
    del items[0]
    for p in items:
        res = []
        f = False
        for id, canopy in enumerate(canopy_list):
            if dist(p, canopy) < T2:
                yield "{};{}".format(p[0], p[1]), str(id)
                f = True
                break

            if dist(p, canopy) < T1:
                res.append(str(id))
        if f:
            continue
        #print(res)
        for id in res:
            yield "{};{}".format(p[0], p[1]), str(id)

def reducecnp2(k, vs):
    return [int(x) for x in vs]
# Маппер получает список, в котором первым элементом записан список центроидов,
# потом список координат canopy а затем
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn1(k, items):
  import math

  def dist(p1, p2):
  	return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

  def intersected(a, b):
      return len(set(a) & set(b))

  T1, T2 = items[0]
  del items[0]

  cur_centroids = items[0]
  del items[0]

  canopy_center_coordinates = items[0]
  del items[0]

  canopy_for_centroid = {}
  for centroid in cur_centroids:
      res = []
      for i, canopy in enumerate(canopy_center_coordinates):
          if(dist(centroid, canopy) < T2):
              res = [i]
              break
          if(dist(centroid, canopy) < T1):
              res.append(i)
      canopy_for_centroid[centroid] = res

  for i in items:
    point = i[0]
    centrs = i[1]
    min_dist = 100
    min_c = -1
    for c in cur_centroids:
      if not intersected(centrs, canopy_for_centroid[c]):
          continue
      if dist(point, c) < min_dist:
        min_c = c
        min_dist = dist(point, c)
    yield str(min_c), "{} {}".format(point[0],point[1])

# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn1(k, vs):
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)

def reducefn2(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required = True)
#малый и большой радиус зонтов
parser.add_argument("-t1", help="Outter canopy radius", required=False, type=float)
parser.add_argument("-t2", help="Inner canopy radius", required=False, type=float)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]

SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]
if args.t1:
    T1 = args.t1
else:
    T1 = 5

if args.t2:
    T2 = args.t2
else:
    T2 = 2.5

input_canopy = {}
input_canopy['set1'] = [(T1, T2)] + SHARD1
input_canopy['set2'] = [(T1, T2)] + SHARD2
#построим canopy centers
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input_canopy)
s.mapfn = mapcnp1
s.reducefn = reducecnp1
results = s.run_server(password="")
if not results.values():
    raise Exception("Couldn't build canopy centers, check T1, T2 parameters")

canopy_centers = results.values()
#каждой точке соответсвуют номера зонтов в которых она может быть
input_canopy_1 = {}
input_canopy_1['set1'] = [(T1, T2)] + canopy_centers + SHARD1
input_canopy_1['set2'] = [(T1, T2)] + canopy_centers + SHARD2
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input_canopy_1)
s.mapfn = mapcnp2
s.reducefn = reducecnp2
results = s.run_server(password="")
if not results.values():
    raise Exception("Couldn't build canopy centers, check T1, T2 parameters")
#на этом этапе у нас для каждой точке известны номера зонтов в которых она лежит. Теперь
#точки можно каким-то образом разделить по новым файлам(в новых шардах). Для простоты здесь
#возьмем один шард

SHARD0 = []
for point, canopys in results.items():
    p = list(float(x) for x in point.split(';'))
    SHARD0.append([p, canopys])

for i in xrange(1,args.n):
  s = mincemeat.Server() 

  input2 = {}
  input2['set'] = [(T1, T2)] + [centroids] + canopy_centers + SHARD0
  s.map_input = mincemeat.DictMapInput(input2)
  s.mapfn = mapfn1
  s.reducefn = reducefn1

  results = s.run_server(password="") 
  centroids = [c for c in results.itervalues()]
# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server() 
input3 = {}
input3['set'] = [(T1, T2)] + [centroids] + canopy_centers + SHARD0
s.map_input = mincemeat.DictMapInput(input3)
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="")


for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )

