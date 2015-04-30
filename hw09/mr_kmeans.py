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
                yield "%f %f" % canopy, p # выплевываем координаты зонтика и точку принадлежащюю ему

def reducefn2(k, items):
    # вот здесь мы записываем для каждого зонтика точки которые он содержит
    # один зонтик - один шард
    # конкретно здесь мы возвращаем словарь содержащий все зонтики
    # однако в реальном приложении редьюс должен был записать зонтик как отдельный шард
    return items
    
# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn3(k, items):
  import math  

  def dist(p1, p2):
  	return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2) 

  T1, T2 = items[0]
  T1 = int(T1)
  T2 = int(T2) 
        
  cur_centroids = items[1]
  
  canopy = items[2].split(' ')
  canopy = (float(canopy[0]), float(canopy[1]))
  points_in_canopy = items[3]
  
  for c in cur_centroids:
      if dist(c, canopy) < T1:                                       
          for p in points_in_canopy:    
              yield "%f %f" % p, c         

# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn3(k, vs):
    import math  

    def dist(p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2) 

    point = k.split(' ')    
    point = (float(point[0]), float(point[1]))
    min_dist = -1
    min_c = -1
    for c in vs:
        if min_dist == -1 or dist(point, c) < min_dist:
            min_dist = dist(point, c)
            min_c = c
    return (point, min_c)

def mapfn4(k, items):
    yield '%f %f' % items[1], items[0]

def reducefn4(k, vs):
    new_cx = 0
    new_cy = 0
    for v in vs:
        new_cx += v[0]
        new_cy += v[1]
    new_cx /= len(vs)
    new_cy /= len(vs);    
    return (new_cx, new_cy)

def reducefn5(k, vs):
    return vs

parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required = True)
parser.add_argument("-t1", help="T1 - Outter radius", required = True, type = int)
parser.add_argument("-t2", help="T1 - Inner radius", required = True, type = int)

args = parser.parse_args()

T1 = args.t1
T2 = args.t2
# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]

SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]

input0 = {}
input0['set1'] = [[T1, T2]] + [SHARD1]
input0['set2'] = [[T1, T2]] + [SHARD2]
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn1
canopy_coordinates = s.run_server(password="")

input0 = {}
input0['set1'] = [[T1, T2]] + [canopy_coordinates[str(T1) + ',' + str(T2)]] + [SHARD1] 
input0['set2'] = [[T1, T2]] + [canopy_coordinates[str(T1) + ',' + str(T2)]] + [SHARD2]
s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn2
s.reducefn = reducefn2
canopies_with_points = s.run_server(password="") # это как бы было записано в шард

for i in xrange(1,args.n):
  s = mincemeat.Server() 
# вообще читаем зонтики из шардов
# но конкретно сейчас они у нас в списке canopies_with_point
  input0 = {}
  iter_num = 0
  import math  

  def dist(p1, p2):
      return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2) 
      
  for k in canopies_with_points.keys():
      current_canopy_contains_at_least_one_centroid = False
      for c in centroids:
          canopy_coordinates = k.split(' ')
          canopy_coordinates = (float(canopy_coordinates[0]), float(canopy_coordinates[1]))
          if dist(c, canopy_coordinates) < T1:
              current_canopy_contains_at_least_one_centroid = True
              break
      if current_canopy_contains_at_least_one_centroid == False:
          # данный зонтик не содержит ни одну из центроид при заданном внешнем радиусе T1
          # значит группа точек принадлежащая этому зонтику имеет расстояния до всех центроид равное бесконечности
          # следовательно их можно либо приписать любой центроиде
          # либо объявить отщепенцами и проигнорировать
          # выбор пути зависит от конкретно решаемой задачи
          # я пойду по второму пути
          continue
      v = canopies_with_points[k]
      input0['set%d' % (iter_num + 1)] = [[T1, T2]] + [centroids] + [k, v]
      iter_num += 1
  s.map_input = mincemeat.DictMapInput(input0) 
  s.mapfn = mapfn3
  s.reducefn = reducefn3
  results = s.run_server(password="") 
  points_and_their_centroids = [c for c in results.itervalues()]
  
  # дальше пересчитаем центроиды с помощью еще одного мап редьюса
  s = mincemeat.Server()
  input0 = {}
  iter_num = 0
  for p, c in points_and_their_centroids:
      input0['set%d' % (iter_num + 1)] = [p, c]
      iter_num += 1           
  s.map_input = mincemeat.DictMapInput(input0)
  s.mapfn = mapfn4
  s.reducefn = reducefn4

  results = s.run_server(password="")          
  centroids = [c for c in results.itervalues()]
  print centroids
    
s = mincemeat.Server() 
input0 = {}
iter_num = 0
for k in canopies_with_points.keys():
    current_canopy_contains_at_least_one_centroid = False
    for c in centroids:
        canopy_coordinates = k.split(' ')
        canopy_coordinates = (float(canopy_coordinates[0]), float(canopy_coordinates[1]))
        if dist(c, canopy_coordinates) < T1:
            current_canopy_contains_at_least_one_centroid = True
            break
    if current_canopy_contains_at_least_one_centroid == False:
          # данный зонтик не содержит ни одну из центроид при заданном внешнем радиусе T1
          # значит группа точек принадлежащая этому зонтику имеет расстояния до всех центроид равное бесконечности
          # следовательно их можно либо приписать любой центроиде
          # либо объявить отщепенцами и проигнорировать
          # выбор пути зависит от конкретно решаемой задачи
          # я пойду по второму пути
        continue
    v = canopies_with_points[k]
    input0['set%d' % (iter_num + 1)] = [[T1, T2]] + [centroids] + [k, v]
    iter_num += 1
s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn3
s.reducefn = reducefn3
results = s.run_server(password="") 
points_and_their_centroids = [c for c in results.itervalues()]
  
  # 
s = mincemeat.Server()
input0 = {}
iter_num = 0
for p, c in points_and_their_centroids:
    input0['set%d' % (iter_num + 1)] = [p, c]
    iter_num += 1           
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn4
s.reducefn = reducefn5
results = s.run_server(password="") 
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )