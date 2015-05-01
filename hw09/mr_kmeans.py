# encoding: utf-8
import mincemeat
import argparse

# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
#ко всем точкам добавился зонт
def mapfn1(k, items):
  import math

  def dist(p1, p2):
      (c1, p1) = p1
      (c2, p2) = p2
      if (c1 != c2):
          return 100 #max float
  	  return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

  cur_centroids = items[0]
  print cur_centroids
  del items[0]
  for i in items:
    print "for"
    print i
    min_dist = 100 #max float
    min_c = -1
    for c in cur_centroids:
      if dist(i, c) < min_dist:
        min_c = c
        min_dist = dist(i, c)
    if (min_c == -1):
        continue
    (can_c, mc_p) = min_c
    (can_i, i_p) = i
    yield "%f %f" % can_c + " " + "%f %f" % mc_p, "%f %f" % can_i + " " + "%f %f" % i_p

# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера

# по хорошему тут надо искать зонт, которому принадлежит новый центройд,
# но т.к. для простоты не пользуемся реальной дфс(так бы из файла
# вытащили бы T1 T2 и сами центры зонтов, благо их мало), а передавать
# аргументом это не удобно в map/reduce, то не ищу.
def reducefn1(k, vs):
    new_cx = float(sum((float(v.split()[2]) for v in vs))) / len(vs)
    new_cy = float(sum((float(v.split()[3]) for v in vs))) / len(vs)
    return ((float(k.split()[0]), float(k.split()[1])), (new_cx, new_cy))

def reducefn2(k, vs):
    return vs

#маппер получает список, первый элемент которого это параметры T1(квадрат внутреннего радиуса) и T2(квадрат внешнего радиуса)
#второй элемент - список уже выбранных canopies
#третий элемент - список нераспределённых точек
def map_canopy(k, items):
    print "map canopy"

    T1 = items[0][0]
    T2 = items[0][1]

    del items[0]

    canopies = items[0]

    del items[0]

    #не извлекаю корень, т.к. это только лишнии вычисления, и потеря точности, достаточно сравнивать квадраты расстояний
    def dist(p1, p2):
        return (p1[0] - p2[0])**2 + (p1[1] - p2[1])**2

    canopies.append(items[0])
    # del items[0]

    for i in items:
        isTooNear = False
        used = False
        for c in canopies:
            if (isTooNear):
                break
            if (dist(i, c)) < T2:
                used = True

                if (dist(i, c)) < T1:
                    isTooNear = True

                yield "%f %f" % c, "%f %f" % i #canopy и точка которая в нём

        if (not used):
            yield "not used", "%f %f" % i

#конвертим во флоаты, чтобы дальше было удобно пользоваться
def reduce_canopy(k, vs):
    print("reduce_canopy")
    print(k, vs)
    def convertStringPoint(p):
        (x, y) = p.split(" ")
        return (float(x), float(y))
    return k, [convertStringPoint(v) for v in vs]




parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count", required = True, type = int)
parser.add_argument("-c", help="Initial centroids separated by commas and semicolons, like 1,1;2,6;6,2", required = True)

args = parser.parse_args()

# Начальные центроиды и количество итераций принимаются параметрами
centroids = [(float(c.split(",")[0]), float(c.split(",")[1])) for c in args.c.split(";")]

SHARD1 = [(0,0),(0,3),(1,0),(1,1),(1,5),(1,6),(2,1),(2,2),(2,6)]
SHARD2 = [(4,4),(3,6),(5,2),(5,3),(6,1),(6,2)]

canopy_build = False
print "canopy started"

params = [4, 9] #T1 T2, и это квадраты расстояний
#canopiesDict следует понимать в реальной системе как набор файлов,
#в которых хранятся зонтики
canopiesDict = {}
#canopies собствено хранит "имена" файлов
canopies = []
notUsed = SHARD1 + SHARD2

def convertStringPoint(p):
    (x, y) = p.split(" ")
    return (float(x), float(y))

def convertStringListToFloatList(l):
    return [convertStringPoint(p) for p in l]

#итеративно строим зонтики, пока каждая точка не окажется под зонтом,
#в реальной системе следовало бы
#разбить notUsed на шарды, чтобы можно было дальше параллельно запускать
#для упрощения, храним notUsed в "одном большом шарде" в памяти
canopies = []
while (not canopy_build):
    print "please, run mincemeat"
    s = mincemeat.Server()

    input0 = {}

    input0['input'] = [params] + [canopies] + notUsed
    s.map_input = mincemeat.DictMapInput(input0)
    s.mapfn = map_canopy
    s.reducefn = reduce_canopy
    results = s.run_server(password="")


    input0 = {}
    notUsed = []
    canopies = []
    for i in results.values():
        (k, v) = i
        if (k == 'not used'):
            notUsed = v
        else:
            print(k, v)
            k = convertStringPoint(k)
            if (k in canopiesDict):
                canopiesDict[k] += v
            else:
                canopiesDict[k] = v
    for i in canopiesDict:
        canopies.append(i)
    if (len(notUsed) == 0):
        break

print "canopy end"

for i in canopiesDict.items():
    print(i)

#помните что canopiesDict это набор файлов?
#вот здесь явно это показывается
shards = canopiesDict


def map_prep(k, items):
    c = items[0]
    del items[0]
    for i in items:
        yield "%f %f" % c, "%f %f" % i

def reduce_prep(k, vs):
    def convertStringPoint(p):
        (x, y) = p.split(" ")
        return (float(x), float(y))
    c = convertStringPoint(k)
    return [(c, convertStringPoint(p)) for p in vs]

print "Prepearing to K-Means, please run mincemeat"

s = mincemeat.Server()

input0 = {}

for i in shards.items():
    (c, ps) = i
    print [c] + ps
    input0[c] = [c] + ps
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = map_prep
s.reducefn = reduce_prep
results = s.run_server(password="")

print "Prepearing stoped"
print "Points with canopy"
points = []
for i in results.values():
    points += i;

print points

centroidsP = []
#запишем центройдам их зонты
def dist(p1, p2):
    return (p1[0] - p2[0])**2 + (p1[1] - p2[1])**2

#центройдов мало, полагаем что и зонтов мало(влазят в память),
# иначе наш алгоритм всё равно
#плоховато отработает
print "centroids with canopy"
for canopy in canopies:
    for centroid in centroids:
        if (dist(canopy, centroid) < params[1]):
            centroidsP.append((canopy, centroid))
print centroidsP

#опять же, points в реальной системе бы находился на множестве файлов,
#чтобы была возможность параллельной работы,
#для простоты это 1 список
print "K-Means can start, please run mincemeat"
for i in xrange(1,args.n):
  s = mincemeat.Server()
  input0 = {}
  input0['shards'] = [centroidsP] + points

  s.map_input = mincemeat.DictMapInput(input0)
  s.mapfn = mapfn1
  s.reducefn = reducefn1

  results = s.run_server(password="") 
  centroidsP = [c for c in results.itervalues()]
  print centroidsP

# На последней итерации снова собираем кластер и печатаем его
s = mincemeat.Server()
input0 = {}
input0['shards'] = [centroidsP] + points
s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="") 
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )

