# encoding: utf-8
import mincemeat
import matplotlib.pyplot as plt
import argparse
import  sys

# Маппер получает список, в котором первым элементом записан список центроидов,
# а последущими элементами являются точки исходного набора данных
# Маппер выплевывает для каждой точки d пару (c, d) где c -- ближайший к точке центроид
def mapfn1(k, items):
  import math

  def dist(p1, p2):
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

  cur_centroids = items[0]
  del items[0]
  for i in items:
    min_dist = 1000000
    min_c = -1
    for c in cur_centroids:
      if dist(i, c) < min_dist:
        min_c = c
        min_dist = dist(i, c)
    yield "%f %f" % min_c, "%f %f" % i

# У свертки ключом является центроид а значением -- список точек, определённых в его кластер
# Свёртка выплевывает новый центроид для этого кластера
def reducefn1(k, vs):
    new_cx = float(sum([float(v.split()[0]) for v in vs])) / len(vs)
    new_cy = float(sum([float(v.split()[1]) for v in vs])) / len(vs)
    return (new_cx, new_cy)

def reducefn2(k, vs):
    return vs


parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Iterations count for pre-kmeans", required = True, type = int)
parser.add_argument("-f", help="File with data", required = True)
parser.add_argument("-c", help="The number of clasters", required = True)

args = parser.parse_args()

# finding possible centers
def mapfn3(k, items):
    l = 10
    leftBorder = -10000 #the border of the left corner
    stepSize = 20000 / 10 #the size of the grid step
    for it in items:
        isFind = False
        for xx in xrange(l):
            if isFind:
                break
            for yy in xrange(l):
                if isFind:
                    break
                xl = leftBorder + xx * stepSize
                yl = leftBorder + yy * stepSize
                xr = xl + stepSize
                yr = yl + stepSize
                if (it[0] > xl and it[0] <xr and it[1] > yl and it[1] < yr):
                    yield (xl + stepSize / 2, yl + stepSize / 2), it
                    isFind = True

def reducef3(k, v):
    cnt = len(v)
    return cnt

shard = {}

import random
sys.stdin = open(args.f, 'r')
#sys.stdout = open('result', 'w')

points = zip(map(int, raw_input().split()),map(int, raw_input().split()))
random.shuffle(points)
N = len(points)

for i in xrange(N / 1000 + 1):
    shard['set' + str(i)] = points[i * 1000: (i + 1) * 1000]

input1 = {}
for x in shard.keys():
    input1[x] = shard[x]

canopy = {}
canopySet = set(points)

def canopymap1(k, v):

    def sqr(x):
        return x * x

    T1 = 3000

    xxx = float(k.split()[0])
    yyy = float(k.split()[1])
    for (x, y) in v:
        if sqr(x - xxx) + sqr(y - yyy) < T1 * T1:
            yield k, (x, y)

def canopyreduce1(k, v):
    return v

while len(canopySet) > 20:
    inputq = {}

    tmpList = list(canopySet)
    random.shuffle(tmpList)
    print len(tmpList)
    inputq[str(tmpList[0][0]) + ' ' + str(tmpList[0][1])] = tmpList
    s = mincemeat.Server()
    s.map_input = mincemeat.DictMapInput(inputq)
    s.mapfn = canopymap1
    s.reducefn = canopyreduce1
    ret = s.run_server(password="")
    for key, value in ret.items():
        canopy[key] = value
        for (x, y) in value:
            canopySet.remove((x, y))

tmpList = list(canopySet)
for (x,y) in tmpList:
    canopy[str(x) + ' ' + str(y)] = [(x, y)]
print 'done'

s = mincemeat.Server()
s.map_input = mincemeat.DictMapInput(input1)
s.mapfn = mapfn3
s.reducefn = reducef3
ret = s.run_server(password="")

cnt = 3 * int(args.c)

centroids = []

ret = sorted(ret.items(), key = lambda x : x[1])
tmpq = range(min(cnt,len(ret)))
for i in tmpq:
    centroids.append(ret[i][0])

results = []

def getCentroids(x, l):
    T2 = 2500 * 2500
    xxx, yyy = float(x.split()[0]), float(x.split()[1])
    t = [y for y in l if (y[0] - xxx) * (y[0] - xxx) + (y[1] - yyy) * (y[1] - yyy) < T2]
    if t == []:
        t = l
    return t

                     
for i in xrange(1,int(args.n)):
  s = mincemeat.Server()

  input0 = {}
  for x in canopy.keys():
    input0[x] = [getCentroids(x,centroids)] + canopy[x]
  s.map_input = mincemeat.DictMapInput(input0)
  s.mapfn = mapfn1
  s.reducefn = reducefn1
  results = s.run_server(password="") 
  centroids = [c for c in results.itervalues()]

#После предварительного кмина отберем парней для небольшого cura.

s = mincemeat.Server()
input0 = {}
for x in canopy.keys():
  input0[x] = [getCentroids(x,centroids)] + canopy[x]
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="")


toCure = []
for key, value in (results.items()):
    tmp = (min(10, len(value)))
    for x in value[:tmp]:
        toCure.append((float(x.split()[0]), float(x.split()[1])))


centroids = toCure

import cure
centroids = cure.cure(toCure, (cnt / 3) * 2)
for i in xrange(1,int(args.n)*2):
  s = mincemeat.Server() 

  input0 = {}
  for x in canopy.keys():
    input0[x] = [getCentroids(x,centroids)] + canopy[x]
  s.map_input = mincemeat.DictMapInput(input0)
  s.mapfn = mapfn1
  s.reducefn = reducefn1
  results = s.run_server(password="") 
  centroids = [c for c in results.itervalues()]

s = mincemeat.Server()
input0 = {}
for x in canopy.keys():
  input0[x] = [getCentroids(x,centroids)] + canopy[x]
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="")


toCure = []
for key, value in (results.items()):
    tmp = (min(10, len(value)))
    for x in value[:tmp]:
        toCure.append((float(x.split()[0]), float(x.split()[1])))

centrId = cure.cure(toCure, int(args.c), True)

centroids = []
for value, key in centrId:
    for x in value:
        centroids.append(x)

# На последней итерации снова собираем кластер и печатаем его


s = mincemeat.Server() 
input0 = {}
for x in canopy.keys():
    input0[x] = [getCentroids(x, centroids)] + canopy[x]
s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn1
s.reducefn = reducefn2
results = s.run_server(password="")

from collections import defaultdict

q = defaultdict(list)
for key, value in results.items():
    x,y = map(float, key.split())
    for value2, key2 in centrId:
        for (u,w) in value2:
            if (u,w) == (x,y):
                for x in value:
                    q[str(key2[0])+' '+str(key2[1])].append(x)


for key, value in q.items():
    X = []
    Y = []
    for x in value:
       X.append(float(x.split(' ')[0]))
       Y.append(float(x.split(' ')[1]))
       plt.plot(X, Y, color=(random.random(),random.random(),random.random()), marker = 'o', linestyle='')

plt.show()