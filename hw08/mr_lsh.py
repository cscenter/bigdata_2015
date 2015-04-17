# encoding: utf-8
import mincemeat

#
# From 'Mining Massive Datasets' threeshold T
#   T ~ (1 / b) ^ (1 / r) = (1 / b) ^ (b / n)
# where n - characteristic vector length, b - bands amount, r - band width
# thereby:
#   b = (T ^ -1) ^ r
#   n = r * (T ^ -1) ^ r
# than lets assume that
#   r ~ T * ln n
#

def mapfn1(docid, docvector):
  from math import log, ceil

  # Here we could define threshold
  T = 0.7
  n = len(docvector)
  r = int(ceil(T * log(n)))
  b = int(ceil(1.0 * n / r))
  
  for band in range(b):
    yield band, (docid, docvector[r * band : r * (band + 1)])

def reducefn1(k, vs):
  return vs


def mapfn2(band, data):
  from collections import defaultdict

  def myHash(v):
    res = 0
    for i in v:
      res = res * 43 + i
    res = res % 115249
    return res

  buckets = defaultdict(list)
  for doc, vector in data:
    h = myHash(vector)
    buckets[h].append(doc)

  for bucket in buckets.values():
    for i in range(len(bucket)):
      for j in range(i + 1, len(bucket)):
        yield min(bucket[i],bucket[j]),max(bucket[i],bucket[j])

def reducefn2(k, vs):
  return [(k, v) for v in set(vs)]


s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]

input0['doc5'] = [22, 5, 34, 12, 31, 41, 11, 89, 18, 142, 89,  12,  78, 77, 45, 1, 39, 53, 83, 20]
input0['doc6'] = [21, 5, 34, 12, 31, 41, 11, 89, 18, 142, 89,  12,  78, 77, 45, 1, 39, 53, 83, 21]
input0['doc7'] = [48, 23, 69, 33, 22, 23, 88, 33, 71, 3, 68, 63, 20, 30, 96, 3, 50, 33, 30, 33]
input0['doc8'] = [48, 23, 69, 36, 22, 24, 88, 33, 71, 8, 68, 60, 20, 33, 96, 3, 50, 77, 30, 33]


s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="") 


s.map_input = mincemeat.DictMapInput(results) 
s.mapfn = mapfn2
s.reducefn = reducefn2

results = s.run_server(password="") 


for key, value in results.items():
  for x, y in value:
    print("({0}, {1})".format(x, y))

