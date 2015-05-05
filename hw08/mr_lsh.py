# encoding: utf-8
import mincemeat

def mapfn(docid, docvector):
  # размер r-вектора. С уменьшением r мы будем находить более непохожие документы. r=4 даёт возможность найти документы, совпадающие 
  # (в тестовом случае) на 25% (если повезет). 
  r = 4
  # число значений хеш-функции для каждой полосы
  # должно быть сравнимо с числом документов, а лучше больше, чтобы исключить коллизии из-за остатка деления по модулю
  buckets = 17

  from zlib import crc32
  def hashfn(stripe_num, buckets_count, rvector):
    crc = crc32(str(rvector))
    res = (crc % buckets_count) + stripe_num * buckets_count
    print "%d: %s" % (res, str(rvector))
    return res
    
  rvector = []
  stripe = 0
  counter = 0
  for v in docvector:
    if counter < r:
      rvector.append(v)
      counter += 1
    else:
      yield "%d" % hashfn(stripe, buckets, rvector), docid
      rvector = []
      stripe += 1
      counter = 0
  if counter > 0:
    yield "%d" % hashfn(stripe, buckets, rvector), docid

def reducefn(k, vs):
  print vs
  return vs

def mapfn_dedup(k, docs):
  for d1 in docs:
    for d2 in docs:
      if d1 != d2:
        if d1 < d2:
          yield "%s %s" % (d1, d2), 1
        else:
          yield "%s %s" % (d2, d1), 1

def reducefn_dedup(pair, vs):
  return sum(vs)

s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22,   24, 88, 37, 71, 8,   68, 60, 20, 33, 96,    9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22,   24, 45, 37, 71, 8,   68, 60, 63, 78, 12,    9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74,   100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31,    41, 14, 89, 18, 100, 89,  63,  66, 96, 78,  19, 39, 53, 83, 20]

s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 

s = mincemeat.Server() 
s.map_input = mincemeat.DictMapInput(results)
s.mapfn = mapfn_dedup
s.reducefn = reducefn_dedup

results = s.run_server(password="") 

for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )
