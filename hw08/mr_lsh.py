	# encoding: utf-8
import mincemeat

def mapfn(docid, docvector):
  for v in docvector:
    yield docid, v

def reducefn(k, vs):
  return vs

s = mincemeat.Server() 

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]

s.map_input = mincemeat.DictMapInput(input0) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value) )
