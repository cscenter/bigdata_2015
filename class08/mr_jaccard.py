# encoding: utf-8
import mincemeat


def mapfn1(k, v):
    set_id, shard_id = k
    items = v
    for i in items:
        yield i, set_id


def reducefn1(k, vs):
    return vs


s = mincemeat.Server()

input0 = {}
input0[('set1', 'shard1')] = ['a', 'c', 'e']
input0[('set1', 'shard2')] = ['b', 'f', 'h']
input0[('set2', 'shard1')] = ['a', 'f', 'g']
input0[('set2', 'shard2')] = ['b', 'd', 'e']
input0[('set2', 'shard3')] = ['c', 'i', 'j']
input0[('set3', 'shard1')] = ['i', 'k', 'm']

# и подаем этот список на вход мапперам
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn1
s.reducefn = reducefn1

results1 = s.run_server(password="")
for key, value in sorted(results1.items()):
    print("%s: %s" % (key, value) )


def mapfn2(item, plist):
    for d1 in plist:
        for d2 in plist:
            yield d1, d2


def reducefn2(d1, docs):
    from collections import defaultdict

    counts = defaultdict(int)
    for d2 in docs:
        counts[d2] += 1
    return counts


s.map_input = mincemeat.DictMapInput(results1)
s.mapfn = mapfn2
s.reducefn = reducefn2

results2 = s.run_server(password="")
for key, value in sorted(results2.items()):
    print("%s: %s" % (key, value) )


def mapfn3(k, v):
    set_id, shard_id = k
    items = v
    yield set_id, len(items)


def reducefn3(set_id, vs):
    return sum(vs)


s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn3
s.reducefn = reducefn3

sizes = s.run_server(password="")

for set1, intersections in results2.items():
    for set2, intersect_size in intersections.items():
        union_size = sizes[set1] + sizes[set2] - intersect_size
        print "%s, %s: INTERSECTION = %d, UNION = %d, JACCARD_SIMILARITY=%f  " % (
            set1, set2, intersect_size, union_size, float(intersect_size) / float(union_size))
