# encoding: utf-8
import mincemeat
import os
import sys
sys.path.append("../dfs/")
import client as dfs


# mapping term into (document, tf) pair
def mapfn(k, v):
    import util

    filename, pagetitle = v.split(" ", 1)
    print v

    import sys
    sys.path.append("../dfs/")
    import client as dfs

    f = {}
    squared_vector_len = 0
    for l in dfs.get_file_content(filename):
        for term in l.encode("utf-8").split():
            term = term.lower()
            f[term] = f[term] + 1 if term in f else 1

    max_f = float(max(f.values()))
    for term in f:
        tf = .5 * (1 + f[term] / max_f)  # scaling
        squared_vector_len += tf ** 2
        yield util.encode_term(term), (filename, tf)

    with dfs.file_appender("/%s/doc_vector_len" % 'khazhoyan') as buf:
        from math import sqrt
        buf.write('%s %f\n' % (filename, sqrt(squared_vector_len)))


# write posting list (with tf-idf) in a temp file on local disc
def reducefn(k, vs):
    import util
    import sys
    sys.path.append("../dfs/")

    if len(k) > 100:
        print "Skipping posting list for term %s" % (util.decode_term(k))
        return {}
    with open("tmp/plist/%s" % k, "w") as plist:
        plist.write('\n'.join('%s %f' % (pair[0], pair[1]) for pair in vs))
    return {}


s = mincemeat.Server()

wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
s.map_input = mincemeat.MapInputSequence(wikipedia_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")


def mapfn1(k, v):
    yield k[0:1], v


def reducefn1(k, vs):
    def string_to_tuple(line):
        splitted = line.split()
        return splitted[0], float(splitted[1])

    term_plist = {}
    for term in vs:
        if term == '.DS_Store':  # OS X issues
            continue
        with open("tmp/plist/%s" % term) as f:
            plist = [string_to_tuple(l) for l in f.read().split("\n")]
            plist.sort(key=lambda x: x[1], reverse=True)
            term_plist[term] = plist

    import sys
    sys.path.append("../dfs/")
    import client as dfs
    import json
    with dfs.file_appender("/%s/posting_list/%s" % ('khazhoyan', k)) as buf:
        buf.write(json.JSONEncoder().encode(term_plist))


s = mincemeat.Server()
plist_files = os.listdir("tmp/plist/")
s.map_input = mincemeat.MapInputSequence(plist_files)
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="")