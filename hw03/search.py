import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util
import heapq
from math import log

metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument("--query", required = True)
args = parser.parse_args()

USERNAME = "pershinmr"
CORPUS_SIZE = 62.0 

vectors = {}
scores = {}

for word in args.query.split(" "):

    word = word.lower()
    letter = util.encode_term(word)[0:1]
    shard = "".join([sh for sh in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME,letter))])
    plists = json.JSONDecoder().decode(shard)

    if not util.encode_term(word) in plists: continue

    for line in plists[util.encode_term(word)]:
        if line == "": continue
        filename, docs, n = line.split("_")
        if not filename in vectors: vectors[filename] = {}
        vectors[filename][word] = (float(n) / float(docs)) * (1 + log(CORPUS_SIZE / len(plists[util.encode_term(word)])))
##        if not filename in scores: scores[filename] = 0
##        scores[filename] += (float(n) / float(docs)) * (1 + log(CORPUS_SIZE / (len(plists[util.encode_term(word)]))))


for key in vectors:
    scores[key] = 0
    for x in vectors[key].values():
        scores[key] += (1 - x)**2

if len(scores) > 0:
##    k_top = heapq.nlargest(10, scores, key = scores.get)
    k_top = heapq.nsmallest(10, scores, key = scores.get)
    for f in k_top:
        print f # + " " + str(scores[f])
else:
    print "Nothing was found"
