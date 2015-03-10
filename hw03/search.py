import json
import sys
import math

sys.path.append("../dfs/")
import argparse
import client as dfs
import logging
import util

metadata = dfs.CachedMetadata()

from collections import defaultdict

logging.basicConfig(level=logging.INFO)

page_scores = defaultdict(float)
# Number of documents in corpus
D = len([line for line in dfs.get_file_content("/wikipedia/__toc__")])

parser = argparse.ArgumentParser()
parser.add_argument('--query', required=True)
args = parser.parse_args()
terms = [util.encode_term(x.lower()) for x in args.query.split()]

plists = None
first_letter = terms[0][0]

USERNAME = "maximmaximov"

metadata = dfs.CachedMetadata()

# todo:use mapreduce

for term in terms:
    if plists is None:
        shard = "".join(
            [l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, term[0:1]))])
        plists = json.JSONDecoder().decode(shard)
        if term not in plists:
            continue
        # Number of documents that contains this term
        d = len(plists[term])
        logging.info("found %d documents" % d)
        idf = math.log(1.0 * D / d)

    for posting in plists[term]:
        page, tf = posting.split()
        page_scores[page] += float(tf) * idf

page_scores_inv = [(v, k) for k, v in page_scores.iteritems()]
page_scores_inv.sort(reverse=True)
for v, k in page_scores_inv[:10]:
    print k


#==========================+MAPREDUCE

import sys

sys.path.append("../dfs/")
import argparse
import client as dfs
import logging
import util
import mincemeat

metadata = dfs.CachedMetadata()

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('--query', required=True)
args = parser.parse_args()



# todo:use mapreduce


#
# def mapfn(k, v):
#     from collections import defaultdict
#
#     page_scores = defaultdict(float)
#     # Number of documents in corpus
#     import sys
#     import math
#
#     USERNAME = "maximmaximov"
#     sys.path.append("../dfs/")
#     import client as dfs
#     import logging
#     import json
#
#     D = len([line for line in dfs.get_file_content("/wikipedia/__toc__")])
#     terms = v
#     plists = None
#     metadata = dfs.CachedMetadata()
#
#     #logging.log(terms)
#     term = v
#     #for term in terms:
#     if plists is None:
#         shard = "".join(
#             [l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, term[0:1]))])
#
#         logging.info(term)
#         plists = json.JSONDecoder().decode(shard)
#         if term not in plists:
#             return
#         # Number of documents that contains this term
#         d = len(plists[term])
#         logging.info("found %d documents" % d)
#         idf = math.log(1.0 * D / d)
#
#         for posting in plists[term]:
#             page, tf = posting.split()
#             page_scores[page] += float(tf) * idf
#
#     for k, v in page_scores:
#         yield (k, v)
#
#
# def reducefn(k, v):
#     #total_score = 0
#     #for item in v:
#     #    total_score += float(item)
#     #return (k, total_score)


#terms = [util.encode_term(x.lower()) for x in args.query.split()]
#s = mincemeat.Server()
#s.map_input = mincemeat.MapInputSequence(terms)
#s.mapfn = mapfn
#s.reducefn = reducefn

results = s.run_server(password="")

#page_scores = dict(results.values())

#page_scores_inv = [(v, k) for k, v in page_scores.iteritems()]
#page_scores_inv.sort(reverse=True)
#for v, k in page_scores_inv[:10]:
#    print k