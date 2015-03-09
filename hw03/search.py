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