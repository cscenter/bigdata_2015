from collections import defaultdict
import json
import math
import operator
import util
import argparse
import sys

sys.path.append("../dfs/")
import client as dfs

__author__ = 'root'

def iterlen(x):
  n = 0
  try:
    while True:
      next(x)
      n += 1
  except StopIteration: pass
  return n

PAGES_AMOUNT = iterlen(dfs.get_file_content("/wikipedia/__toc__"))
USERNAME = "plflok"

parser = argparse.ArgumentParser()
parser.add_argument("--query", required = True)
args = parser.parse_args()
query = {term for term in args.query.split()}
if not query:
    print("Query can't be empty")
    exit()

metadata = dfs.CachedMetadata()
results = defaultdict(float)

for term in query:
    enc_term = util.encode_term(term)
    try:
        shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, enc_term[0:1]))])
        plists = json.JSONDecoder().decode(shard)
        docs = plists[enc_term]
    except:
        continue
    # todo: count tf
    idf = math.log(PAGES_AMOUNT / len(docs))
    for d in docs:
        text = metadata.get_file_content(d)
        tf = 0
        for w in [term for line in text for term in line]:
            if w == term:
                tf += 1
        results[d] += tf * idf

if results:
    for page in sorted(results, key=operator.itemgetter(1), reverse=True)[:10]:
        print(page)
else:
    print("no results found for query \"%s\"" % query)