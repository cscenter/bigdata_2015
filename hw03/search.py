# encoding: utf-8
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

# подсчитываем общее число документов.
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
# валидация запроса. Убираем повторяющиеся слова, чтобы повторы не меняли вес документа
original_query = args.query
query = {term for term in args.query.split()}
if not query:
    print("Query can't be empty")
    exit()

metadata = dfs.CachedMetadata()
results = defaultdict(float)

# для каждого слова находим содержащие его документы и считаем их tf * idf
for term in query:
    enc_term = util.encode_term(term)
    try:
        shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, enc_term[0:1]))])
        plists = json.JSONDecoder().decode(shard)
        docs =  filter(bool, plists[enc_term])
    except:
        continue
    idf = math.log(float(PAGES_AMOUNT) / len(docs))
    for d in docs:
        doc, tf = d.split()
        tf = float(tf)
        results[doc] += tf * idf

print("results:")
if results:
    for page in sorted(results.items(), key=operator.itemgetter(1), reverse=True)[:10]:
        print(page[0])
else:
    print("no results found for query \"%s\"" % original_query)

