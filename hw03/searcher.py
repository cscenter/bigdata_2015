# encoding: utf-8
import sys
import itertools

sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util

metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument("--query", required=True)
args = parser.parse_args()

# Ваш псевдоним в виде строковой константы

USERNAME="zarechenskiy"


def get_page_lists(letter):
  shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(letter)[0:1]))])
  return json.JSONDecoder().decode(shard)


sim = {}
K = 10

# Обработчик запроса.
# Группируем термы в запросе по первой букве, чтобы меньше раз доставать список страниц, где есть данный терм
# Считаем скалярное произведение документа и запроса. Вектор документа состоит из tf_idf, запрос из 0, 1.
def process_query(words):
  for key, group in itertools.groupby(words, lambda x: x[0:1]):
    plists = get_page_lists(key)
    for word in group:
      term = util.encode_term(word)
      if not term in plists:
        continue
      pages = plists[term]
      for page in pages:
        doc, tf_idf = page.split()
        if doc in sim:
          sim[doc] += tf_idf
        else:
          sim[doc] = tf_idf

  sorted_sim = sorted(sim.items(), key=lambda x: x[1], reverse=True)
  i = 0
  for doc, tf_idf in sorted_sim:
    if i > K:
      break
    print(doc)
    i += 1

process_query(args.query.split())