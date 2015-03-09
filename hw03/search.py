# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util
import operator


def pull_max_doc_tdidf(plists):
    """
    Функция вынимает из plists документ с максимальным tfidf
    Для каждого терма документы уже были отсортированы при составлении
    индекса, поэтому будем сравнивать только первые документы в списках
    каждого терма
    """
    max_doc_filename = None
    max_doc_tdidf = 0
    max_term = None
    for term in plists:
        if max_doc_filename == None:
            max_term = term
            max_doc_filename, max_doc_tdidf = plists[term][0].split(" ")
            continue
        filename, tdidf = plists[term][0].split(" ")
        if tdidf > max_doc_tdidf:
            max_term = term
            max_doc_filename = filename
            max_doc_tdidf = tdidf
    plists[max_term].pop(0)
    if len(plists[max_term]) == 0:
        del plists[max_term]
    return max_term, max_doc_filename, float(max_doc_tdidf)


metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument("words", type=str, nargs="+")
args = parser.parse_args()


query = {}
plists = {}

# считаем частоты слов в запросе
for word in args.words:
    raw_freq = query[word] + 1 if query.get(word) else 1
    query[word] = {"freq": raw_freq}

for word in query:
    # собираем шарды для всех слов в запросе
    USERNAME = "arkichek"
    shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(word)[0:1]))])
    shard_decoded = json.JSONDecoder().decode(shard)
    key = util.encode_term(word)
    if shard_decoded.get(key) == None:
        continue
    plists[word] = shard_decoded[key]
    # нормируем
    tf = query[word]["freq"] / float(len(plists[word]))
    query[word]["tf"] = tf

if len(plists) == 0:
    print("Nothing found")
else:
    # document-at-a-time
    top = {}
    while len(plists) > 0:
        term, filename, tdidf = pull_max_doc_tdidf(plists)
        # считаем итоговый рейтинг каждого документа среди отобранных
        score = query[term]["tf"] * tdidf
        top[filename] = top[filename] + score if top.get(filename) else score
        
    # сортируем собранные рейтинги и берём первую десятку
    sorted_top = sorted(top.items(), key=operator.itemgetter(1), reverse=True)
    sorted_top = sorted_top[:10]
    for record in sorted_top:
        print(record[0])

