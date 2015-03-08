__author__ = 'phil'
# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util

metadata = dfs.CachedMetadata()

# parser = argparse.ArgumentParser()
# parser.add_argument("--term", required = True)
# args = parser.parse_args()

query = str(raw_input("enter query\n")).split()
# Ваш псевдоним в виде строковой константы
USERNAME='PhilippDolgolev'

D = len([l for l in dfs.get_file_content("/wikipedia/__toc__")])
q = len(query)

scores = {}

for word in query:
    from math import log
    try:
        shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(word)[0:1]))])
        plists = json.JSONDecoder().decode(shard)
        d = len(plists[util.encode_term(word)])
        idf = log(float(D) / float(d))
        for doc in plists[util.encode_term(word)]:
            doc_id, tf = doc.split("///", 1)
            if doc_id in scores:
                scores[doc_id] += float(tf) * idf
            else:
                scores[doc_id] = float(tf) * idf
    except KeyError:
        # print "corpus not contain this word %s" % word
        pass

for doc_id in scores:
    scores[doc_id] /= D * q

for doc_id in sorted(scores, None, None, True)[:10]:
    print(doc_id)