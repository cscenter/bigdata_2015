# encoding: utf-8

import sys
import json
sys.path.append("../dfs/")

import client as dfs

#query = "alert answers computers Gain insight"
query = str(raw_input("query:\n"))

metadata = dfs.CachedMetadata()

K = 10 # Top K

wikipedia_files = [l for l in metadata.get_file_content("/wikipedia/__toc__")]
D = len(wikipedia_files)

import util
score = {}
USERNAME = "andreybalakshiy"
for q in query.split(" "):
    base = util.encode_term(q.lower())
    shard = "".join([d for d in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, base[0:1]))])
    f_list = json.JSONDecoder().decode(shard)
    d_cnt = len(f_list[base]) # для idf
    for l in f_list[base]:
        if l == "":
            continue
        doc_name, tf = l.split(" ", 1)
        tf = float(tf)
        import math
        idf = math.log(float(D) / d_cnt) 
        tf_idf = tf * idf
     #   tf_word_query = 1.0 / (count + 1)
    #    idf_word_query = math.log(float(D + 1) / (d_cnt + 1))
     #   tf_idf_word_query = tf_word_query * idf_word_query

        rev = tf_idf
        if score.has_key(doc_name):
            score[doc_name] += rev
        else:
            score[doc_name] = rev

k = min(K, len(score)) # Выведем не больше 10 документов
used = {} # сортировать долго, K раз за O(len(score)) пробежимся
for s in score.keys():
    used[s] = False
for i in range(0, k):
    best = ""
    mx = 0.0
    for s in score.keys():
        if used[s] == False and score[s] > mx:
            mx = score[s];
            best = s
    used[best] = True
    print(best)            
         
#for l1 in dfs.get_file_content(best):
#	print l1
       