# encoding: utf-8

import sys
import json
sys.path.append("../dfs/")

import client as dfs

#query = "support vector machine"
query = "alert answers computers Gain insight"

wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
D = len(wikipedia_files)

import util
score = {}
USERNAME = "andreybalakshiy"
for q in query.split(" "):
    base = util.encode_term(q.lower())
    f = "".join([d for d in dfs.get_file_content("/%s/posting_list/%s" % (USERNAME, base[0:1]))])
    f_list = json.JSONDecoder().decode(f)
    d_cnt = len(f_list[base])
    for l in f_list[base]:
        if l == "":
            continue
        doc_name, tf, count = l.split(" ", 2)
        tf = float(tf)
        count = float(count)
        import math
        idf = math.log(float(D) / d_cnt) 
        tf_idf = tf * idf
        tf_word_query = 1.0 / (count + 1)
        idf_word_query = math.log(float(D + 1) / (d_cnt + 1))
        tf_idf_word_query = tf_word_query * idf_word_query

        # нормированное скалярное произведение
        rev = tf * idf
        if score.has_key(doc_name):
            score[doc_name] += rev
        else:
            score[doc_name] = rev
            
mx = 0.0
n = "a";
for s in score.keys():
 #   print(score[s])
    if score[s] > mx:
        mx = score[s];
        n = s            
         

for l1 in dfs.get_file_content(n):
	print l1
print(n)  
       