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
    base = util.encode_term(q)
    f = "".join([d for d in dfs.get_file_content("/%s/posting_list/%s" % (USERNAME, base[0:1]))])
    f_list = json.JSONDecoder().decode(f)
    id = -1
    for l in f_list[base]:
        if l == "":
            continue
        if id == -1:
            id = int(l)
     #       print(id)
            continue
        doc_name, tf, count = l.split(" ", 2)
        tf = float(tf)
        count = float(count)
        import math
        rev = tf * math.log(float(D) / id)
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
print(n)           

for l1 in dfs.get_file_content(n):
	print l1
       