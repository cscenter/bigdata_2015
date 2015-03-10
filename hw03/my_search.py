# encoding: utf-8
import math
import json
import util
import sys
sys.path.append("../dfs/")
import client as dfs
wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
count_of_docs = len(wikipedia_files)
USERNAME= "sukhochev"
query = str(raw_input("query:\n"))
query = query.lower()
docs = {}
vector_of_query = {}
for term in query.split(" "):
    if not vector_of_query.has_key(term):
        vector_of_query[term] = 1
for term in vector_of_query:
    encoded_term = util.encode_term(term)
    s = "".join([l for l in dfs.get_file_content("/%s/posting_list/%s"%(USERNAME, encoded_term[0]))])
    s_dict = json.JSONDecoder().decode(s)
    if s_dict.has_key(encoded_term):
        for doc in s_dict[encoded_term]:
            if doc != "":
                doc_name, tf, count_of_words, count_of_docs_with_same_term = doc.split(" ")
                if not docs.has_key(doc_name):
                    docs[doc_name] = 0
                idf = math.log(float(count_of_docs) / float(count_of_docs_with_same_term))
                docs[doc_name] += float(tf)*idf
if len(docs) == 0:
    print 'There is no matches'
else:
    doc_list = docs.values()
    doc_list.sort(reverse = True)
    iden = 0
    i = 0
    if len(docs) >= 10:
        while i < 10:
            for doc_name in docs:
                if docs[doc_name] == doc_list[iden]:
                    print doc_name
                    iden += 1
                    i += 1
    else:
        while i < len(docs):
            for doc_name in docs:
                if docs[doc_name] == doc_list[iden]:
                    print doc_name
                    iden += 1
                    i += 1
