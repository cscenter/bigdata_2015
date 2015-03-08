__author__ = 'phil'
# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util
import math as m

metadata = dfs.CachedMetadata()

#считываем запрос
query = str(raw_input("enter query\n")).split()
# Ваш псевдоним в виде строковой константы
USERNAME='PhilippDolgolev'

#в алгоритме используется косинус угла между векторами документа и запроса, вектор документа состоит из tf, вектор запроса из idf
#можно сделать это всё через map-reduce, в мапперы подать слова начинающиеся с одной и той же буквы, и тогда они один раз пройдутся по posting_lists, в reduce просчитать
#скалярное произведение с запросом для каждого задействованного документа (просто суммировать произведения tf и idf, ключом естественно является имя документа),
# и нормировать длинной векторов документа и запроса, в результате пары (имя документа, косинус)
# из результата взять необходимое колчичество наиболее приближённых к 1.
D = len([l for l in dfs.get_file_content("/wikipedia/__toc__")]) # количество документов в корпусе
q_vector_sqr_len = 0.0

scores = {}
for word in query:
    try:
        shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(word)[0:1]))])
        plists = json.JSONDecoder().decode(shard)
        d = len(plists[util.encode_term(word)])
        idf = m.log(float(D) / float(d))
        q_vector_sqr_len += idf ** 2.0

        for doc in plists[util.encode_term(word)]:
            doc_id, tf = doc.split("///", 1)
            #здесь в scores по сути записываем скалярное произведение
            if doc_id in scores:
                scores[doc_id] += float(tf) * idf
            else:
                scores[doc_id] = float(tf) * idf

    except KeyError:
        # print "corpus not contain this word %s" % word
        pass

for doc_id in scores:
#на самом деле квадраты длин документов можно закэшировать, и будет работать быстро, но здесь наивно считывает из файла по мере надобности,
#в каждый файл конечно не более одного раза обращаемся
    for l in dfs.get_file_content("/%s/vectors/%s" % (USERNAME, doc_id)):
        doc_vector_sqr_len = float(l)
    scores[doc_id] /= m.sqrt(doc_vector_sqr_len * q_vector_sqr_len) # значения в идеале будут от 0 до 1, чем ближе к 1, тем более похоже (arccos(1) = 0), считать дополнительно arccos не вижу смысла

for doc_id in sorted(scores, None, None, True)[:10]:
    print(doc_id)