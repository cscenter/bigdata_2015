__author__ = 'phil'
# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util
import math as m
import mincemeat

metadata = dfs.CachedMetadata()

# Ваш псевдоним в виде строковой константы
USERNAME='PhilippDolgolev'
#кэишруем длины векторов документов
doc_vectors = {}
for l in dfs.get_file_content("/%s/d_vectors_len" % USERNAME):
    doc_id, sqr_len = l.split('///', 1)
    doc_vectors[doc_id] = float(sqr_len)
#создаём файл в котором будем аккумулировать длину вектора запроса в мапперах
with open("vector_q_len", "w") as file:
    pass
#считываем запрос
query = str(raw_input("enter query\n")).split()


#в алгоритме используется косинус угла между векторами документа и запроса, вектор документа состоит из tf, вектор запроса из idf
#можно сделать это всё через map-reduce, в мапперы подать слова начинающиеся с одной и той же буквы, и тогда они один раз пройдутся по posting_lists, в reduce просчитать
#скалярное произведение с запросом для каждого задействованного документа (просто суммировать произведения tf и idf, ключом естественно является имя документа),
# и нормировать длинной векторов документа и запроса, в результате пары (имя документа, косинус)
# из результата взять необходимое колчичество наиболее приближённых к 1.

#разбиваем запрос на группы слов начинающиеся с одной и той же буквы
q = {}
for word in query:
    if word[0] in q:
        q[word[0]] = q[word[0]] + [word]
    else:
        q[word[0]] = [word]


def mapfn(k, v):
    import sys
    sys.path.append("../dfs/")

    import json
    import client as dfs
    import util
    import math as m
    USERNAME='PhilippDolgolev'

    metadata = dfs.CachedMetadata()

    v = json.JSONDecoder().decode(v)

    shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(v[0])[0:1]))])
    plists = json.JSONDecoder().decode(shard)
    D = len([l for l in dfs.get_file_content("/wikipedia/__toc__")]) # количество документов в корпусе

    q_vector_sqr_len = 0
    scores = {}
    for word in v:
        try:
            d = len(plists[util.encode_term(word)]) #количество документов содержащих это слово
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
            #print "word %s not contains in corpus" word
            pass
    #запишем в файл не в дфс, для удобства многократного запуска(если бы можно было удалять из дфс, то писали бы туда)
    #по хорошему надо решить ещё проблему одновременного доступа к файлу, пока оставляю так
    with open("vector_q_len", "a") as file:
        file.write(str(q_vector_sqr_len)+'\n')

    #возвращаю пары имя дока и уже набранные очки
    for doc_id, score in scores.items():
        yield (doc_id, score)

def reducefn(k, v):
    acc = 0
    for s in v:
        acc += float(s)
    return (k, acc)

s = mincemeat.Server()


s.map_input = mincemeat.MapInputSequence([json.JSONEncoder().encode(q[i]) for i in q])
s.mapfn = mapfn
s.reducefn = reducefn

print "you can start MP"
results = s.run_server(password="")

q_vector_sqr_len = 0
with open("vector_q_len", "r") as file:
    for l in file:
        q_vector_sqr_len += float(l)

scores = dict(results.values())

# закомментированный код - тоже самое, без map-reduce, и без группировки слов по первой букве
# D = len([l for l in dfs.get_file_content("/wikipedia/__toc__")]) # количество документов в корпусе
# q_vector_sqr_len = 0.0
#
# scores = {}
# for word in query:
#     try:
#         shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(word)[0:1]))])
#         plists = json.JSONDecoder().decode(shard)
#         d = len(plists[util.encode_term(word)])
#         idf = m.log(float(D) / float(d))
#         q_vector_sqr_len += idf ** 2.0
#
#         for doc in plists[util.encode_term(word)]:
#             doc_id, tf = doc.split("///", 1)
#             #здесь в scores по сути записываем скалярное произведение
#             if doc_id in scores:
#                 scores[doc_id] += float(tf) * idf
#             else:
#                 scores[doc_id] = float(tf) * idf
#
#     except KeyError:
#         # print "corpus not contain this word %s" % word
#         pass

#нормируем евклидовами длинами
for doc_id in scores:
    doc_vector_sqr_len = doc_vectors[doc_id]
    scores[doc_id] /= m.sqrt(doc_vector_sqr_len * q_vector_sqr_len) # значения в идеале будут от 0 до 1, чем ближе к 1, тем более похоже (arccos(1) = 0), считать дополнительно arccos не вижу смысла

for doc_id in sorted(scores, None, None, True)[:10]:
    print(doc_id)