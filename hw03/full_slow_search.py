# encoding: utf-8
import sys

sys.path.append("../dfs/")

import client as dfs
import argparse
import json
import util
import math

# Простой векторный поиск
# Для слов из запроса определятся множестов документов где они встречается. Для каждого документа из множества,
# определяем косинусную меру сходства вектора слов его характеризующую с вектором слов запроса. Выбираем 10 близких.

metadata = dfs.CachedMetadata()

parser = argparse.ArgumentParser()
parser.add_argument("-q", required=True)
args = parser.parse_args()

query_sqtr = args.q.strip()
word_vec = {}
if query_sqtr:
    # Почистим запрос от повторяющихся слов
    for word in query_sqtr.split(" "):
        word_vec[word] = True

if len(word_vec.keys()) > 0:
    USERNAME = "izhleba"
    # Подготовка списка документов для поиска
    search_area = {}
    for word in word_vec.keys():
        word_id = util.encode_term(word)
        filename = "/%s/posting_list/%s" % (USERNAME, word_id[0:1])
        shard = "".join([l for l in metadata.get_file_content(filename)])
        # Группируем слова по документам где они встречаются
        for doc_id in json.JSONDecoder().decode(shard)[word_id]:
            if doc_id in search_area:
                search_area[doc_id].append(word_id)
            else:
                search_area[doc_id] = [word_id]

    # Считаем косинусное сходство для всего списка документов
    search_result = []
    for doc_id_location, word_ids in search_area.iteritems():
        word_map = dict((l, 0) for l in word_ids)
        max_word = max(word_ids)
        for word_data in dfs.get_file_content("/%s/doc_tfidf%s" % (USERNAME, str(doc_id_location))):
            word_id, TFIDF = util.str_to_arg(util.DEF_DELIM, word_data, "str float")
            if word_id in word_map:
                word_map[word_id] = TFIDF
            if word_id > max_word:
                break
        # Подсчет формулы косинусного сходства
        factor_sum = 0
        square_sum = 0
        for word_id, factor in word_map.iteritems():
            factor_sum += factor
            square_sum += (factor * factor)
        cos_factor = factor_sum / (math.sqrt(square_sum) + math.sqrt(len(word_ids)))
        search_result.append((cos_factor, doc_id_location))

    # Подготовим название статей для удобства визуализации
    wikipedia_files = {}
    for line in dfs.get_file_content("/wikipedia/__toc__"):
        pair = line.split(" ", 1)
        wikipedia_files[pair[0]] = pair[1]

    for factor, doc_id_location in sorted(search_result, reverse=True)[:10]:
        print(doc_id_location, factor, wikipedia_files[doc_id_location])

else:
    print "Query is empty"
