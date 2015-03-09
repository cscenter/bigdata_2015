# encoding: utf-8
from __future__ import division

import mincemeat
import os
import util
import sys
sys.path.append("../dfs/")
import client as dfs

# версия д/з №3 с использованием dfs
# для корректной работы небходимо указать число документов в mapfn


# Это последовательность из двух Map-Reduce
# Диспетчер запускается командой python mr_posting_lists.py
# Рабочий процесс запускается командой python mincemeat.py localhost
# для каждого из Map-Reduce. То есть, когда отработает первый рабочий процесс,
# нужно запустить эту же команду еще раз
#
# Этот конвейер пока что работает только на одной машине
# (потому что результаты первого MR записываются в локальные файлы)

# Первый Map-Reduce отображает терм в документ,
# а также сохраняет число вхождений терма в документ и длину документа
def mapfn(k, v):
    import util
    filename, pagetitle = v.split(" ", 1)
    print v

    import sys
    sys.path.append("../dfs/")

    import client as dfs
    words = {}
    doc_len = 0
    for l in dfs.get_file_content(filename):
        for word in l.encode("utf-8").split():
            if word in words:
                words[word] += 1
            else:
                words[word] = 1
            doc_len += 1
    for word in words:
        yield util.encode_term(word), (filename, words[word], doc_len)

# и записывает список документов для каждого терма во временный файл
# плюс записывает для каждого ключа список коэффициентов tf-idf для каждого документа, где он встречается
# это записывается в файлы с названием _%ключ%
def reducefn(k, tup):
    NUM_OF_DOCS = 60
    import util
    from math import log
    if len(k) > 100:
        print "Skipping posting list for term %s" % (util.decode_term(k))
        return {}

    idf = log(NUM_OF_DOCS / len(tup))
    with open("tmp/plist/%s" % k, "w") as plist:
        with open("tmp/plist/_%s" % k, "w") as tf_idf_file:
            for doc in sorted(tup):
                document = doc[0]           # название документа
                count = doc[1]              # сколько раз слово в нем встретилось
                doc_len = doc[2]            # длина документа

                tf_idf = (count / doc_len) * idf
                plist.write("{0}\n".format(document))
                tf_idf_file.write("{0} {1}\n".format(document, tf_idf))

    return {}


# Второй Map-Reduce читает временные файлы и отображает первую букву файла в терм
def mapfn1(k, v):
    yield k[0:1], v


# свертка собирает все списки вхождений для термов, начинающихся на одну и ту же букву,
# составляет из них словарь, сериализует его и записывает в файл на DFS
# файлы с названием, начинающимся на _, записываются в отдельный файл tf_idf
def reducefn1(k, vs):
    term_plist = {}
    for term in vs:
        with open("tmp/plist/%s" % term) as f:
            if k != '_':
                term_plist[term] = f.read().split("\n")
            else:
                term_plist[term[1:]] = f.read().split("\n")

    import sys
    sys.path.append("../dfs/")

    import client as dfs
    import json

    # Ваш псевдоним в виде строковой константы
    USERNAME='lenavolzhina'
    if k != '_':
        with dfs.file_appender("/%s/posting_list/%s" % (USERNAME, k)) as buf:
            buf.write(json.JSONEncoder().encode(term_plist))
    else:
        with dfs.file_appender("/%s/tf_idf" % USERNAME) as buf:
            buf.write(json.JSONEncoder().encode(term_plist))


def prepare_stuff():
    # первый прогон:

    s = mincemeat.Server()
    # читаем оглавление корпуса википедии
    wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
    # и подаем этот список на вход мапперам
    s.map_input = mincemeat.MapInputSequence(wikipedia_files)
    s.mapfn = mapfn
    s.reducefn = reducefn

    s.run_server(password="")

    # второй прогон:
    s = mincemeat.Server()
    plist_files = os.listdir("tmp/plist/")
    s.map_input = mincemeat.MapInputSequence(plist_files)
    s.mapfn = mapfn1
    s.reducefn = reducefn1

    s.run_server(password="")


##################################################
# теперь учимся отвечать на запросы

import json

all_tf_idf = None
metadata = dfs.CachedMetadata()

# возвращает коэффициент tf_idf для пары терм-документ
def get_tf_idf(term, document):
    global all_tf_idf
    if all_tf_idf is None:
        all_tf_idf = {}   # один раз загружаем tf_idf (может занимать много памяти, но постоянно к нему стучаться долго)

        global metadata
        # Ваш псевдоним в виде строковой константы
        USERNAME='lenavolzhina'
        shard = "".join([l for l in metadata.get_file_content("/%s/tf_idf" % USERNAME)])

        shard = shard.replace("}{", ",")   # как-то странно записалось, поправляю

        content = json.JSONDecoder().decode(shard)

        for term_ in content:
            if term_ not in all_tf_idf:
                all_tf_idf[term_] = {}       # небольшой костыль, чтобы создать словарь словарей
            for line in content[term_]:
                if len(line):
                    doc, tf_idf = line.split()
                    all_tf_idf[term_][doc] = float(tf_idf)
    return all_tf_idf[util.encode_term(term)][document]


# возвращает список документов, где встречается слово
def get_documents(key):
    global metadata

    # Ваш псевдоним в виде строковой константы
    USERNAME='lenavolzhina'
    shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, util.encode_term(key)[0:1]))])
    shard = shard.replace("}{", ",")   # как-то странно записалось, поправляю

    plists = json.JSONDecoder().decode(shard)

    if util.encode_term(key) in plists:
        return plists[util.encode_term(key)]
    else:
        return []


# пересекает списки за O(len)
def merge_intersect(docs1, docs2):
    result = []
    i = j = 0
    while i < len(docs1) and j < len(docs2):
        if docs1[i] == docs2[j]:
            result.append(docs1[i])
            i += 1
            j += 1
        elif docs1[i] < docs2[j]:
            i += 1
        else:
            j += 1
    return result


# считает расстояние между векторами
def distance(v1, v2):
    if len(v1) != len(v2):
        raise Exception("Something is wrong :( Vector are not equal length")
    l = len(v1)
    res = 0
    for i in range(l):
        res += v1[i] * v2[i]
    res /= l * l
    return res


# отвечает на запрос :)
def answer_query(query):
    plists = {}
    sizes = []

    for word in query.split():
        plists[word] = get_documents(word)
        sizes.append((len(plists[word]), word))
    sizes = sorted(sizes, reverse=True)       # более эффективно пересекать, начиная с меньших по объему
    if len(sizes) == 0:
        print("Empty query or at least one of these words are not in the corpus")
        return

    current_docs = plists[sizes[0][1]]      # список документов самого редкого слова из запроса
    for row in sizes[1:]:
        new_docs = plists[row[1]]
        current_docs = merge_intersect(current_docs, new_docs)
    if len(current_docs) == 0:
        print("There isn't any documents containing all of these words in this corpus")
        return

    # осталось упорядочить их
    # составляем вектора в пространстве размерности (кол-во слов в запросе)
    vectors = {}
    query_vector = []
    for doc in current_docs:
        vectors[doc] = []
    for word in query.split():
        for doc in current_docs:
            vectors[doc].append(get_tf_idf(word, doc))
        query_vector.append(1)

    # считаем расстояния до вектора запроса
    dist = []
    for doc in current_docs:
        dist.append((distance(vectors[doc], query_vector), doc))

    # сортируем по убыванию расстояния
    dist = sorted(dist, reverse=True)

    # выводим результат
    for i in range(min(10, len(dist))):
        print("doc #{0}:  {1}   dist = {2}".format(i, dist[i][1], dist[i][0]))


# печатает оглавление
def print_toc():
    for l in dfs.get_file_content("/wikipedia/__toc__"):
        print(l)


# достаточно запустить один раз
prepare_stuff()


# print_toc()

import sys

if len(sys.argv) > 1:
    query = " ".join(sys.argv[1:])    # запуск из консоли
else:
    query = "IT analytics"

print("query = " + query)

answer_query(query)
