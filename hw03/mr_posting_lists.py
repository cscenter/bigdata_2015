# encoding: utf-8
import mincemeat
import os

import sys
sys.path.append("../dfs/")

import client as dfs

# Это последовательность из двух Map-Reduce
# Диспетчер запускается командой python mr_posting_lists.py
# Рабочий процесс запускается командой python mincemeat.py localhost 
# для каждого из Map-Reduce. То есть, когда отработает первый рабочий процесс, 
# нужно запустить эту же команду еще раз
# 
# Этот конвейер пока что работает только на одной машине 
# (потому что результаты первого MR записываются в локальные файлы)

# Первый Map-Reduce отображает терм в документ
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
            # заодно подсчитываем количество слов в документе
            doc_len += 1
            # считаем сырую частоту слов
            words[word] = words[word] + 1 if words.get(word) else 1
    for word in words:
        # нормализуем частоту 
        tf = float(words[word]) / float(doc_len)
        yield util.encode_term(word), {"filename": filename, "freq": tf}

# и записывает список документов для каждого терма во временный файл
def reducefn(k, vs):
    import util
    if len(k) > 100:
        print "Skipping posting list for term %s" % (util.decode_term(k))
        return {}

    # Нужно узнать сколько всего документов в корпусе для расчета idf
    import sys
    sys.path.append("../dfs/")
    import client as dfs
    corpus_size = sum(1 for i in dfs.get_file_content("/wikipedia/__toc__"))

    with open("tmp/plist/%s" % k, "w") as plist:
        # записываем во временные файлы название файла, в котором находится терм и его it-idf
        def tfidf(tf):
            import math
            idf = math.log(float(corpus_size) / float(len(vs)))
            return tf * idf

        plist.write("\n".join(["%s %f" % (record["filename"], tfidf(record["freq"])) for record in vs]))
    return {}

s = mincemeat.Server() 

# читаем оглавление корпуса википедии
wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputSequence(wikipedia_files) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 

# Второй Map-Reduce читает временные файлы и отображает первую букву файла в терм
def mapfn1(k, v):
    yield k[0:1], v

# свертка собирает все списки вхождений для термов, начинающихся на одну и ту же букву, 
# составляет из них словарь, сериализует его и записывает в файл на DFS
def reducefn1(k, vs):
    term_plist = {}
    for term in vs:
        with open("tmp/plist/%s" % term) as f:
            # Для каждого терма отсортируем документы по убыванию tf-idf
            records = f.read().split("\n")
            plist = [{"filename":record.split(" ")[0], "tfidf": float(record.split(" ")[1])} for record in records]
            plist.sort(key=lambda v: v["tfidf"], reverse=True)
            # Возращаем в представление "на одной строке через пробел документ и tf-idf"
            term_plist[term] = ["%s %f" % (record["filename"], record["tfidf"]) for record in plist]

    import sys
    sys.path.append("../dfs/")

    import client as dfs
    import json

    # Ваш псевдоним в виде строковой константы
    USERNAME = "arkichek"
    with dfs.file_appender("/%s/posting_list/%s" % (USERNAME, k)) as buf:
        buf.write(json.JSONEncoder().encode(term_plist))

s = mincemeat.Server() 
plist_files = os.listdir("tmp/plist/")
s.map_input = mincemeat.MapInputSequence(plist_files) 
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="") 

