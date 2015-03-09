# encoding: utf-8
import mincemeat
import os

import sys

sys.path.append("../dfs/")

import client as dfs

# Первый MapReduce
#
# Маппер читает отдельный документ и подсчитывает в нем статистику для документа, общее количество слов
# и количество вхождений слов. Выдает пару (key,value), где key - идентификатор слова, value - строка в которой
# закодированы данные об этом слове: из какого оно документа, сколько встречается в документе, общее количество
# слов в документе, и общее количество документов.
#
# Редьюсер подсчитывает количество документов в которых встречается слово и TFIDF далее записывает данные
# в промежуточное хранилище, данные хранятся списком пар(идентификатор документа, TFIDF) сгруппированными по
# идентификаторам слов в отдельных файлах.
#
# Второй MapReduce
#
# Маппер читает результаты прошлого MapReduce и отображает множество сгруппированное по словам в множество
# сгруппированное по документам.
#
# Редьюсер сортирует в лексикографическом порядке по идентификатору слова и записывает в DFS для каждого отдельного
# документа этот сортированный список пар (идентификатор слова, TFIDF).

def mapfn1(k, docs_total):
    import util

    doc_id, _ = k.split(" ", 1)

    import sys

    sys.path.append("../dfs/")

    import client as dfs

    words = {}
    words_in_doc_sum = 0
    for line in dfs.get_file_content(doc_id):
        for word in line.encode("utf-8").split():
            word_id = util.encode_term(word)
            if len(word_id) > 100:
                print "Skipping calculation for term %s" % word
                continue
            words_in_doc_sum += 1
            if word_id in words:
                words[word_id] += 1
            else:
                words[word_id] = 1

    for word_id, count_in_doc in words.iteritems():
        # (str, str int int int) (word_id, doc_id |count_in_doc |words_in_doc_sum |docs_total)
        yield word_id, util.arg_to_str(util.DEF_DELIM, doc_id, count_in_doc, words_in_doc_sum, docs_total)


#
def reducefn1(word_id, word_data_list):
    import util
    import math

    docs_with_word = len(word_data_list)
    result = []
    for word_data in word_data_list:
        doc_id, count_in_doc, words_in_doc_sum, docs_total = util.str_to_arg(util.DEF_DELIM, word_data,
                                                                             "str int int int")
        TF = float(count_in_doc) / words_in_doc_sum
        IDF = math.log(docs_total / float(docs_with_word))
        TFIDF = TF * IDF
        result.append((doc_id, TFIDF))

    with open("tmp/task_1/%s" % word_id, "w") as word_file:
        for doc_id, TFIDF in result:
            # str float (doc_id |TFIDF )
            data_str = util.arg_to_str(util.DEF_DELIM, doc_id, TFIDF)
            word_file.write(data_str + "\n")


def mapfn2(word_id, v):
    import util

    with open("tmp/task_1/%s" % word_id) as word_file:
        for line in word_file:
            doc_id, TFIDF = util.str_to_arg(util.DEF_DELIM, line, "str float")
            # (str, str float) (doc_id, |word_id |TFIDF)
            yield doc_id, util.arg_to_str(util.DEF_DELIM, word_id, TFIDF)


def reducefn2(doc_id, word_data_list):
    import util

    result = []
    for word_data in word_data_list:
        word_id, TFIDF = util.str_to_arg(util.DEF_DELIM, word_data, "str float")
        result.append((word_id, TFIDF))

    import sys

    sys.path.append("../dfs/")

    import client as dfs

    USERNAME = "izhleba"
    doc_filename = "/%s/doc_tfidf%s" % (USERNAME, doc_id)
    with dfs.file_appender(doc_filename) as buf:
        for word, TFIDF in sorted(result):
            buf.write(util.arg_to_str(util.DEF_DELIM, word, TFIDF))
    return doc_id


s = mincemeat.Server()
# читаем оглавление корпуса википедии
wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
documents_total_number = len(wikipedia_files)
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputSequenceWithData(wikipedia_files, documents_total_number)
s.mapfn = mapfn1
s.reducefn = reducefn1
results = s.run_server(password="")

s = mincemeat.Server()
task1_files = os.listdir("tmp/task_1/")
s.map_input = mincemeat.MapInputSequence(task1_files)
s.mapfn = mapfn2
s.reducefn = reducefn2
results = s.run_server(password="")

USERNAME = "izhleba"
docs_result_filename = "/%s/docs_tfidf" % USERNAME
with dfs.file_appender(docs_result_filename) as buf:
    for doc_filename in results:
        buf.write(doc_filename + "\n")
print("Root file for TFIDF by doc: %s" % docs_result_filename)


