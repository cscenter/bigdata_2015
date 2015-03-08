# encoding: utf-8
import mincemeat
import os

import sys

sys.path.append("../dfs/")

import client as dfs

#
def mapfn1(k, v):
    import util

    doc_id, _ = v.split(" ", 1)

    import sys

    sys.path.append("../dfs/")

    import client as dfs

    words = {}
    for line in dfs.get_file_content(doc_id):
        for word in line.encode("utf-8").split():
            if word in words:
                words[word] += 1
            else:
                words[word] = 1

    for word, count_in_doc in words.iteritems():
        word_id = util.encode_term(word)
        if len(word_id) > 100:
            print "Skipping calculation for term %s" % word
            continue
        else:
            # (str, str int) (word_id, doc_id |count_in_doc)
            yield word_id, util.arg_to_str(util.DEF_DELIM, doc_id, count_in_doc)


#
def reducefn1(word_id, doc_name_with_count_list):
    import util

    words_in_doc_sum = 0
    for doc_name_with_count in doc_name_with_count_list:
        _, count_in_doc = util.str_to_arg(util.DEF_DELIM, doc_name_with_count, "str int")
        words_in_doc_sum += count_in_doc

    with open("tmp/task_1/%s" % word_id, "w") as word_file:
        for doc_name_with_count in doc_name_with_count_list:
            doc_id, count_in_doc = util.str_to_arg(util.DEF_DELIM, doc_name_with_count, "str int")
        # str int int (doc_id |count_in_doc |words_in_doc_sum )
        data_str = util.arg_to_str(doc_id, count_in_doc, words_in_doc_sum)
        word_file.write(data_str + "\n")
    return {}


s = mincemeat.Server()

# читаем оглавление корпуса википедии
wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
documents_total_number = len(wikipedia_files)
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputSequence(wikipedia_files)
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="")

# Второй
def mapfn2(word_id, docs_total):
    import util

    with open("tmp/task_1/%s" % word_id) as word_file:
        for line in word_file:
            doc_id, count_in_doc, words_in_doc_sum = util.str_to_arg(util.DEF_DELIM, line, "str int int")
            # (int, str int int int) (doc_id, |word |count_in_doc |words_in_doc_sum |docs_total)
            yield doc_id, util.arg_to_str(util.DEF_DELIM, word_id, count_in_doc, words_in_doc_sum, docs_total)


#
def reducefn2(doc_id, word_data_list):
    import util
    import math


    docs_with_word = len(word_data_list)
    result = []
    for word_data in word_data_list:
        word_id, count_in_doc, words_in_doc_sum, docs_total = util.str_to_arg(util.DEF_DELIM, word_data,
                                                                              "str int int int")
        TF = count_in_doc / words_in_doc_sum
        IDF = math.log(docs_total / docs_with_word)
        TFIDF = TF * IDF
        result.append((word_id, TFIDF))

    import sys

    sys.path.append("../dfs/")

    import client as dfs

    # Ваш псевдоним в виде строковой константы
    USERNAME = "izhleba"
    with dfs.file_appender("/%s/doc_tfidf/%s" % (USERNAME, doc_id)) as buf:
        for word, TFIDF in sorted(result):
            buf.write(util.arg_to_str(util.DEF_DELIM, word, TFIDF))

# doc_id_filename = util.encode_term(doc_id)
# with open("tmp/task_2/%s" % doc_id_filename, "w") as word_file:
# # str int int int int (word |count_in_doc |words_in_doc_sum |docs_total |docs_with_word)
#     word_file.write(new_word_data)


s = mincemeat.Server()
plist_files = os.listdir("tmp/task_1/")
s.map_input = mincemeat.MapInputSequenceWithData(plist_files, documents_total_number)
s.mapfn = mapfn2
s.reducefn = reducefn2

results = s.run_server(password="")