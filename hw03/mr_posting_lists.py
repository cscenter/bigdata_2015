# encoding: utf-8
import os
import sys
import logging

import mincemeat


sys.path.append("../dfs/")

import client as dfs


def mapfn(k, v):
    """
    emits reduce_key=term, value=page term_frequency
    :param k:
    :param v:
    :return:
    """
    import util

    filename, pagetitle = v.split(" ", 1)

    import sys

    sys.path.append("../dfs/")

    import client as dfs

    from collections import defaultdict

    term_frequency = defaultdict(int)
    for line in dfs.get_file_content(filename):
        for term in line.encode("utf-8").split():
            processed_term = term.lower().strip(",.;")
            if len(processed_term) > 0:
                term_frequency[processed_term] += 1

    NTerms = sum(term_frequency.values())
    logging.info("Found %d terms in %s" % (NTerms, filename))
    for term in term_frequency:
        tf = 1.0 * term_frequency[term] / NTerms
        logging.info("reduce_key=%s\tvalue=%f" % (term, tf))
        yield util.encode_term(term), ("%s %s" % (filename, tf))


# и записывает список документов для каждого терма во временный файл
def reducefn(k, vs):
    import util

    if len(k) > 100:
        print "Skipping posting list for term %s" % (util.decode_term(k))
        return {}
    with open("tmp/plist/%s" % k, "w") as plist:
        plist.write("\n".join(vs))
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
            term_plist[term] = f.read().split("\n")

    import sys

    sys.path.append("../dfs/")

    import client as dfs
    import json

    # Ваш псевдоним в виде строковой константы
    USERNAME = "maximmaximov"
    with dfs.file_appender("/%s/posting_list/%s" % (USERNAME, k)) as buf:
        buf.write(json.JSONEncoder().encode(term_plist))


s = mincemeat.Server()
plist_files = os.listdir("tmp/plist/")
s.map_input = mincemeat.MapInputSequence(plist_files)
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="")
