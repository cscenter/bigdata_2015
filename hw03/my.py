from collections import defaultdict

__author__ = 'alex'
import mwclient
import mwparserfromhell as mwparser
import sys
import json
import util
from math import log

sys.path.append("../dfs/")

import client as dfs

USERNAME = "mylnikovorg"
site = mwclient.Site('en.wikipedia.org')
category = site.Pages['Category:Big_data']
counter = 0

metadata = dfs.CachedMetadata()
for one in metadata.get_file_content("/wikipedia/__toc__"):
    print(one)

# query_in = raw_input("some query: ").strip()
query_in="architecture HANA"
D_long = int(len([document for document in dfs.get_file_content("/wikipedia/__toc__")]))
Q_long = int(len(query_in))


def get_tf(url, word):
    count = 0
    word_count = 0
    for line in dfs.get_file_content(url):
        # print line
        for one_word in line.split():
            one_word=one_word.strip()
            count += 1
            # print one_word
            if word in one_word:
                word_count += 1

    return float(word_count)/float(count)


mass = defaultdict(lambda: 0)

for word in query_in.split():
    print word

    shard = "".join(
        [l for l in metadata.get_file_content("/{}/posting_list/{}".format(USERNAME, util.encode_term(word)[0:1]))])
    plists = json.JSONDecoder().decode(shard)
    # print(plists)
    d = len(plists[util.encode_term(word)])
    idf = log(float(D_long) / float(d), 4)
    for docum in plists[util.encode_term(word)]:
        print(docum)
        D_id = docum.split("/")[2]
        # print(get_tf(docum, word))
        tf = get_tf(docum, word)#float(1 / float(d))
        mass[D_id] += float(tf) * idf
    # print(list(map(lambda x: x.split(), plists[util.encode_term(word)])))

for D_id in mass:
    mass[D_id] /= Q_long * D_long

for D_id in list(reversed(list(sorted(mass))))[:10]:
    print(D_id)