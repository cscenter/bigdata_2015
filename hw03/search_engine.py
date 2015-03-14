# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import json
import util

USERNAME = "dzaycev"


def search(search_str):
    metadata = dfs.CachedMetadata()

    scored = {}
    for word in search_str.split(" "):
        encoded = util.encode_term(word)

        shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, encoded[0:1]))])
        plists = json.JSONDecoder().decode(shard)

        idf = plists[encoded][encoded + "_idf"]
        for document, tf in plists[encoded].items():
            if document == encoded + "_idf":
                continue

            if document not in scored:
                scored[document] = 0

            scored[document] += tf * idf

    return sorted(scored, key=lambda x: x[1])[:10]


print(search("HANA"))