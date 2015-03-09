# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import json
import math
import util
from collections import Counter

metadata = dfs.CachedMetadata()

USERNAME="rurenko"

total = float(len([l for l in dfs.get_file_content("/wikipedia/__toc__")]))
c = Counter()
query = [util.encode_term(x) for x in sorted(sys.argv[1:])]
plists = None
letter = query[0][0]
for word in query:
    if plists is None or letter != word[0]:
        letter = word[0]
        shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, letter))])
        plists = json.JSONDecoder().decode(shard)
    if not word in plists: continue
    idf = math.log(total / len(plists[word]))
    for page, tf in map(lambda x: x.split(), plists[word]):
        c[page] += float(tf) * idf

for a, b in c.most_common()[:10]:
    print a