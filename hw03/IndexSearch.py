import sys
sys.path.append("../dfs/")

import client as dfs
import json
import math
import util

metadata = dfs.CachedMetadata()

USERNAME = "kozmirchuk"

query = sys.argv[1:]
query_words = set()
for word in query:
    query_words.add(util.encode_term(word))
amount_of_documents = 0.0
titles = {}
for line in metadata.get_file_content('/wikipedia/__toc__'):
    docId, title = line.split(' ', 1)
    titles[docId] = title
    amount_of_documents += 1

result = {}
for word in query_words:
    filename = "/%s/posting_list/%s" % (USERNAME, word[0])
    try:
        shard = "".join([l for l in metadata.get_file_content(filename)])
        plist = json.JSONDecoder().decode(shard)
        if not word in plist: continue
        idf = math.log( amount_of_documents / len(plist[word]))
        for line in plist[word]:
            docId, tf = line.split(' ')
            result[docId] = result.get(docId,0) + float(tf) * idf
    except:
        #print 'something is wrong'
        pass

 # тут стоит не сортировать, а искать мап-редьюсом, будет быстрее   
for x in sorted(map(lambda x, y: (y,x), result.keys(), result.values()))[:10]:
    print x[1][len('/wikipedia/page'):], titles[x[1]]
