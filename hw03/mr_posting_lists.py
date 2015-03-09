# -*- coding: utf-8 -*-
from __future__ import print_function
import mincemeat
import sys
sys.path.append('../dfs/')
import client as dfs

# Этот конвейер пока что работает только на одной машине
# (потому что результаты первого MR записываются в локальные файлы)


# Первый Map-Reduce отображает терм в документ
def mapfn(k, v):
    import json
    import util
    import sys
    sys.path.append('../dfs/')
    import client as dfs

    filename, pagetitle = k.split(' ', 1)
    print(k)

    words = {}
    for l in dfs.get_file_content(filename):
        for word in l.encode('utf-8').strip().split():
            if word in words:
                words[word] += 1
            else:
                words[word] = 1

    for word, frequency in words.items():
        yield json.dumps((util.encode_term(word), v)), (filename, frequency)


# и записывает список документов для каждого терма во временный файл
def reducefn(k, vs):
    import json
    import math
    import util

    k, count = json.loads(k)
    k = str(k)

    if len(k) > 100:
        print('Skipping posting list for term %s' % util.decode_term(k))
        return {}

    with open('tmp/plist/%s' % k, 'w') as plist:
        # plist.write('\n'.join(vs))
        idf = math.log(float(count) / len(vs))
        for doc, freq in vs:
            tf = 1 + math.log(freq)
            print(json.dumps((doc, tf * idf)), file=plist)

    return {}


s = mincemeat.Server()

# читаем оглавление корпуса википедии
wikipedia_files = list(dfs.get_file_content('/wikipedia/__toc__'))
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputSequenceWithCount(wikipedia_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password='')
