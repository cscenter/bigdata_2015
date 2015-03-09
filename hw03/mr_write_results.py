# -*- coding: utf-8 -*-
from __future__ import print_function
import mincemeat
import os
import sys
sys.path.append('../dfs/')
import client as dfs


# Второй Map-Reduce читает временные файлы и отображает первую букву файла в терм
def mapfn(k, v):
    yield k[0:1], v


# свертка собирает все списки вхождений для термов, начинающихся на одну и ту же букву,
# составляет из них словарь с весами, сериализует его и записывает в файл на DFS
def reducefn(k, vs):
    import json
    import sys
    sys.path.append('../dfs/')
    import client as dfs

    term_plist = {}
    for term in vs:
        with open('tmp/plist/%s' % term) as f:
            term_plist[term] = [json.loads(x) for x in f.read().split('\n') if x]

    # Ваш псевдоним в виде строковой константы
    USERNAME = 'nightuser'
    with dfs.file_appender('/%s/posting_list/%s' % (USERNAME, k)) as buf:
        print(json.dumps(term_plist), file=buf)


s = mincemeat.Server()
plist_files = os.listdir('tmp/plist/')
s.map_input = mincemeat.MapInputSequence(plist_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password='')
