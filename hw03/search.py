# encoding: utf-8
import sys
sys.path.append("../dfs/")

import client as dfs
import json
import math
import util
from collections import defaultdict

metadata = dfs.CachedMetadata()

# Замечание: mr-posting-list измене так, что tf слов хранится рядом c индификатором файла

USERNAME="nizshee"
# Считает, сколько всего файлов в индексе (нужно для idf) 
total = float(len([l for l in dfs.get_file_content("/wikipedia/__toc__")]))
# На вход подается множество, в котором все слова начинаются на одну букву
# и словарь, в который будет записываться результат 
def func(request, d):
    # Узнаем, на какую букву начинаются слова
    s = request.pop()
    request.add(s)
    # Загружаем часть индекса, начинающуюся с этой буквы
    shard = "".join([l for l in metadata.get_file_content("/%s/posting_list/%s" % (USERNAME, s[0]))])
    plists = json.JSONDecoder().decode(shard)
    # Для каждого слова из запроса
    for word in request:
        # Считаем idf этого слова
        idf = math.log(total / len(plists[word]))
        # И аккумулируем для каждого документа произведение tf * idf
        for wiki, tf in map(lambda x: x.split(), plists[word]):
            d[wiki] += float(tf) * idf

alpha = " "
d = defaultdict(lambda: 0)
# Разбивает запрос на группы из слов, начинающиеся с одной буквы, и отправляет их func
for word in sorted(map(lambda x: util.encode_term(x), sys.argv[1:])):
    if word[0] == alpha:
        request.add(word)
        continue
    if alpha != " ":
        func(request, d)
    request = set()
    request.add(word)
    alpha = word[0]
func(request, d)
# Сортируем ответ и выводим
answers = sorted(d.items(), cmp=lambda x, y: 1 if x[1] > y[1] else (-1 if x[1] < y [1] else 0), reverse=True)[:10]
for answer in answers:
    print(answer[0])

