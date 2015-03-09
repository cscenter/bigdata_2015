# encoding: utf-8
import mincemeat
import os

import sys
sys.path.append("../dfs/")

import client as dfs

# Это последовательность из двух Map-Reduce
# Диспетчер запускается командой python mr_posting_lists.py
# Рабочий процесс запускается командой python mincemeat.py localhost
# для каждого из Map-Reduce. То есть, когда отработает первый рабочий процесс,
# нужно запустить эту же команду еще раз
#
# Этот конвейер пока что работает только на одной машине
# (потому что результаты первого MR записываются в локальные файлы)

# Первый Map-Reduce отображает терм в документ
def mapfn(k, v):
#здесь надо реализовать подсчёт количества встреч терма в докумнте и общее количество термов в слове, а затем по ним вычислить tf
    import util
    filename, pagetitle = v.split(" ", 1)
#    print v

    import sys
    sys.path.append("../dfs/")

    import client as dfs
    words = {}
    count_of_words = 0
    for l in dfs.get_file_content(filename):
        for word in l.encode("utf-8").split():
            word = word.lower()
            if (words.has_key(word)):
                words[word] += 1
            else:
                words[word] = 1
            count_of_words += 1
    for word in words:
        tf = float(words[word]) / count_of_words
        yield util.encode_term(word), filename + ' ' + str(tf) + ' ' + str(count_of_words)

# и записывает список документов для каждого терма во временный файл
def reducefn(k, vs):
    import util
    if len(k) > 100:
        print "Skipping posting list for term %s" % (util.decode_term(k))
        return {}
    count_of_docs_with_term = ' ' + str(len(vs)) + '\n'
    with open("tmp/plist/%s" % k, "w") as plist:

        for s in vs:
            plist.write(s + count_of_docs_with_term)
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
	USERNAME= "sukhochev"
	with dfs.file_appender("/%s/posting_list/%s" % (USERNAME, k)) as buf:
		buf.write(json.JSONEncoder().encode(term_plist))

s = mincemeat.Server()
plist_files = os.listdir("tmp/plist/")
s.map_input = mincemeat.MapInputSequence(plist_files)
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="")