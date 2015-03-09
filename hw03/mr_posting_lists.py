# encoding: utf-8
import mincemeat
import os

import sys
sys.path.append("../dfs/")

import client as dfs

#Ваш старый мапик только теперь еще и считаем частоту вхождения слова и все разобъем на 5граммы
#
def mapfn(k, v):
	filename, pagetitle = v.split(" ", 1)
	print v, filename

	import sys
	sys.path.append("../dfs/")

	import client as dfs
	import math
	import util
	words = {}
	for l in dfs.get_file_content(filename):
		for word in l.encode("utf-8").split():
			if word == "":
				continue
			if word in words:
				words[word.lower()] += 1
			else:
				words[word.lower()] = 1
			if len(word) < 5:
				continue
			continue
			lenW = len(word)
			for j in xrange(0, lenW - 4):
				wrd = word[j:j+5].lower()
				if wrd in words:
					words[wrd] += 1
				else:
					words[wrd] = 1
												
	for word in words:
		yield word, [filename, 1 + math.log(words[word])]

# и записывает список документов для каждого терма во временный файл
def reducefn(k, vs):
	print 'reduce\n\n'
	import util
	k = util.encode_term(k)
	if len(k) > 50:
		print "Skipping posting list for term %s" % (util.decode_term(k))
		return {}
	print 'Done\n'
	size = ' '+ str(len(set(map(lambda x : x[0], vs))))+'\n'
	print 'Victory\n'
	with open("tmp/plist/%s" % k, "a") as plist:
		for fl in vs:
			plist.write(" ".join(map(str, fl)) + size)
	return {}

s = mincemeat.Server() 

# читаем оглавление корпуса википедии
wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputSequence(wikipedia_files) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 

# Второй Map-Reduce читает временные файлы и отображает первые 3 буквы, и дополняем '__' если букв не хватает. Слова длиной меньше трех вряд ли буду нести 
# полезную инфу.. и на них можно забить.
def mapfn1(k, v):
	yield k[0:2], v
# свертка собирает все списки вхождений для термов, имеющие одинаковую Scorpion кодировку
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
	USERNAME="antongulikov"
	with dfs.file_appender("/%s/posting_list/%s" % (USERNAME, k)) as buf:
		buf.write(json.JSONEncoder().encode(term_plist))

s = mincemeat.Server() 
plist_files = os.listdir("tmp/plist/")
s.map_input = mincemeat.MapInputSequence(plist_files) 
s.mapfn = mapfn1
s.reducefn = reducefn1

results = s.run_server(password="") 

                    	