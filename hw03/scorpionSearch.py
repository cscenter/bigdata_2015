# encoding: utf-8

from collections import defaultdict
import mwclient
import argparse
import mwparserfromhell as mwparser
import sys
import json
import util
import sys
sys.path.append('../dfs/')
import client as dfs
USERNAME = 'antongulikov'

metadata = dfs.CachedMetadata()

print 'start'
querry_str = sys.argv[1:]
words = set()

for word in querry_str:
	words.add(word.lower())
	words.add((word+'.').lower())
#	if len(word) < 5:
#		continue
#	for j in xrange(0, len(word) - 4):
#		words.add(word[j:j+5].lower())

All = 0.0
magic = len('/wikipedia/page')
title = {}
for l in metadata.get_file_content('/wikipedia/__toc__'):
	a,b = l.split(' ', 1)
	title[int(a[magic:])] = b[:-1]
	All += 1.
#мера важности документа

All -= 1

result = defaultdict(float)

for word in words:
	encodeWord = util.encode_term(word)
	filename = "/%s/posting_list/%s" % (USERNAME, encodeWord[0:2])
	try:
		shard = "".join([l for l in metadata.get_file_content(filename)])
		plists = json.JSONDecoder().decode(shard)
#		print word, 'ok' , encodeWord
#		print plists
		for l in plists[encodeWord]:
			try:
				if l == "":
					continue
				name, tf, occ = l.split()
#				print name, tf, occ
				tf = float(tf)
				occ = float(occ)
				import math
#				print All
				result[int(name[magic:])] += tf * math.log(All / occ) 
			except:
#				print 'badbadbabdabdabd'
#				print word
				pass
	except:     
#		print 'something went wrong'
		pass


#вывожу 10 самых самых. 
#10 раз нходим максимум и того имеем сложность O(Maybe), Maybe - количество документов, где есть хотя бы один из термов.
All = int(All)
used = [False] * (All + 2)
for i in xrange(10):
	mi = -1
	pos = 0
	for x in xrange(1, All + 1):
		if (not used[x]) and (mi < result[x]):
			pos = x
			mi = result[x]
	print pos, title[pos], result[pos]
	used[pos] = True
#Конечно, если бы данных было бы >> чем сейчас лучше было бы запихнуть мои форики в мап-редьюс.
