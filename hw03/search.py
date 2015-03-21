#encoding: utf-8
import json
import math
import sys
sys.path.append('../dfs')
import client as dfs
import util

from collections import defaultdict

#векторная модель строится следующим образом:
#каждый документ представлен как вектор tf, запрос-вектор из idf
#мера близости векторов считается через модуль расстояния между ними

 
q = raw_input("Please, insert your query here:")
q = q.split(' ')

doc_set = defaultdict(list) #множество документов, где встречается хотя бы один из терминов запроса, ключ - имя документа, значения - tf для каждого присутсвующего терма
docCounter = 0 #счетчик количества документов, где встречается конкретный терм
doc_amount = 62
idfList = []

for term in q:
  term = util.encode_term(term)
  try:
    for l in open('tmp/plist/%s' % term):
      l = l.strip().split(' ')
      doc_set[l[0]].append(l[1])
      docCounter += 1
    idf = math.log(doc_amount/float(docCounter))
  except:
    idf = 0
  idfList.append(idf)

pagetitles = {}
for l in dfs.get_file_content('/wikipedia/__toc__'):
    l = l.strip().split(" ", 1)
    pagetitles[l[0]] = l[1]

#проходя по данному циклу, мы выбираем 10 документов, для кот. мера близости была наибольшей  
rel_doc = sorted(doc_set.items(),
                   key=lambda(k, v):
                   math.sqrt(sum([(float(v[i])-idfList[i])**2 for i in xrange(len(v))])),
                   reverse=True)[:10]
if rel_doc:
  for i, j in rel_doc:
    print i, ' ', pagetitles[i]
else:
  print "There are no relevant results"
