# encoding: utf-8
# -*- coding: utf-8 -*-
from dfs_client import *
import mincemeat
#просто отправим запросы в Reduce
# n * m * q / magic  маперов.. не более столько
def mapfn(k, v):
	for l in get_file_content(v):
		key, value = l.split(' ')
		yield key, value         

# Времени очень мало. Сделаем все очень тупо. Для каждого числа будет файл, хранящий его.))) Есть даже небольшой плюс такого подхода.
# Есть поелементный доступ. Всего не более n * q / magic редьюсов. 
def reducefn(k, v):
	write_file(k, str(sum(map(int, v))) + '\n')
	return k      

s = mincemeat.Server()


precalc = []
for l in get_file_content('precalc.dat'):
	precalc.append(l)

s.map_input = mincemeat.MapInputDFSFileName(precalc)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
# пишем в файл все файлы описывающие матрицу
result = ''
for key, value in results.items():
    result += str(key) + '\n'
write_file('result.dat', result)