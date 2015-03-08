# encoding: utf-8
# -*- coding: utf-8 -*-

#Мапер берет файл и выплевывает ечейки в нужном количество, чтобы потом редьс их поперемножал. Будет работать дико медленно. Я вообще нигде не использую
#классный формат данных, но времени мало(.
#
from dfs_client import *
import mincemeat

def mapfn(k, v):
	n = 3
	m = 4
	q = 6
	matrixId = None
	curCol = 0
	curRaw = 0
	for l in get_file_content(v):
		if matrixId is None:
			matrixId, curRaw, finishRaw = map(int, l.split())
			curCol = 1
			continue
		for value in l.split():
			if matrixId == 1:
				for j in xrange(1, q + 1):
					yield '_'.join([str(curRaw),str(j),str(curCol)]), value
				curCol += 1
				if curCol > m:
					curCol = 1
					curRaw += 1
			if matrixId == 2:
				for j in xrange(1, n + 1):
					yield '_'.join([str(j),str(curCol),str(curRaw)]), value
				curCol += 1
				if curCol > q:
					curCol = 1
					curRaw = 1

def reducefn(k, v):
	i, j, index = map(int, k.split('_'))
	key = 'r'+str(i)+'c'+str(j)
	value = reduce(lambda x, y : int(x) * int(y), v);
	write_file(k, str(key) + ' ' + str(value) + '\n')
	return k                     

s = mincemeat.Server()

matrix_files = []
for l in get_file_content("/matrix1"):
	matrix_files.append(l)
for l in get_file_content("/matrix2"):
    matrix_files.append(l)

s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
# пишем в файл все файлы для следующего MapReducа
precalc = ''
for key, value in results.items():
    precalc = precalc + str(key) + '\n'
write_file('precalc.dat', precalc)