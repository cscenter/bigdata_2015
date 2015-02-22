#!/usr/bin/python
# encoding: utf-8
#Anton Gulikov 
#SPb SU contest.gulikov@gmail.com
import time
import http_dfs as dfs
import copy
# ласс дл€ ключей, по которым нужно будет суммировать
#ќсновна€ иде€, реализованна€ здесь
#≈сть набор ключей - сначала разделим их по шардам
#ѕотом в каждой такой группе разделим по чанкам
#ƒл€ конкретного чанка и набора ключей посчитаем ответ
#ћы полностью пройдемс€ только по чанкам в которых точно есть наш ключ.
#я предположил, что количество шардов и чанков в одном шарде не столь велико, чтобы измер€тьс€ в гигабайтах, и некоторую информацию € все-таки сохран€л


class Key:
	def __init__(self, value, args):
		self.value = value;
		self.left = args[0]
		self.right = args[1]
		self.shard = args[2];

	def __cmp__(self, other):
		return cmp(self.shard, other.shard);

def get_file(filename):
	for f in dfs.files():
		if (f.name == filename):
			return f
	raise Exception("Error : Coudln't find a file %s" % (filename))


def get_file_content(filename):
	chunks_for_file = get_file(filename).chunks
	for chunk in chunks_for_file:
		for line in dfs.get_chunk_data(chunks[chunk], chunk):
			yield line

def get_partions():
#¬озращает содержимое /partion
	result = []
	for line in get_file_content('/partitions'):
		tmp = line.split(' ')
		if (len(tmp) < 3):
			continue
		tmp[2] = tmp[2][:-1]
		if (tmp[0] > tmp[1]):
			raise Exception("Partitions are not valid")
		result.append(tmp)
	return result

#посчитаем ответ дл€ конкретного чанка просто двига€ указатель по нему
def calc_for_chunk(keys, chunk_name):
	size = len(keys)
	index_k = 0
	result = 0;

	for line in dfs.get_chunk_data(chunks[chunk_name], chunk_name):
		tmp = line[:-1].split(' ');
		if (tmp[0] == keys[index_k].value):
			result += int(tmp[1]);
			index_k += 1;
			if (index_k == size):
				return result;

	return result;  			
	
#дл€ каждого чанка сначала посмотрим на первую строку в нем. “огда мы спокойно можем понимать дл€ конкретного ключа лежит ли он в определнном чанке или нет
#ну и потом двум€ указател€ми подсчитываем дл€ конкретного чанка и тех ключей которые в него попали наше значение

def calc_for_shard(keys):
	shard_name = keys[0].shard
	chunks_list = get_file(shard_name).chunks
	start_keys = []
	for x in chunks_list:
		for line in dfs.get_chunk_data(chunks[x], x):
			start_keys.append(line.split(' ')[0])
			break

	result = 0
	size = len(keys);
	inf = '';
	for i in xrange(len(keys[-1].value)):
		inf = inf + 'z';
	start_keys.append(inf);
	index_k = 0

	for index in xrange(len(start_keys)):
		start_index = index_k;
		if (not (start_keys[index] <= keys[index_k].value and
		         keys[index_k].value < start_keys[index + 1])):
			continue
		while (start_keys[index] <= keys[index_k].value and
			   keys[index_k].value < start_keys[index + 1]):
			index_k += 1
			if (index_k == size):
				break
		result += calc_for_chunk(keys[start_index:index_k], chunks_list[index]);
		if (index_k == size):
			break
#	sorted(querry, key = lambda x : chunks[chunks_list[x[2]]]);
#	print querry;
	return result;
		
#получим ключи, интервальчики, распределим ключики по шардам, и дл€ каждого конкретного шарада посчитаем ответ.
def calculate_sum(keys_filename):
	keys = []
	for key in get_file_content(keys_filename):
		keys.append(key[:-1])

	for key in keys:
		for symbol in key:
			if ('a' <= symbol and symbol <= 'z'):
				continue;
			if ('0' <= symbol and symbol <= '9'):
				continue;
			raise Exception('Key %s consists not only from [a-z0-9]' % key);

	buckets = get_partions()

#дл€ каждого шарда найдем максимальный и минимальный элемент в нем
	sorted(buckets, key = lambda x : x[1]);
	keys.sort();
#Ќайдем те интервалы, которые нас интересуют, заодно разобъем ключи по шардам
	len_b = len(buckets)
	len_k = len(keys)
	index_b = 0
	index_k = 0
	new_keys = []


	while (index_b < len_b and index_k < len_k):
		if (buckets[index_b][1] < keys[index_k]):
			index_b += 1
			continue
		if (buckets[index_b][0] <= keys[index_k] and keys[index_k] <= buckets[index_b][1]):
			new_keys.append(Key(keys[index_k], buckets[index_b]));
			index_k += 1
			continue;


	result = 0

	new_keys.sort();
	if len(new_keys) < 1:
		return 0
	candidats = [new_keys[0]]
	for index in xrange(1, len(new_keys)):
		if (candidats[0].shard == new_keys[index].shard):
			candidats.append(new_keys[index])
		else:
			result += calc_for_shard(candidats);
			candidats = [new_keys[index]];

	result += calc_for_shard(candidats);
	return result;		

#сохраним дл€ каждого чанка его сревер
start = time.time()
chunks = {}
for chunk in dfs.chunk_locations():
	chunks[chunk.id] = chunk.chunkserver
print calculate_sum('/keys')
finish = time.time()
print(finish - start)
#¬опросы как ускор€ть, возможно, что если мы не будем каждый раз мен€ть сервер, к которому надо обращатьс€ будет быстрее