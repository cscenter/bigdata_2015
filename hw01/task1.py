#!/usr/bin/python
# encoding: utf8

import time

# Для быстрого локального тестирования используйте модуль test_dfs
#import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
def demo():
  for f in dfs.files():
    print("File {0} consists of fragments {1}".format(f.name, f.chunks))

  for c in dfs.chunk_locations():
    print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  print("\nThe contents of chunk partitions:")
  with dfs.get_chunk_data("cs0", "partitions") as f:
    for line in f:
      # удаляем символ перевода строки
      print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
    file = open(filename)
    for line in file:
        yield line

def get_shard_by_key(key, partitions):
    for line in partitions:
        temp = line[:-1].split(' ')
        if len(temp) != 3:
            raise Exception("Invalid input format")
        L, R, shard = temp
        if (L <= key) & (key <= R):
            return shard
    raise Exception("Can't find shard for key {0}!".format(key))

def get_chunks_by_shard(shard):
    for f in dfs.files():
        if f.name == shard:
            return f.chunks
    raise Exception("Where is shard {0}?".format(shard))

def get_chunk_server(chunk):
    for c in dfs.chunk_locations():
        if chunk == c.id:
            return c.chunkserver
    raise Exception("Where is chunk {0}?".format(chunk))

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    start_time = time.clock()

    partition_chunk = 0
    for f in dfs.files():
        if f.name == "/partitions":
            partition_chunk = f.chunks[0]
            break
    if partition_chunk == 0:
        raise Exception("There is no file with partitions")

    partition_cs = 0
    for c in dfs.chunk_locations():
        if c.id == partition_chunk:
            partition_cs = c.chunkserver
    if partition_cs == 0:
        raise Exception("There is no file with partitions")

    # узнали, где находится файл, описывающий разбиение данных

    #print("Time used for finding partitions time: {0} seconds".format((time.clock() - start_time)))

    result_sum = 0
    for key in get_file_content(keys_filename):
        key = key[:-1]
        shard = get_shard_by_key(key, dfs.get_chunk_data(partition_cs, partition_chunk))
        chunks = get_chunks_by_shard(shard)
        for ch in chunks:
            with dfs.get_chunk_data(get_chunk_server(ch), ch) as f:
                for line in f:
                    if (len(line) > 0) & (line.split(' ')[0] == key):
                        result_sum += int(line.split(' ')[1])
                        break
    #print("Used time: {0} seconds".format(int(time.clock() - start_time)))
    return result_sum

#demo()
print(calculate_sum("data\keys"))
