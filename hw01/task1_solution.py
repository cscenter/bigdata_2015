#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
#import test_dfs as dfs
import operator
from bisect import *

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
def demo():
  # for f in dfs.files():
  #   print("File {0} consists of fragments {1}".format(f.name, f.chunks))

  # for c in dfs.chunk_locations():
  #   print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
  # chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  for line in get_file_content("/shard_0"):
    print(line[:-1])
  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  # print("\nThe contents of chunk partitions:")
  # for line in chunk_iterator:
    # удаляем символ перевода строки
    # print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunks = []
  for f in dfs.files():
    if f.name == filename:
      chunks = f.chunks
  if len(chunks) == 0:
    return
  clocs = {}
  for c in dfs.chunk_locations():
    clocs[c.id] = c.chunkserver

  for chunk in chunks:
    try:
      loc = clocs[chunk]
      if loc == "":
        raise "ERROR: location of chunk %s is unknown" % chunk
      for l in dfs.get_chunk_data(loc, chunk):
        yield l
    except StopIteration:
      pass

# Читает содержимое файла /partitions и возвращает отсортированный
# массив ключей, в котором ключу-началу диапазона соответствует
# значение - имя файла, содержащего диапазон, а ключу - концу диапазона
# соответствует значение False
def read_partitions():
  parts = {}
  for line in get_file_content("/partitions"):
    line = line[:-1]
    if len(line) == 0:
      continue
    start, end, shard = line.split(" ", 2)
    parts[start] = shard
    parts[end] = False
  return sorted(parts.items(), key = operator.itemgetter(0))

# Возвращает имя шарда, в котором следует искать данный ключ k,
# на основании отсортированных границ диапазонов
def get_shard(k, partitions):
  keys = [t[0] for t in partitions]
  pos = bisect_left(keys, k)
  # если ключ больше конца последнего диапазона
  if pos == len(keys):
    raise Exception("ERROR: Key %s is not in any of the known partitions" % k)
  # если ключ попал на границу диапазона 
  if keys[pos] == k:
    if partitions[pos][1] == False:
      pos = pos - 1
    return partitions[pos][1]

  # Не на границе -- значит внутри диапазона (в таком случае pos находится на конце диапазона)
  # или между диапазонами (в таком случае pos на начале следующего)
  pos = pos - 1
  # было pos=0 если были до начала первого диапазона 
  if pos < 0:
    raise Exception("ERROR: Key %s is not in any of the known partitions" % k)
  if partitions[pos][1] == False:
    raise Exception("ERROR: Key %s is not in any of the known partitions" % k)
  return partitions[pos][1]

def sum_shard(shard, keys):
  result = 0
  cur_key = keys.pop(0)
  for line in get_file_content(shard):
    line = line[:-1]
    if len(line) == 0:
      continue
    k, v = line.split(" ", 1)
    if k == cur_key:
      result += int(v)
      # если ключей больше нет то выходим
      if len(keys) == 0:
        break
      cur_key = keys.pop(0)
  return result

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  total = 0
  parts = read_partitions()
  read_tasks = {}

  # прочитаем ключ, для каждого найдем имя шарда и запишем ключ в список искомых в этом шарде
  for k in get_file_content(keys_filename):
    k = k[:-1]
    shard = get_shard(k, parts)
    if not shard in read_tasks:
      read_tasks[shard] = []
    read_tasks[shard].append(k)

  # читаем все затронутые шарды и суммируем значения ключей, искомых в каждом шарде
  for shard in read_tasks.keys():
    keys = read_tasks[shard]
    keys.sort()
    print("Summing shard %s keys=%s" % (shard, keys))
    subtotal = sum_shard(shard, keys)
    print("  Subtotal for shard %s is %d" % (shard, subtotal))
    total += subtotal

  return total

print("Total sum=%d" % calculate_sum("/keys"))