#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs
from collections import Counter, defaultdict
import sys

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

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  locate = {l.id:l.chunkserver for l in dfs.chunk_locations()}
  server = locate[filename]
  return dfs.get_chunk_data(server, filename)

def find_keys(keys, partitions):
  key_num = 0
  part_num = 0 # хотим найти соответствия за линию. Можем это сделать, т.к. оба отсоритрованы
  res_dict = {}
  count = Counter() # будем считать, сколько каких фрагментов встретилось, чтобы потом быстро к этому фрагменту обратиться
  this_part = next(partitions)
  par_line = this_part.strip().split()
  if par_line:
    count[par_line[2]] += 1
  while key_num < len(keys):
    if not par_line:
      this_part = next(partitions)
      par_line = this_part.strip().split()
      continue # встречаются пустые строки

    if (keys[key_num] >= par_line[0]) and (keys[key_num] <= par_line[1]):
      res_dict[keys[key_num]] = (par_line[2], count[par_line[2]] - 1)
      key_num += 1

    if (key_num < len(keys)):
      if not ((keys[key_num] >= par_line[0]) and (keys[key_num] <= par_line[1])):
        this_part = next(partitions) #читаем следующее, если тут ключи уже закончались
        par_line = this_part.strip().split()
        count[par_line[2]] += 1
  return res_dict

def get_fragment_name(key_value):
  file_list = {l.name:l.chunks for l in dfs.files()}
  keys = {key:file_list[key_value[key][0]][key_value[key][1]] for key in key_value}
  return keys
    

def get_sum_in_file(keys, file_content, keys_count):
  keys.sort()
  key_num = 0
  key_sum = 0
  while key_num < len(keys):
    this_key = next(file_content)
    key_line = this_key.strip().split()
    if not key_line:
      continue # встречаются пустые строки
    if keys[key_num] == key_line[0]:
      key_sum += int(key_line[1]) * keys_count[keys[key_num]]
      key_num += 1
  return key_sum



# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  keys = sorted([line.strip() for line in open(keys_filename)])
  locate = dict(dfs.chunk_locations())
  partitions = get_file_content("partitions")
  
  fragments = get_fragment_name(find_keys(keys, partitions))
  shards = defaultdict(list)
  keys_count = Counter(keys)
  for key in fragments:
    shards[fragments[key]].append(key)
  total_sum = 0
  for shard in shards:
    total_sum += get_sum_in_file(shards[shard], get_file_content(shard), keys_count)  # мы потеряли повторяющиеся ключи. восстановим их и запишем все для файла
  return total_sum

print(calculate_sum("data/keys"))
# demo()
