#!/usr/bin/python
# encoding: utf8
from collections import defaultdict
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

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
#  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/

#находит расположение чанка
def get_chunk_location(chunk):
  for c in dfs.chunk_locations():
      if c.id == chunk:
        return c.chunkserver

def get_file_content(filename):
  
  chunks = []
  
  for f in dfs.files():
    if f.name == filename:
       chunks = f.chunks
  
  for chunk in chunks:
    for line in dfs.get_chunk_data(get_chunk_location(chunk), chunk):
      yield line
      
      
# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def find_filename(key, partitions):
  for line in partitions:
      l = line.split(' ')
      if l[0].isalnum():
        if (key >= l[0]) and (key <= l[1]):
          return l[2].strip()
    

def calculate_sum(keys_filename):
  keys = get_file_content(keys_filename)
  partitions = get_file_content("/partitions")
  
  sum = 0
  shard_key = defaultdict(list, {})
  
  for key in sorted(list(keys)):
    filename = find_filename(key.strip(), partitions)
    shard_key[filename].append(key.strip())
 
  for k, v in shard_key.items():
    for el in sorted(v):
      print("Searching for ...", el)
      for c in get_file_content(k):
        if el == c.split(' ')[0]:
          sum += int(c.split(' ')[1])
  
  print("result sum = ", sum)
  return sum   
  

demo()
calculate_sum("/keys")