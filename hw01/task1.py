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


#возвращает номер сервера, где лежит чанк
def get_chunk_location(chunk):
  for c in dfs.chunk_locations():
      if c.id == chunk:
        return c.chunkserver


#возвращает файл построчно
def get_file_content(filename):
  chunks = []
  
  for f in dfs.files():
    if f.name == filename:
       chunks = f.chunks
  
  for chunk in chunks:
    for line in dfs.get_chunk_data(get_chunk_location(chunk), chunk):
      yield line

      
#возвращает массив генераторов чанков по первой строке
def find_chunk(filename, intervals):
  print(filename, intervals[0])
  chunks=[]
  chunks_data=[]
  i=0
  
  for f in dfs.files():
    if f.name == filename:
      chunks = f.chunks
  
  for chunk in chunks:
    if next(dfs.get_chunk_data(get_chunk_location(chunk), chunk)).split(' ')[0] == intervals[i]:
      data = (get_chunk_location(chunk), chunk)
      chunks_data.append(data)
      if i == len(intervals)-1:
        break
      else:
        i += 1
  return chunks_data


#возвращает строку, содержащую путь к файлу
def find_filename(key, partitions):
  for line in partitions:
      l = line.split(' ')
      if l[0].isalnum():
        if (key >= l[0]) and (key <= l[1]):
          return line
    

def calculate_sum(keys_filename):
  keys = get_file_content(keys_filename)
  partitions = get_file_content("/partitions")
  
  sum = 0
  shard_int = defaultdict(list, {})
  
  for key in sorted(list(keys)):
    filename = find_filename(key[:-1], partitions)
    shard_name = filename.split(' ')[2][:-1]
    interval = filename.split(' ')[0]
    if shard_name not in shard_int:
      shard_int[shard_name] = defaultdict(list, {})
    shard_int[shard_name][interval].append(key[:-1])

  for k, v in shard_int.items():
    v2 = sorted(v)
    chunks = find_chunk(k, v2)
    for inter in v2:
      for el in sorted(v[inter]):
        data = chunks[v2.index(inter)]
        chunk = dfs.get_chunk_data(data[0], data[1])
        print("Searching for ...", el)
        a = next(chunk)
        b = a.split(' ')[0]   
        while el != b:
          a = next(chunk)
          b = a.split(' ')[0]
        sum += int(a.split(' ')[1])
        print(sum)
     
  print("result sum = ", sum)        
  return sum
  

demo()
calculate_sum("/keys")