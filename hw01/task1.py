#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
# import http_dfs as dfs

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



# Support functions

def get_file_chunks(filename):
  if filename is None: raise Exception("No filename given")
  for f in dfs.files():
    if f.name == filename:
      return f.chunks
  raise Exception("File not found")

def get_chunk_locations(file_chunks):
  chunk_locations = []
  if file_chunks is None: raise Exception("No chunks given")
  for chunk in dfs.chunk_locations():
    if chunk.id in file_chunks:
      chunk_locations.append((chunk.chunkserver, chunk.id))
  return chunk_locations

# Эту функцию надо реализовать. Функция принимает имя файла и 
# возвращает итератор по его строкам. 
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunk_locations = get_chunk_locations(get_file_chunks(filename))
  for (server_id, chunk_id) in chunk_locations:
    with dfs.get_chunk_data(server_id, chunk_id) as f:
      for line in f:
        yield line[:-1]
  

def get_search_file(key):
  for l in get_file_content("/partitions"):
    (range_min, range_max, shard) = l.strip().split()
    if key >= range_min and key <= range_max:
      return shard
    if key < range_min: break 
  raise Exception("Key is out of range")


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает 
# число
def calculate_sum(keys_filename):
  res = 0
  with open(keys_filename) as f:
    for key in f:
      key = key.strip()
      for line in get_file_content(get_search_file(key)):
        if line == "": continue
        (found_key, found_value) = line.strip().split()
        if found_key == key:
          res += int(found_value)
          break
  return res
    

#demo()
