#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs

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
  # chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

dfs_files = dfs.files()
dfs_chunk_locations = dfs.chunk_locations()

def chunks(filename):
  '''return list of chunk_id for file'''
  if filename is None:
    raise Exception("Empty filename!")
  for f in dfs_files:
    if f.name.strip("/") == filename.strip("/"):
      return f.chunks
  raise Exception("Chunks for '{}' not found!".format(filename))

def get_chunk_server(chunk_id):
  for chunk in dfs_chunk_locations:
    if chunk.id == chunk_id:
      return chunk.chunkserver
  raise Exception("Chunkserver for '{}' not found!".format(chunk_id))

def partitions(filename):
  pchunk_id, *_ = chunks(filename)
  return (get_chunk_server(pchunk_id), pchunk_id)

# tuple (chunk_server_id, chunk_id) for partitions
dfs_partitions = partitions("/partitions")

def get_shard_name(key):
  # with dfs.get_chunk_data("cs0", "partitions") as f:
    for line in dfs.get_chunk_data(*dfs_partitions):
      try:
        start, finish, name = line.rstrip().split()
        if start <= key and key <= finish:
          return name.strip("/")
      except ValueError:
        continue
    raise Exception("Shard for key '{}' not found!".format(key))


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunks_param = [(get_chunk_server(id), id) for id in chunks(filename)]
  for (server_id, chunk_id) in chunks_param:
    for line in dfs.get_chunk_data(server_id, chunk_id):
      yield line
  raise StopIteration

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  sum_ = 0
  # Для быстрого локального тестирования c test_dfs
  # for key in open("data" + keys_filename):
  # Для настоящего тестирования c http_dfs
  for key in get_file_content(keys_filename):
    key = key.rstrip()
    shard = get_shard_name(key)
    for line in get_file_content(shard):
      try:
        key_, val_ = line.rstrip().split()
        if (key == key_):
          sum_ += int(val_)
          break
      except ValueError:
        continue
  return sum_


print(calculate_sum("/keys"))
#demo()