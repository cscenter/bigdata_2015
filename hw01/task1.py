#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
#import http_dfs as dfs

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

def get_shard_name(key):
  with dfs.get_chunk_data("cs0", "partitions") as f:
    for line in f:
      start, finish, name = line.rstrip().split()
      if start <= key and key <= finish:
        # print("keys from {} to {}, shard: {}".format(start, finish, name))
        return name
    raise "No shard found!"

def get_chunk_server(chunk_id, chunk_locations):
  for chunk in chunk_locations:
    if chunk.id == chunk_id:
      return chunk.chunkserver
  raise "Chunkserver not found!"

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunks = []
  for line in dfs.files():
    if line.name == filename:
      for chunk in line.chunks:
        cs = get_chunk_server(chunk, dfs.chunk_locations())
        chunks.append("/".join(["data", cs, chunk]))
  for chunk in chunks:
    with open(chunk) as f:
      for line in f:
        if len(line) > 2:
          yield line
  raise StopIteration

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  sum_ = 0
  with open(keys_filename) as keys_:
    for key in keys_:
      key = key.rstrip()
      shard = get_shard_name(key)
      for line in get_file_content(shard):
        key_, val_ = line.split()
        if (key == key_):
          sum_ += int(val_)
          break
  return sum_

print(calculate_sum("data/keys"))
