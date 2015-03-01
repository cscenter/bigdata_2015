#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
# использованы исключительно для демонстрации)
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


files = None
locations = None

def get_cached_files():
  global files
  if files is None:
    files = dfs.files()
  return files


def get_cached_locations():
  global locations
  if locations is None:
    locations = dfs.chunk_locations()
  return locations


def any_element(predicate, iterable):
  for it in iterable:
    if predicate(it):
      return it

  raise "Element not found"


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  file = any_element(lambda f: f.name == filename, get_cached_files())
  for chunk in file.chunks:
    location = any_element(lambda c: c.id == chunk, get_cached_locations())  # Учитываем, что сервер один
    for line in dfs.get_chunk_data(location.chunkserver, location.id):
      yield line


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  sorted_keys = sorted(map(lambda k: k[:-1], get_file_content(keys_filename)))
  count_keys = len(sorted_keys)

  key_num = 0
  value_sum = 0

  sorted_partitions = sorted(get_file_content("/partitions"))
  for partition in sorted_partitions:
    if not partition.strip():
      continue

    left, right, shard_name = partition.split()
    if left <= sorted_keys[key_num] <= right:
      for line in get_file_content(shard_name):
        if not line.strip():
          continue

        key, value = line.split()
        if sorted_keys[key_num] == key:
          value_sum += int(value)
          key_num += 1
        elif sorted_keys[key_num] < key:
          break

        if count_keys <= key_num:
          break

    if count_keys <= key_num:
      break

  return value_sum

# demo()
print(calculate_sum("/keys"))