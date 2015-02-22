#!/usr/bin/python3
# encoding: utf8

from collections import Counter, defaultdict
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


# По названию файла получает список его фрагментов.
def get_chunks(filename):
  for f in dfs.files():
    if f.name == filename or f.name == filename[1:]:
      return f.chunks
  raise "No file found"


# Для списка фрагментов возвращает список кортежей (фрагмент, сервер).
def get_servers(chunks):
  chunk_ls = dfs.chunk_locations()
  acc = []
  for chunk in chunks:
    flag = False
    for c in chunk_ls:
      if c.id == chunk:
        acc.append((c.id, c.chunkserver))
        flag = True
        break
    if not flag:
      raise "No chunkserver found"
  return acc


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunks = get_chunks(filename)
  for chunk, server in get_servers(chunks):
    for line in dfs.get_chunk_data(server, chunk):
      yield line


# Ищет, в каких файлах есть данный ключ.
def find_files(key, partitions="/partitions"):
  for string in get_file_content(partitions):
    try:
      first, last, filename = string.split()
      if first <= key <= last:
        yield filename 
    except Exception:
      pass


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename, debugging=True):
  count = Counter()
  dd = defaultdict(set)
  for key in map(lambda x: x[:-1], get_file_content(keys_filename)):
    count[key] = 0
    for filename in find_files(key):
      dd[filename].add(key)
  if debugging:
    print("Number of keys  - {}".format(len(count)))
    print("Number of files - {}".format(len(dd)))
    print("----------------------")
  for filename in dd:
    if debugging:
      print("Processing file - {}".format(filename))
    for line in get_file_content(filename):
      try:
        key, value = line.split()
        if key in dd[filename]:
          count[key] += int(value)
      except Exception:
        pass
  if debugging:
    print("----------------------")
    print(count)
    print("----------------------")
  return(sum(count.values()))


#demo()
#for line in get_file_content("/keys"): 
#  print(line, end="")

print(calculate_sum("/keys", debugging=True))