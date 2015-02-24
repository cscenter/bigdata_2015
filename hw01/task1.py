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

def get_chunks(filename):
  for f in dfs.files():    
    if f.name == filename:
      return f.chunks

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunck_serv = list()
  chunks = get_chunks(filename)
  for chunk in chunks:
    for serv in dfs.chunk_locations():
      if serv.id == chunk:
        chunck_serv.append([serv.chunkserver, chunk])
  for el in chunck_serv:
    data = dfs.get_chunk_data(el[0],el[1])
    for line in data: 
      yield line

def get_keys_shard(keys_filename):
  keys = get_file_content(keys_filename)
  key_shard = {}
  for k in keys:
    key = k.strip()
    partitions = get_file_content('/partitions')
    for line in partitions:
      p = line.split()
      if (key >= p[0]) and (key <= p[1]):
        key_shard[key] = p[2]
  return key_shard

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  key_shard = get_keys_shard(keys_filename)
  summ = 0
  for key in key_shard:
    content = get_file_content(key_shard[key])
    for line in content:
      data = line.split()
      if data:
        if key == data[0]:
          summ += int(data[1])
  return summ

demo()
