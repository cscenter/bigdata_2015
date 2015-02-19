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

# Эту функцию надо реализовать. Функция принимает имя файла и 
# возвращает итератор по его строкам. 
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  raise "Comment out this line and write your code below"

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает 
# число
def calculate_sum(keys_filename):
  raise "Comment out this line and write your code below"

demo()