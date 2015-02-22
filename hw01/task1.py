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

# дял начала я сделаю свою демо которя будет показывать имеющиеся 
# DFS файлы, разположение их фрагментов и содержимое заданного фрагмента 
# с заданного  сервера
def demo2(server = "cs0", partitions = "partitions"):
  for f in dfs.files():
    print("File {0} consists of fragments {1}".format(f.name, f.chunks))
#    
     
 # for c in dfs.chunk_locations():
  #  print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

#   print("\nThe contents of chunk partitions:")
 # with dfs.get_chunk_data(server, partitions) as f:
  #  for line in f:
      # удаляем символ перевода строки 
   #   print(line[:-1])
#################################################################################

# Эту функцию надо реализовать. Функция принимает имя файла и 
# возвращает итератор по его строкам. 
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  for f in dfs.files():
    if f.name == filename:
      print("File {0} consists of fragments {1}".format(f.name, f.chunks))
      for c in dfs.chunk_locations():
        if (c.id == f.chunks[0]):
          print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))
          print("\nThe contents of chunk {0}:".format(c.id))
          with dfs.get_chunk_data(c.chunkserver, f.chunks[0]) as f:
            for line in f:
              print(line[:-1])       # удаляем символ перевода строки 
          return
#  print(dir(files[0]))
#  print(type(files[0]))
#  print(files.index(filename))
#  raise "Comment out this line and write your code below"

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает 
# число
def calculate_sum(keys_filename = "./data/keys"):
  keyShard = {}
  f = open(keys_filename, "r")
  s = f.read()
  f.close()
  keys = sorted(list((set(s.split('\n')))))
  if len(keys[0]) < 1: keys = keys[1:]
  myCoolSum = 0;
  #for key in keys:
    #
  print(keys)
  return 42
#  raise "Comment out this line and write your code below"

get_file_content("/partitions")
#demo2()
#calculate_sum()




























































