#!/usr/bin/python
# encoding: utf8

import test_dfs as dfs

#import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
def demo():
 # for f in dfs.files():
  #  print("File {0} consists of fragments {1}".format(f.name, f.chunks))
  #  print('             1111')

  for c in dfs.chunk_locations():
    print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
 # chunk_iterator = dfs.get_chunk_data("cs0", "partitions")
 
 

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  file = open(filename)
  for line in file:
      yield line      
    
def get_chunks(shard_name):
  for f in dfs.files():
      if f.name == shard_name:
          return f.chunks
          
def get_server_for_chunk(chunk):
  for c in dfs.chunk_locations():
      print(1)              

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  result = 0  
  file = open(keys_filename)
  
  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  for key in file:
      for line in dfs.get_chunk_data("cs0", "partitions"):
          (begin, end, shard_name) = line[:-1].split(' ')
          if key >= begin and key <= end:
              chunks = get_chunks(shard_name)
              for chunk in chunks:
                  server_name = get_server_for_chunk(chunk)
              break
   
  return result           
  
demo()
#calculate_sum('data/keys')
