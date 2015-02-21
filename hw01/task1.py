#!/usr/bin/python
# encoding: utf8

import test_dfs as dfs

#import http_dfs as dfs

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
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
      if c.id == chunk:
          return c.chunkserver
       
# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  result = 0  
  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  for keys in get_file_content(keys_filename):
      key = keys[:-1]
      for line in dfs.get_chunk_data("cs0", "partitions"):
          (begin, end, shard_name) = line[:-1].split(' ')
          if key >= begin and key <= end:
              chunks = get_chunks(shard_name)
              for chunk in chunks:
                  haveFoundKey = False #уже нашли текущий ключ
                  server_name = get_server_for_chunk(chunk)
                  for line in dfs.get_chunk_data(server_name, chunk):
                      if line == '\n':
                          break
                      (chunk_key, chunk_value) = line[:-1].split(' ')
                      if key == chunk_key:
                          result += int(chunk_value)
                          haveFoundKey = True
                          break
                  if haveFoundKey:
                      break
              break
   
  return result           
  
#demo()
result_sum = calculate_sum('data/keys')
print(result_sum)
