#!/usr/bin/python
# encoding: utf8

import http_dfs as dfs

files = dfs.files() # получаем содержимое /files чтобы не обращаться каждый раз к нему через dfs   
chunk_locations = dfs.chunk_locations() # получаем содержимое /chunk_locations чтобы не обращаться каждый раз к нему через dfs        

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
def get_file_content(filename):
  file = open(filename)
  for line in file:
      yield line      
    
def get_chunks(shard_name):
  for f in files:
      if f.name == shard_name[1:]: # shard_name начинается с символа '/'
          return f.chunks
         
def get_server_for_chunk(chunk):
  for c in chunk_locations:
      if c.id == chunk:
          return c.chunkserver
       
def calculate_sum(key_chunk_name, chunkserver):
  result = 0  
  for keys in dfs.get_chunk_data(chunkserver, key_chunk_name):
      key = keys[:-1] # удаляем символ перевода строки
      partitions = () 
      for f in dfs.files(): # ищем где chunks для /partitions
          if f.name == "/partitions":
              partitions = f.chunks
              break
      for partition in partitions: # для каждого chunk файла /partitions ищем необходимый нам диапозон
          for c in dfs.chunk_locations():
              if c.id == partition:
                  #for line in dfs.get_chunk_data(c.chunkserver, partition):
                  for line in dfs.get_chunk_data("104.155.8.206", "partitions"):  
                      if len(line[:-1]) == 0:
                          continue
                      (begin, end, shard_name) = line[:-1].split(' ')
                      if key >= begin and key <= end:
                          chunks = get_chunks(shard_name)
                          haveFoundKey = False #уже нашли текущий ключ
                          for chunk in chunks:
                              server_name = get_server_for_chunk(chunk)
                              for line in dfs.get_chunk_data(server_name, chunk):
                                  if len(line[:-1]) == 0:
                                      continue
                                  (chunk_key, chunk_value) = line[:-1].split(' ')
                                  if key == chunk_key:
                                      result += int(chunk_value)
                                      haveFoundKey = True
                                      break
                              if haveFoundKey:
                                  break
                          break 
            #      break # 
  return result           
  
result_sum = 0
keys = ()
for f in files: # ищем где chunks для /keys
    if f.name == "/keys":
        keys = f.chunks
        break
for key in keys: # для каждого chunk файла /keys считаем    
    for c in chunk_locations:
        if c.id == key:
            result_sum += calculate_sum(key, c.chunkserver)
            break
print(result_sum)
