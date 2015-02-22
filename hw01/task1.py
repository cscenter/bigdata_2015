#!/usr/bin/python
#encoding: utf-8

# Для быстрого локального тестирования используйте модуль test_dfs
#import test_dfs as dfs

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
  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  with open(filename) as inp:
    for line in inp:
      if line.strip():
        yield line.strip()


# Возвращает chunks данного файла
def chunks_by_name(name):
  for file in dfs.files():
    try:
      if file.name == name:
        return file.chunks
    except:
      raise Exception("Can not retrieve file name: {}".format(file))
  raise Exception("No such file {}".format(name))

#Получает сервер на котором хранится chunk
def server_by_chunk_id(id):
  for chunk in dfs.chunk_locations():
    try:
      if chunk.id == id:
        return chunk.chunkserver
    except:
      raise Exception("Can not chunk id: {}".format(chunk))
  raise Exception("No chunk found with id:{}".format(id))


# Получает шард в котором хранится диапазон ключей соответсвующий ключу
def get_shard_by_key(key, partition_server, partition_chunk):
  for line in dfs.get_chunk_data(partition_server, partition_chunk):
      if line.strip():
        if len(line.strip().split()) == 3:
          lkey, rkey, shard = line.strip().split()
          if lkey <= key <= rkey:
            return shard
        else:
          raise Exception("Invalid partition file format:{}".format(line))
  return None


# Получает значение соответсвующее ключу
def get_value(key, chunks):
  for chunk in chunks:
    server = server_by_chunk_id(chunk)
    f = False
    ans = 0
    for line in dfs.get_chunk_data(server, chunk):
      if line.strip() and len(line.strip().split()) == 2:
        p_key, value = line.strip().split()
        if p_key == key:
          if value.isdigit():
            ans += int(value)
            f = True
          else:
            raise Exception("Invalid value:{}".format(value))
    if f and ans:
      return ans
  raise Exception("Can not find key:{}".format(key))


#на удаленном сервере в файле partitions имена шардов не начинаются с '/'
def modify_shard(shard):
  if shard[0] == '/':
    return shard[1:]
  else:
    return shard
# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename=None, use_remote_keys=False):
  partition_chunks = chunks_by_name('/partitions')
  ans = 0
  #Файл /partitions может быть разбин на несколько chunk
  if not use_remote_keys:
    keys = get_file_content(keys_filename)
  else:
    keys_chunk = chunks_by_name('/keys')[0]
    key_server = server_by_chunk_id(keys_chunk)
    keys = dfs.get_chunk_data(key_server, keys_chunk)

  for key_n in keys:
    key = key_n.strip()
    found = False
    for partition_chunk in partition_chunks:
      partition_server = server_by_chunk_id(partition_chunk)
      shard = get_shard_by_key(key, partition_server, partition_chunk)
      #if use_remote_keys:
        #shard = modify_shard(shard)
      #print(key, shard)
      if shard:
        chunks = chunks_by_name(shard)
        val = get_value(key, chunks)
        #print(val)
        ans += val
        found = True
        break
    if not found:
      raise "Can not find shard containing key:{}".format(key)

  print("Calculated sum of views:{}".format(ans))


#print(dfs.files())
calculate_sum("data/keys", use_remote_keys=True)
