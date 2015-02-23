#!/usr/bin/python
# encoding: utf8
# import time
# import test_dfs as dfs
import http_dfs as dfs


# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
def demo():
  for f in dfs.files():
    print("File {0} consists of fragments {1}".format(f.name, f.chunks))

  for c in dfs.chunk_locations():
    print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")
  # chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")

  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    print(line[:-1])


# Функция ищет файл в РФС по имени
def get_file(filename):
    for file in dfs.files():
        if filename == file.name:
            return file
    raise Exception("File %s does not exist" % filename)


# Функция принимает имя файла и возвращает итератор по его строкам.
def get_file_content(filename):
    file = get_file(filename)
    chunks = (chunk for chunk in dfs.chunk_locations() for chunk_id in file.chunks if chunk.id == chunk_id)
    for chunk in chunks:
        for line in dfs.get_chunk_data(chunk.chunkserver, chunk.id):
            yield line


# Функция ищет файл, содержащий ключ в диапазоне значений
def get_key_file(key):
    # chunk_iterator = dfs.get_chunk_data("cs0", "partitions")
    chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
    for chunk in chunk_iterator:
        if len(chunk) > 1:
            (first, last, file) = chunk[:-1].split()
            if (key >= first) and (key <= last):
                return file
    raise Exception("Key %s is not found" % key)


# Функция принимает название файла с ключами и возвращает сумму их значений
def calculate_sum(keys_filename):
    keys = (key[:-1] for key in get_file_content(keys_filename) if len(key) > 1)
    sum = 0
    for key in keys:
        key_file = get_key_file(key)
        for line in get_file_content(key_file):
            if len(line) > 1:
                (new_key, value) = line[:-1].split()
                if key == new_key:
                    sum += int(value)
                    break
    return sum

# start = time.time()
print(calculate_sum("/keys"))
# finish = time.time()
# print(finish - start)
# demo()
