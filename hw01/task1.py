#!/usr/bin/python
# encoding: utf8

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

def get_file(filename):
    for currFile in dfs.files():
        if(currFile.name == filename):
            return currFile
    raise Exception('file not found')

def get_chunk_location(chunk):
    for x in dfs.chunk_locations():
            if(x.id == chunk):
                return x.chunkserver

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
    file = get_file(filename)
    for chunk in file.chunks:
        server = get_chunk_location(chunk)
        for stringLine in dfs.get_chunk_data(server, chunk):
            yield stringLine



def get_shard_for_key(key):
    for line in dfs.get_chunk_data("104.155.8.206", "partitions"):
        if len(line[:-1]) == 0:
            continue
        left, right, shard = line[:-1].split(' ')
        if (left <= key) and (right >= key):
            return shard
    raise Exception("Shard not found for {0}".format(key))

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    answer = 0
    keys = get_file_content(keys_filename)

    for key in keys:
        filename = get_shard_for_key(key)
        for line in get_file_content(filename):
            if (len(line) != 0) and (line.split(' ')[0] == key):
                answer += int(int(line.split(' ')[1]))
                break
    return answer

print(calculate_sum("/keys"))

demo()
