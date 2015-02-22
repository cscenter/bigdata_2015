#!/usr/bin/python
# encoding: utf8
#blabla
# Для быстрого локального тестирования используйте модуль test_dfs
import http_dfs as dfs

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
  #chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
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
    # raise "Comment out this line and write your code below"
    #f = open(filename, 'r')
    f = dfs.urlopen(url = filename, timeout = 10)
    for line in f:
        yield line[:-1]
    f.close()
#эта функция возвращает итератор на все нужные шарды
def get_chunks_of_shard(shard):
    #замнить dfs.files() на list_files, вычисленный раньше
    for i in dfs.files():
        if '/' + i.name == shard:
            list_chunks = i.chunks
            break
    for j in list_chunks:
        yield j

def find_freq_of_key_in_chunk(chunk, key):
    #заменить dfs.chunk_locations() на list_chunk_locations, вычисленный раньше
    for i in dfs.chunk_locations():
        if i.id == chunk:
            serv = i.chunkserver
            break
    strings = dfs.get_chunk_data(serv, chunk)
    res = -1
    for j in strings:
        pair = j.split(" ")
        if pair[0] == key:
            res = int(pair[1])
            break
    return res



# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    #raise "Comment out this line and write your code below"
    sum = 0
    keys = get_file_content(keys_filename)
    for i in keys:
        # всегда ли будет такой путь???
        strings_from_partitions = get_file_content("http://104.155.8.206/chunks/partitions")
        for j in strings_from_partitions:
            if j == '':
                continue
            interval_and_shard = j.split(" ")
            if i>=interval_and_shard[0] and i<=interval_and_shard[1]:
                chunks_of_shard = get_chunks_of_shard(interval_and_shard[2])
                freq = -1
                for k in chunks_of_shard:
                    freq = find_freq_of_key_in_chunk(k, i)
                    if freq != -1:
                        break
                #что если freq всё-таки остался равен -1
                sum = sum + freq
    return sum



print calculate_sum("http://104.155.8.206/chunks/keys")
#demo()

