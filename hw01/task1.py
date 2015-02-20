#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
from setuptools.compat import basestring
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
# import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
# использованы исключительно для демонстрации)
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


files = {}

for c in dfs.files():
    files[c.name] = c.chunks

chunks = {}
for c in dfs.chunk_locations():
    chunks[c.id] = c.chunkserver


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам. 
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
    if not isinstance(filename, basestring):
        for line in filename:
            yield line[:-1]
    else:
        with open(filename) as f:
            for line in f:
                yield line[:-1]


search_files = []

if "/partitions" in files and "partitions" in chunks:
    for line in get_file_content(dfs.get_chunk_data(chunks["partitions"], "partitions")):
        f = line.strip().split()
        search_files.append([f[0], f[1], f[2]])


def get_number_by_file(filename):
    for element in search_files:
        if element[0] <= filename <= element[1]:
            for file in files[element[2]]:
                for line in get_file_content(dfs.get_chunk_data(chunks[file], file)):
                    if len(line.strip()) > 0:
                        line_arr = line.split()
                        if line_arr[0] == filename:
                            return line_arr[1]
    return 0


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum_all = 0
    for i in get_file_content(keys_filename):
        sum_all += int(get_number_by_file(i))
    return sum_all


print(calculate_sum("data/keys"))
# demo()