#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
from setuptools.compat import basestring
import http_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
#import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
# использованы исключительно для демонстрации)
def demo():
    for f in dfs.files():
        print("File {0} consists of fragments {1}".format(f.name, f.chunks))

    for c in dfs.chunk_locations():
        print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

<<<<<<< HEAD
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
=======
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
>>>>>>> 6fd1ee7934cb6735ffbce3e4e8588c425332ce63
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


<<<<<<< HEAD
search_files = []
# print(chunks)
# print(files)
# print(list(dfs.get_chunk_data(chunks["partitions"], "partitions")))
if "/partitions" in files and "partitions" in chunks:
    for line in get_file_content(dfs.get_chunk_data(chunks["partitions"], "partitions")):
        # print(line)
        if line.strip() != "":
            f = line.strip().split()

            search_files.append([f[0], f[1], f[2]])


def get_number_by_file(filename):
    for element in search_files:
        if element[0] <= filename <= element[1]:
            # print(element[2])
            for file in files[element[2].replace("/", "")]:
                # print(file)
                for line in get_file_content(dfs.get_chunk_data(chunks[file], file)):
                    if len(line.strip()) > 0:
                        line_arr = line.split()
                        if line_arr[0] == filename:
                            return line_arr[1]
    return 0


=======
>>>>>>> 6fd1ee7934cb6735ffbce3e4e8588c425332ce63
# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum_all = 0
    for i in get_file_content(keys_filename):
        # print(keys_filename)
        # print(sum_all)
        sum_all += int(get_number_by_file(i))
    return sum_all


<<<<<<< HEAD
print(calculate_sum(dfs.get_chunk_data(chunks["keys"], "keys")))
# demo()
=======
demo()
>>>>>>> 6fd1ee7934cb6735ffbce3e4e8588c425332ce63
