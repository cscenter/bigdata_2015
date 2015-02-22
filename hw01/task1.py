#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs

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
  chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

def find_file(filename):
    for f in dfs.files():
        if filename == f.name:
            return f
    raise "File not found"
# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/

def get_file_content(filename):
    f = find_file(filename)
    file_chunks = (c for fragment in f.chunks for c in dfs.chunk_locations() if c.id == fragment) # находим все чанки этого файла
    return (line[:-1] for c in file_chunks for line in dfs.get_chunk_data(c.chunkserver, c.id) if len(line[:-1]) > 0) # возвращаем генератор строчек файла, обрезая символ перехода строки, и игнорируя пустые строчки

#не совсем понял где будет хранится файл с ключами, эта функция считывает ключи из файла который не в РФС
# def get_local_file_content(filename):
#     file = open(filename)
#     return (line[:-1] for line in file)

#следующие 3 функции вытаскивают тройку значений из строки (границы диапозона и имя файла)
def get_start(diaposone):
    return diaposone.split(' ')[0]
def get_finish(diaposone):
    return diaposone.split(' ')[1]
def get_filename(diaposone):
    return diaposone.split(' ')[2][:-1]

#функция возвращает имя файла, в котором возможно хранится данный ключ
def get_filename_for_key(key):
    # при использовании http_dfs
    for d in dfs.get_chunk_data("104.155.8.206", "partitions"):
    # при использовании test_dfs
    # for d in dfs.get_chunk_data("cs0", "partitions"):
        start = get_start(d)
        finish = get_finish(d)
        filename = get_filename(d)
        if (key >= start) and (key <= finish):
            return filename
    raise "File for key {0} not found".format(key)

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum = 0
    # если файл с ключами находится в нашей РФС
    keys = get_file_content(keys_filename)
    # keys = get_local_file_content(keys_filename)
    for k in keys:
        print(k)
        filename = get_filename_for_key(k)
        for l in get_file_content(filename):
            if (k == l.split(' ')[0]): # сраниваем ключ с ключём в файле
                sum += int(l.split(' ')[1]) # прибавляем из файла значение соответствующего ключу
                break
    return sum


# demo()
print(calculate_sum("data\keys"))


