#!/usr/bin/python
# encoding: utf8
import time
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
    return (line[:-1] for c in file_chunks for line in dfs.get_chunk_data(c.chunkserver, c.id)) # возвращаем генератор строчек файла, обрезая символ перехода строки


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
        if (len(d[:-1]) == 0): # проверяем строку на пустую
            continue
        start = get_start(d)
        finish = get_finish(d)
        filename = get_filename(d)
        if (key >= start) and (key <= finish):
            return filename[1:] # костыль для удаления символа '/', т.к. имена файлов в partition и в files различаются
    raise "File for key {0} not found".format(key)


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum = 0
    keys = get_file_content(keys_filename)
    for k in keys:
        if len(k) == 0: #игнорируем пустые строчки
            continue
        filename = get_filename_for_key(k)
        for l in get_file_content(filename):
            if len(l) == 0: # игнорируем пустые строчки
                continue
            if (k == l.split(' ')[0]): # сраниваем ключ с ключём в файле
                sum += int(l.split(' ')[1]) # прибавляем из файла значение соответствующего ключу
                break
    return sum


def get_file_chunks(filename):
    f = find_file(filename)
    return (c for fragment in f.chunks for c in dfs.chunk_locations() if c.id == fragment)

#проверяет, может ли содержаться данный ключ в данном чанке
def key_in_chunk(key, chunk):
    data = dfs.get_chunk_data(chunk.chunkserver, chunk.id)
    first = data.__next__()
    last = first
    for l in data:
        last = l
    return (key >= first) and (key <= last)

def find_key_value_in_chunk(key, chunk):
    for l in dfs.get_chunk_data(chunk.chunkserver, chunk.id):
        if key == l.split(' ')[0]:
            return int(l.split(' ')[1])

def calculate_sum2(keys_filename):
    sum = 0
    keys = get_file_content(keys_filename)
    for k in keys:
        filename = get_filename_for_key(k)
        for c in get_file_chunks(filename):
            if key_in_chunk(k, c):
                sum += find_key_value_in_chunk(k, c)
    return sum


# demo()
start = time.time()
print(calculate_sum("/keys"))
finish = time.time()
print(finish - start)

# оказалось медленнее процентов на 20
# start = time.time()
# print(calculate_sum2("/keys"))
# finish = time.time()
# print(finish - start)


