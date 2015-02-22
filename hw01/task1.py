#!/usr/bin/env python3
# -*- coding: utf8 -*-

# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
# import http_dfs as dfs


# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs.
# Они использованы исключительно для демонстрации)
def demo():
    for f in dfs.files():
        print("File {0} consists of fragments {1}".format(f.name, f.chunks))

    for c in dfs.chunk_locations():
        print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

    # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
    # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
    # на использованных тут файл-серверах

    # При использовании test_dfs читаем из каталога cs0
    # chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

    # При использовании http_dfs читаем с данного сервера
    # chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
    chunk_iterator = dfs.get_chunk_data('130.211.64.240', 'shard_1_chunkaa')

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
    try:
        chunk_names = next(x for x in dfs.files() if x.name == filename).chunks
    except StopIteration:
        raise FileNotFoundError("File %s not found" % filename)
    chunks = filter(lambda x: x.id in chunk_names, dfs.chunk_locations())
    for chunk in chunks:
        data = dfs.get_chunk_data(chunk.chunkserver, chunk.id)
        for line in data:
            yield line.rstrip()


def demo2():
    temp = get_file_content('/shard_1')
    for l in temp:
        print(l)


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    # raise "Comment out this line and write your code below"
    keys_sum = 0
    keys = list(get_file_content(keys_filename))
    keys.sort()
    index = 0
    total = len(keys)
    partitions = get_file_content('/partitions')
    for line in partitions:
        begin, end, shard = line.split()
        print(begin, end)
        # rewrite to explicit iterator on data
        while index < total and keys[index] >= begin and keys[index] <= end:
            data = get_file_content(shard)
            for line in data:
                if line:
                    key, value = line.split()
                    if key == keys[index]:
                        keys_sum += int(value)
                        index += 1
                        if index >= total or not(keys[index] >= begin and keys[index] <= end):
                            break
            index += 1
    return keys_sum


def demo3():
    s = calculate_sum("/keys")
    print(s)


# demo()

# demo2()

demo3()
