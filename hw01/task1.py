#!/usr/bin/env python3
# -*- coding: utf8 -*-

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs


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
    chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
    # chunk_iterator = dfs.get_chunk_data('130.211.64.240', 'shard_1_chunkaa')

    print("\nThe contents of chunk partitions:")
    for line in chunk_iterator:
        # удаляем символ перевода строки
        print(line[:-1])


def get_file_chunks(filename):
    # HACK:
    # for some reason some files were ranames from '/shard*' to 'shard*'
    # if filename.startswith('/shard'):
    #     filename = filename.lstrip('/')

    try:
        chunk_names = next(x for x in dfs.files() if x.name == filename).chunks
    except StopIteration:
        raise FileNotFoundError('File "%s" not found' % filename)
    return filter(lambda x: x.id in chunk_names, dfs.chunk_locations())


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
    chunks = get_file_chunks(filename)
    for chunk in chunks:
        data = dfs.get_chunk_data(chunk.chunkserver, chunk.id)
        for line in data:
            yield line.rstrip()


def demo2():
    temp = get_file_content('/shard1')
    for l in temp:
        print(l)


def binary_search_chunk(haystack, needle, start):
    left = start
    right = len(haystack)
    while left < right - 1:
        middle = (left + right) // 2
        key, value = haystack[middle].split()
        if key == needle:
            return value, middle + 1
        elif key < needle:
            left = middle
        else:
            right = middle
    key, value = haystack[left].split()
    if key == needle:
        return value, left + 1
    raise KeyError('Key "%s" not found' % needle)


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
        if line:
            begin, end, shard = line.split()

            if index >= total:
                break

            if keys[index] >= begin and keys[index] <= end:
                chunks = get_file_chunks(shard)
                for chunk in chunks:
                    data = list(x for x in
                                (y.rstrip() for y in
                                 dfs.get_chunk_data(chunk.chunkserver, chunk.id))
                                if x)
                    chunk_begin = data[0].split()[0]
                    chunk_end = data[-1].split()[0]
                    start = 0
                    while index < total and keys[index] >= chunk_begin and keys[index] <= chunk_end:
                        value, start = binary_search_chunk(data, keys[index], start)
                        # print('Debug: Got %s | %s' % (keys[index], value))
                        keys_sum += int(value)
                        index += 1

                    if index >= total or keys[index] > end:
                        break

    return keys_sum


def demo3():
    s = calculate_sum("/keys")
    print(s)


# demo()

# demo2()

demo3()
