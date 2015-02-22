#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
#import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

import collections


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
    return dfs.get_chunk_data(get_file_location(filename), filename)


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    if not keys_filename:
        return 0

    summary = 0

    chuck_to_keys = get_chuck_to_keys_mapping(get_keys(keys_filename))
    for chuck_id, keys in chuck_to_keys.items():
        summary += calculate_for_one_chunk(chuck_id, keys)

    return summary


def get_file_location(filename):
    """
    This function may be cached as map (filename -> location)
    if files positions are unchangeable but i'm not sure whether they are or not

    :param filename: the name of file to search
    :return: server identifier
    """
    for c in dfs.chunk_locations():
        if filename == c.id:
            return c.chunkserver

    return None


def get_chunk_parts(chunk_id):
    """
    :param chunk_id: chunk identifier
    :return: chunk parts
    """
    for f in dfs.files():
        if f.name == chunk_id:
            return f.chunks

    return None


def calculate_for_one_chunk(chuck_id, keys):
    """
    Calculates pages data contained in one chunk

    :param chuck_id: chunk identifier
    :param keys: keys which could be found in that chunk
    :return: total impression summary for current chunk
    """
    summary = 0

    for part_id in get_chunk_parts(chuck_id):
        for line in get_file_content(part_id):
            data = line[:-1].split(" ")
            if len(data) != 2:
                continue

            for key in keys:
                if key == data[0]:
                    summary += int(data[1])

    return summary


def get_chuck_to_keys_mapping(keys):
    """
    :param keys: all known keys
    :return: map chunk -> keys array from that chunk
    """
    chuck_to_key = collections.defaultdict(lambda: [])

    # Comment said that 'partitions' file could have the other name.
    # But there is no way how we could get know it.
    for line in get_file_content("partitions"):
        data = line.split(" ")
        if len(data) != 3:
            continue

        for key in keys:
            if data[0] <= key <= data[1]:
                # Issue (data[2][1:-1]) found in "production"
                # We do not need to cut first '/' symbol in test client. But for http_dfs we need so.
                chuck_to_key[data[2][1:-1]].append(key)

    return chuck_to_key


def get_keys(keys_filename):
    """
    :param keys_filename: filename which contain keys
    :return: all keys as array
    """
    keys = []
    for key in get_file_content(keys_filename):
        keys.append(key[:-1])

    return keys


print(calculate_sum("keys"))
#demo()
