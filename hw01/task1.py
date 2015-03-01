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

files = dfs.files()
shards = dfs.chunk_locations()


def get_server_id_for_file(filename):
    for entry in shards:
        if entry.id == filename:
            return entry.chunkserver


def get_file_content(filename):
    for entry in files:
        for f in entry.chunks:
            if filename == f:
                for line in dfs.get_chunk_data(get_server_id_for_file(filename), filename):
                    yield line


def get_chunks(shard):
    for entry in files:
        if shard == entry.name:
            return entry.chunks


def get_chunk_with_key(shard, key):
    chunks = get_chunks(shard)

    #Here we collect a list of iterators for all chunks
    chunks_iterators = [get_file_content(chunk) for chunk in chunks]
    chunks_beginnings = []

    #And then collect all first keys for elements in chunks, avoiding empty lines in a clumsy defensive way.
    for iterator in chunks_iterators:
        first_item = ''
        while first_item == '':
            first_item = next(iterator)
        chunks_beginnings.append(first_item.split(" ")[0])

    #Getting index for the chunk which contains the key, according to ordering in the shard.
    chunk_index = -1
    chunks_size = len(chunks_beginnings)
    for i in range(chunks_size):
        #Handles the case of one chunk or case when key is inside the last chunk.
        if (i + 1 == chunks_size) and chunks_beginnings[i] <= key:
            break

        #Key is in the previous chunk if next chunk's first element is greater
        elif chunks_beginnings[i] > key:
            chunk_index = i - 1
            break

            #Return new iterator over the chunk's lines
    return get_file_content(chunks[chunk_index])


def find_value_for_key(key):
    value = None
    for line in get_file_content("partitions"):
        line = line.strip('\n')
        if line != '':
            key_left, key_right, shard = line.split(" ")

            #Using the information about ordering, choosing the shard
            if (key_left <= key) and (key_right >= key):
                chunk = get_chunk_with_key(shard, key)
                for entry in chunk:

                    #Just scanning the chunk's lines searching for entry.
                    if entry != "":
                        entry_items = entry.rstrip('\n').split(" ")
                        if entry_items[0] == key:
                            value = entry_items[1]
                            break
    if not value:
        raise Exception("Value for key {} has not been found".format(key))
    return value


def calculate_sum(keys_filename):
    keys_file = get_file_content(keys_filename)
    resulting_sum = 0
    for key in keys_file:
        resulting_sum += int(find_value_for_key(key.rstrip('\n')))
    return resulting_sum


print(calculate_sum("keys"))