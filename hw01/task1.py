#!/usr/bin/python
# encoding: utf8

import time

# Для быстрого локального тестирования используйте модуль test_dfs
#import test_dfs as dfs
#mode = "local"

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs
mode = "http"

def find_file_by_name(filename):
    for f in dfs.files():
        if f.name == filename:
            return f
    raise Exception("File not found!")

def get_file_content(filename, shared=True):
    file = find_file_by_name(filename)
    for chunk_name in file.chunks:
        for c in dfs.chunk_locations():
            if c.id == chunk_name:
                for line in dfs.get_chunk_data(c.chunkserver, chunk_name):
                    yield line

def get_shard_by_key(key, partitions):
    for line in partitions:
        if len(line[:-1]) > 0:
            temp = line[:-1].split(' ')
            if len(temp) != 3:
                raise Exception("Invalid input format")
            L, R, shard = temp
            if (L <= key) & (key <= R):
                return shard
    raise Exception("Can't find shard for key {0}!".format(key))

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    result_sum = 0
    for key in get_file_content(keys_filename, shared=False):
        key = key[:-1]
        shard = get_shard_by_key(key, get_file_content("/partitions"))
        if mode != "local":
            shard = shard[1:]       # убираем слеш
        for line in get_file_content(shard):
            if len(line) > 0:
                k = line.split(' ')[0]
                if k == key:
                    result_sum += int(line.split(' ')[1])
                    break

    return result_sum


print(calculate_sum("/keys"))
