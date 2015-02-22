#!/usr/bin/python
# encoding: utf8

import http_dfs as dfs

list_name = []
shard_range = {}
shard_chunks = {}
chunk_server = {}

def calculate_sum(keys_filename):
    load()
    mapping_range()
    result = 0
    key_path = shard_chunks[keys_filename][0]
    key_server = chunk_server[key_path]
    file = dfs.get_chunk_data(key_server, key_path)
    for line in file:
        if line != "":
            result += count_visits(line)
    print(result)


def mapping_range():
    partitions_path = shard_chunks["partitions"][0]
    partitions_server = chunk_server[partitions_path]
    chunk_iterator = dfs.get_chunk_data(partitions_server, partitions_path)

    for line in chunk_iterator:
        if line != "\n":
            words = line[:-1].split()
            name = words[2][1:]

            if name in shard_range:
                shard_range[name].append([words[0], words[1]])
            else:
                shard_range[name] = []
                shard_range[name].append([words[0], words[1]])
        line = ""


def count_visits(key):
    for k in shard_range.keys():
        i = -1
        for value in shard_range[k]:
            i += 1
            flag = 0
            if (key >= value[0]) and (key <= value[1]):
                shard_name = k
                index = i
                flag += 1
                break
            if flag == 1:
                break
    chunk_name = shard_chunks[shard_name][index]
    server = chunk_server[chunk_name]
    iterator = dfs.get_chunk_data(server, chunk_name)
    for line in iterator:
        line_array = line[:-1].split(' ')
        if key[:-1] == line_array[0]:
            return int(line_array[1])


def load():
    for f in dfs.files():
        shard_chunks[f.name[1:]] = f.chunks
    for c in dfs.chunk_locations():
        chunk_server[c.id] = c.chunkserver

calculate_sum("keys")
