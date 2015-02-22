from collections import namedtuple
from urllib.request import urlopen
__author__ = 'Flok'

import http_dfs as dfs

Part = namedtuple('Part', ['start', 'finish', 'name'])
MASTER_URL = "bigdata-hw01.barashev.net"
def get_file_content(file):
    return open(file)

#получает чанки, соответствующие одному файлу (одному шарду)
def get_file_chunks(file_name):
    for f in dfs.files():
        if(f.name == file_name):
            return f.chunks
    raise ValueError("no such file in list")

#получает сервер, на котором хранится чанк
def get_chunk_server(chunk_id):
    for c in dfs.chunk_locations():
        if(c.id == chunk_id):
            return c.chunkserver
    raise ValueError("no such chunk in list")

#читает partitions и хранит его структуру
def get_partitions():
    partitions_list = []
    chunk_id = get_file_chunks("/partitions")[0]
    chunk_server_id = get_chunk_server(chunk_id)

    partitions = dfs.get_chunk_data(chunk_server_id, chunk_id)
    for p in partitions:
        if p != '\n':
            partitions_list.append(Part(*p.split()))
    return partitions_list

#получает список ключей из /keys
def get_keys(keys_filename):
    keys_list = []
    chunk_id = get_file_chunks(keys_filename)[0]
    chunk_server_id = get_chunk_server(chunk_id)
    key_file = dfs.get_chunk_data(chunk_server_id, chunk_id)
    for k in key_file:
        try:
            keys_list.append(k[:-1])
        except:
            pass
    return keys_list

def get_key_num(key, partitions):
    #ищем шард
    shard_name = None
    for p in partitions:
        if p.start <= key <= p.finish:
            shard_name = p.name
            break
    if shard_name is None:
        raise ValueError("no such keyrange for given key")
    #ищем чанки
    chunks = get_file_chunks(shard_name)
    #читаем информацию из чанков
    for chunk_id in chunks:
        chunk_server_id = get_chunk_server(chunk_id)
        fc = dfs.get_chunk_data(chunk_server_id, chunk_id)
        for line in fc:
            try:
                current_key, value = line.split()
                if current_key == key:
                    return int(value)
            except:
                pass

    raise ValueError("no such key in storage")

def calculate_sum(keys_filename):
    partitions = get_partitions()
    keys = get_keys(keys_filename)
    acc_summ = 0
    for key in keys:
        acc_summ += get_key_num(key, partitions)
    return acc_summ

print(calculate_sum("/keys"))
