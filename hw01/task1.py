#!/usr/bin/python
# encoding: utf8
import http_dfs as dfs

#  Функция принимает имя файла и
# возвращает итератор по его строкам.
def get_file_content(filename):
    f = dfs.urlopen(url = filename, timeout = 10)
    for line in f:
        yield line[:-1]
    f.close()
#эта функция возвращает итератор на все нужные шарды
def get_chunks_of_shard(shard):
    for i in list_files:
        if i.name == shard:
            list_chunks = i.chunks
            break
    for j in list_chunks:
        yield j
#находит частоту ключа во фрагменте
def find_freq_of_key_in_chunk(chunk, key):
    for i in list_chunk_locations:
        if i.id == chunk:
            serv = i.chunkserver
            break
    strings = dfs.get_chunk_data(serv, chunk)
    res = -1
    for j in strings:
        pair = j.split(" ")
        if pair[0] == key:
            res = int(pair[1])
            break
    return res

#исходя из отсортированности ключей в шарде и его фрагментах, нахожу фрагмент, где
#будет лежать нужный мне ключ
def find_best_chunk(key, chunks_of_shard):
    good_chunk_string = ''
    for chunk in chunks_of_shard:
        for l in list_chunk_locations:
            if l.id == chunk:
                serv = l.chunkserver
                break
        for s in dfs.get_chunk_data(serv, chunk):
            if s == '/n':
                continue
            if s <= key and s > good_chunk_string:
                good_chunk = chunk
                good_chunk_string = s
            break
    return good_chunk


# Эта функция принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum = 0
    keys = get_file_content(keys_filename)
    for i in keys:
        for j in get_file_content("http://%s/chunks/%s" % (partitions_id_server, "partitions")):
            if j == '':
                continue
            interval_and_shard = j.split(" ")
            if i>=interval_and_shard[0] and i<=interval_and_shard[1]:
                chunks_of_shard = get_chunks_of_shard(interval_and_shard[2])
                #здесь, исходя из отсортированности ключей в шарде и его фрагментах, нахожу фрагмент, где
                #будет лежать нужный мне ключ и сервер, где он лежит
                good_chunk = find_best_chunk(i, chunks_of_shard)
                freq = find_freq_of_key_in_chunk(good_chunk, i)
                sum = sum + freq
                break
    return sum
#строки из files
list_files = dfs.files()
#строки из chunk_locations()
list_chunk_locations = dfs.chunk_locations()
#ищу местоположение файла "partitions"
for i in list_chunk_locations:
    if i.id == "partitions":
        partitions_id_server = i.chunkserver
        break
#ищу местоположение "keys"
for i in list_chunk_locations:
    if i.id == "keys":
        keys_id_server = i.chunkserver
        break
print calculate_sum("http://%s/chunks/%s" % (keys_id_server, "keys"))