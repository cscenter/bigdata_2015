#!/usr/bin/python
# encoding: utf8
from collections import namedtuple
import json
import os.path


def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())


def json2obj(data): return json.loads(data, object_hook=_json_object_hook)


def save_files(files):
    with open("data/files", "w") as f:
        f.write(json.JSONEncoder().encode(list({"name": f.name, "chunks": f.chunks} for f in files)))


def save_chunk_locations(chunk_locations):
    with open("data/chunk_locations", "w") as f:
        f.write(json.JSONEncoder().encode(list({"id": cl.id, "chunkserver": cl.chunkserver} for cl in chunk_locations)))


CHUNK_SERVER_COUNT = 4

# Получает от "мастера" список файлов и входящих в каждый файл фрагментов
# Возвращает список объектов с полями "name": String и "chunks": String[]
# "name" - это имя файла, "chunks" - список строковых идентификаторов 
# фрагментов в том порядке, в котором они следуют в файле
def files():
    with open("data/files") as f:
        return json2obj(f.read())


# Получает от "мастера" расположение фрагмента на файловых серверах.
# Так как репликация для нашей задачи несущественна, то файловый сервер
# у каждого фрагмента один.
# Возвращает список объектов с полями "id": String и "chunkserver": String
# где "id" - идентификатор фрагмента, "chunkserver" - идентификатор файлового сервера,
# на котором фрагмент хранится
def chunk_locations():
    with open("data/chunk_locations") as f:
        return json2obj(f.read())


FilesRecord = namedtuple('FilesRecord', ['name', 'chunks'])
ChunkLocationRecord = namedtuple('ChunkLocations', ['id', 'chunkserver'])


def create_file(filename):
    all_files = files()
    for f in all_files:
        if f.name == filename:
            raise Exception("File %s already exists" % filename)

    chunkserver = "cs%d" % (hash(filename) % CHUNK_SERVER_COUNT)

    chunk_id = "%s_chunk00" % filename.lstrip('/')
    chunk_filename = "data/%s/%s" % (chunkserver, chunk_id)
    if os.path.exists(chunk_filename):
        raise Exception("Chunk file %s already exists" % chunk_filename)
    all_files.append(FilesRecord(filename, [chunk_id]))

    clocs = chunk_locations()
    clocs.append(ChunkLocationRecord(chunk_id, chunkserver))

    if not os.path.exists("data/" + chunkserver):
        os.makedirs("data/" + chunkserver)

    save_files(all_files)
    save_chunk_locations(clocs)
    return chunk_id


def write_chunk(chunk_id, content):
    for cl in chunk_locations():
        if cl.id == chunk_id:
            with open("data/%s/%s" % (cl.chunkserver, chunk_id), "w") as f:
                f.write(content)
                return
    raise Exception("Chunk %s does not exist. Forgot to call create_file?" % chunk_id)


# Возвращает содержимое указанного фрагмента с указанного файлового сервера
# в виде потока
def get_chunk_data(chunk_server_id, chunk_id):
    return open("data/%s/%s" % (chunk_server_id, chunk_id))





