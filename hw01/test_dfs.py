#!/usr/bin/python
# encoding: utf8
from collections import namedtuple
import json


def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())


def json2obj(data): return json.loads(data, object_hook=_json_object_hook)


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
# где "id" - идентификатор фрагмента,
# "chunkserver" - идентификатор файлового сервера,
# на котором фрагмент хранится
def chunk_locations():
    with open("data/chunk_locations") as f:
        return json2obj(f.read())


# Возвращает содержимое указанного фрагмента с указанного файлового сервера
# в виде потока
def get_chunk_data(chunk_server_id, chunk_id):
    return open("data/%s/%s" % (chunk_server_id, chunk_id))
