#!/usr/bin/python
# encoding: utf8
from collections import namedtuple
from urllib.request import urlopen
import json

MASTER_URL = "bigdata-hw01.barashev.net"

def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())
def json2obj(data): return json.loads(data, object_hook=_json_object_hook)

def files():
  resp = urlopen("http://%s/files" % MASTER_URL)
  if resp.status != 200:
    raise Exception("ERROR: can't get files from master")
  return json2obj(resp.read().decode(encoding='UTF-8'))

def chunk_locations():
  resp = urlopen("http://%s/chunk_locations" % MASTER_URL)
  if resp.status != 200:
    raise Exception("ERROR: can't get chunk locations from master")
  return json2obj(resp.read().decode(encoding='UTF-8'))

def get_chunk_data(chunk_server_id, chunk_id):
  resp = urlopen("http://%s/chunks/%s" % (chunk_server_id, chunk_id))
  if resp.status != 200:
    raise Exception("ERROR: can't get chunk %s from chunkserver %s" % (chunk_id, chunk_server_id))
  for line in resp:
    yield line.decode(encoding='UTF-8')




