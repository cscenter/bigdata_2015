#!/usr/bin/python
# encoding: utf8
from collections import namedtuple
#Use this import if you're using Python3
<<<<<<< HEAD
#from urllib.request import urlopen
#Use this import if you're using Python2
from urllib2 import urlopen
=======
from urllib.request import urlopen
#Use this import if you're using Python2
#from urllib2 import urlopen
>>>>>>> master
import json

MASTER_URL = "bigdata-hw01.barashev.net"

def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())
def json2obj(data): return json.loads(data, object_hook=_json_object_hook)

def files():
  resp = urlopen(url = "http://%s/files" % MASTER_URL, timeout=10)
  if resp.getcode() != 200:
    raise Exception("ERROR: can't get files from master")
  return json2obj(resp.read().decode(encoding='UTF-8'))

def chunk_locations():
  resp = urlopen(url = "http://%s/chunk_locations" % MASTER_URL, timeout=10)
  if resp.getcode() != 200:
    raise Exception("ERROR: can't get chunk locations from master")
  return json2obj(resp.read().decode(encoding='UTF-8'))

def get_chunk_data(chunk_server_id, chunk_id):
  resp = urlopen(url="http://%s/chunks/%s" % (chunk_server_id, chunk_id), timeout=10)
  if resp.getcode() != 200:
    raise Exception("ERROR: can't get chunk %s from chunkserver %s" % (chunk_id, chunk_server_id))
  for line in resp:
    yield line.decode(encoding='UTF-8')
