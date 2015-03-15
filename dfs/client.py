from collections import namedtuple
#Use this import if you're using Python3
import urllib2
from urllib2 import urlopen
#Use this import if you're using Python2
#from urllib2 import urlopen
import json
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
from contextlib import closing
import argparse

register_openers()

#MASTER_URL = "bigdata-hw01.barashev.net"
MASTER_URL = "localhost:8000"
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
  resp = urlopen(url="http://%s/read?id=%s" % (chunk_server_id, chunk_id), timeout=10)
  if resp.getcode() != 200:
    raise Exception("ERROR: can't get chunk %s from chunkserver %s" % (chunk_id, chunk_server_id))
  for line in resp:
    yield line.decode(encoding='UTF-8')

def get_file_content(filename):
  chunks = []
  for f in files():
    if f.name == filename:
      chunks = f.chunks
  if len(chunks) == 0:
    return
  clocs = {}
  for c in chunk_locations():
    clocs[c.id] = c.chunkserver

  for chunk in chunks:
    try:
      loc = clocs[chunk]
      if loc == "":
        raise "ERROR: location of chunk %s is unknown" % chunk
      for l in get_chunk_data(loc, chunk):
        yield l.rstrip()

    except StopIteration:
      pass

def create_chunk(filename):
  resp = urlopen(url = "http://%s/new_chunk?f=%s" % (MASTER_URL, filename))
  if resp.getcode() != 200:
    raise Exception("ERROR: can't create new chunk of file=%s" % filename)
  return resp.read().split(" ")

def write_chunk_data(chunk_server_id, chunk_id, data):
  datagen, headers = multipart_encode({"data": data, "chunk_id": chunk_id})
  request = urllib2.Request("http://%s/write" % chunk_server_id, datagen, headers)
  response = urllib2.urlopen(request)
  if response.getcode() != 200:
      raise Exception("ERROR: can't write chunk %s to chunkserver %s" % (chunk_id, chunk_server_id))

def file_appender(filename):
  return closing(FileAppend(filename))

class FileAppend:
  def __init__(self, filename):
    self.filename = filename
    self.lines = []

  def write(self, line):
    self.lines.append(line)

  def close(self):
    chunkserver, chunk_id = create_chunk(self.filename)
    write_chunk_data(chunkserver, chunk_id, "\n".join(self.lines))

class CachedMetadata:
  def __init__(self):
    self.file_chunks = {}
    for f in files():
      self.file_chunks[f.name] = f.chunks
    self.chunk_locations = {}
    for cl in chunk_locations():
      self.chunk_locations[cl.id] = cl.chunkserver

  def get_file_content(self, filename):
    for chunk_id in self.file_chunks[filename]:
      for l in get_chunk_data(self.chunk_locations[chunk_id], chunk_id):
        yield l

def put_file(from_file, to_file, master):
  global MASTER_URL
  MASTER_URL=master
  with open(from_file) as f, file_appender(to_file) as buf:
    for l in f:
      buf.write(l.rstrip())

def get_file(from_file, master):
  global MASTER_URL
  MASTER_URL=master
  for l in get_file_content(from_file):
    print l

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--command", required = True)
  parser.add_argument("--f")
  parser.add_argument("--t")
  parser.add_argument("--master", required=True, default="localhost:8000")
  args = parser.parse_args()

  if "put" == args.command:
    put_file(args.f, args.t, args.master)
  elif "get" == args.command:
    get_file(args.f, args.master)  
