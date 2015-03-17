import base64
import sys

sys.path.append("../dfs/")
import client as dfs

def encode_term(term):
  return base64.b64encode(term, "_-")

def decode_term(encoded):
  return base64.b64decode(encoded, "_-")

documents_count = None

def get_documents_count():
  global documents_count

  if documents_count is None:
    documents_count = len(map(lambda x: x, dfs.get_file_content("/wikipedia/__toc__")))
  return documents_count