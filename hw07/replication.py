#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import threading
import time
import argparse
import requests
from urllib2 import urlopen
from urlparse import urlparse
from string import strip

# Message format:
# "GET /?ncommand=L&pindex=2&pterm=1&nterm=2 HTTP/1.1"
# pindex  - preceding entry index
# pterm   - preceding entry term
# nindex  - current(new) entry index
# nterm   - current(new) entry term

# Possible resp
# 200     - mean new entry save at Follower journal
# 404     - mean Follower entry at pindex not match

# "GET /?print=yes HTTP/1.1"
# 1:H,1:E,1:L,2:L,3:O

class RaftJournal():
  def __init__(self, start_log):
    self.data = []
    index = 0
    for pair in args.l.split(","):
      term, command = pair.split(":")
      self.data.append((index, int(term), command))
      index += 1

  def __str__(self):
    return ",".join(["{1}:{2}".format(a[0], a[1], a[2]) for a in self.data])

  def processRPC(self, query):
    pindex = int(query["pindex"])
    if pindex > len(self.data) - 1:
      return False
    pterm = int(query["pterm"])
    _, term, _ = self.data[pindex]
    if term == pterm:
      self.data = self.data[:pindex + 1]
      self.data.append((pindex + 1, int(query["nterm"]), query["ncommand"]))
      return True
    else:
      return False

  def getEntry(self, index):
    return self.data[index]


class RaftHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    query = urlparse(self.path).query
    query_components = dict(qc.split("=") for qc in query.split("&"))
    if "print" in query_components:
      print journal
      self.send_response(200)
      self.send_header('Content-type','text/plain')
      self.end_headers()
      return
    if journal.processRPC(query_components):
      self.send_response(200)
      self.send_header('Content-type','text/plain')
      self.end_headers()
      # Send the html message
      self.wfile.write("Updated from {}".format(query_components))
    else:
      self.send_error(404)
    return


def start_server(port):
  server = HTTPServer(('', port), RaftHandler)
  print 'Started httpserver on port %d' % port
  t = threading.Thread(target=server.serve_forever)
  t.setDaemon(True)
  t.start()
  return t

parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required = True, type = int)
parser.add_argument("-l", help="Existing log", required = True)
parser.add_argument("-t", help="Term number", required = True, type = int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

journal = RaftJournal(args.l)

start_server(args.p)

if args.f:
  print "Leader is starting. Trying to contact followers..."
  follower_ports = [int(f.strip()) for f in args.f.split(",")]
  for port in follower_ports:
    index = len(journal.data) - 1
    url = "http://localhost:%d" % port
    while index != len(journal.data):
      if index < 1:
        raise Exception("Follower on port %d don't have common journal prefix." % port)
      i, t, c = journal.getEntry(index)
      pi, pt, pc = journal.getEntry(index - 1)
      payload = {'pindex': pi, 'pterm': pt, 'nterm': t, 'ncommand': c}
      resp = requests.get(url=url, params=payload)
      if resp.status_code == 200:
        index += 1
        continue
      if resp.status_code == 404:
        index -= 1
      else:
        raise Exception("Unexpected response code {}".format(resp.status_code))
    resp = requests.get(url=url, params={'print': 'yes'})
    if resp.status_code == 200:
      print "Follower on port %d has been replicated" % port
  print journal
else:
  print "Started follower"
while True:
    time.sleep(1)