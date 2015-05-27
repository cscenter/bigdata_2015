#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen
from string import strip
import urlparse

class RaftHandler(BaseHTTPRequestHandler):
  def __init__(self, term, log, *args):
  	self.term = term
  	self.log = log
  	BaseHTTPRequestHandler.__init__(self, *args)

  def do_GET(self):
    self.send_response(200)
    self.send_header('Content-type','text/plain')
    self.end_headers()

    request = urlparse.urlparse(self.path)
    qs = urlparse.parse_qs(request.query)

    if request.path == "/replicate":
      self.process_replicate(qs)
      return
    if request.path == "/print":
      print str(self.log)      
      return

  def process_replicate(self, qs):
    prev_idx = int(qs["prev_idx"][0])
    prev_term = int(qs["prev_term"][0])
    if prev_idx >= len(self.log):
      self.wfile.write("NACK")
      return

    def report_ack():
      self.wfile.write("ACK")
      del(self.log[prev_idx + 1 :])
      cur_term = int(qs["cur_term"][0])  	
      cur_value = qs["cur_value"][0]
      self.log.append((cur_term, cur_value))

    if len(self.log) == 0:
      if prev_idx == -1:
      	report_ack()
        return
      self.wfile.write("NACK")     
      return

    if self.log[prev_idx][0] == prev_term:
      report_ack()
    else:
      self.wfile.write("NACK")
    return

def handleRequestsUsing(term, log):
	return lambda *args: RaftHandler(term, log, *args)

def find_baseline(term, log, port):
  for i in reversed(xrange(len(log))):
    resp = urlopen(url = "http://localhost:%d/replicate?cur_term=%d&cur_value=%s&prev_idx=%d&prev_term=%d" % (port, log[i][0], log[i][1], i - 1, log[i-1][0]))
    if resp.getcode() != 200:
      raise Exception("ERROR: can't get response from follower on port %d" % port)
    answer = resp.read().decode(encoding='UTF-8')
    if "ACK" == answer:
      return i
  return -1

def append_tail(start_idx, log, follower_port):
  i = start_idx
  while i < len(log):
    resp = urlopen(url = "http://localhost:%d/replicate?cur_term=%d&cur_value=%s&prev_idx=%d&prev_term=%d" % (follower_port, log[i][0], log[i][1], i - 1, log[i-1][0]))
    if resp.getcode() != 200:
      raise Exception("ERROR: can't get response from follower on port %d" % follower_port)
    answer = resp.read().decode(encoding='UTF-8')
    if "NACK" == answer:
      raise Exception("ERROR: when appending a log at index %d tail follower returned NACK. What's up?"	% i)
    i += 1 

def replicate_log(term, log, follower_ports):
  for port in follower_ports:
    append_tail(1 + find_baseline(term, log, port), log, port)
    urlopen(url = "http://localhost:%d/print" % port)
  
def start_follower(term, log, port):
  server = HTTPServer(('', port), handleRequestsUsing(term, log))
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

if len(args.l) == 0:
  log = []
else:
  log = [(int(le[0]), le[1]) for le in [log_entry.split(":") for log_entry in args.l.split(",")]]

if args.f:
  print "Leader is starting. Log=%s" % str(log)
  follower_ports = [int(f.strip()) for f in args.f.split(",")]
  replicate_log(args.t, log, follower_ports)
else:
  start_follower(args.t, log, args.p)
  print "Started follower with log=%s" % str(log)
  while True:
    time.sleep(1)