#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
# from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import argparse
<<<<<<< HEAD
import cgi
import requests
import logging

logging.basicConfig(level=logging.INFO)

#todo:rewrite with unit tests, it is very hardto maintain:(

class RaftHandler(BaseHTTPRequestHandler):
    # === utils
    def parse_params(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'multipart/form-data':
            params = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            params = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
            for k in params.keys():
                params[k] = (params[k])[0]
        else:
            params = {}
        return params

    def status(self, status):
        self.send_response(status)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        return

    # === haldlers
    # returs journal
    # Example: curl localhost:8001
    def do_GET(self):
        self.status(200)
        self.wfile.write(",".join([j[0] + ":" + j[1] for j in journal]))
        return

    # adds new record
    # curl --data "curr_generation=2&curr_commit_index=3:A&new=2:B" localhost:8001
    # returns 201 code if new item created
    def do_POST(self):
        global journal

        params = self.parse_params()
        logging.info(params)

        new_gen, new_v = params['@new'].split(":")
        curr_gen = params['@curr_generation']
        curr_commit_index = int(params['@curr_commit_index'])

        out_of_border = curr_commit_index < 0 or curr_commit_index >= len(journal)
        hasNeverSynced = curr_commit_index == -1 and int(curr_gen) == 0 #and len(journal) == 0
        if (not out_of_border and journal[curr_commit_index][0] == curr_gen) or hasNeverSynced:
            if not out_of_border:
                logging.info("accepted")
                journal = journal[0:curr_commit_index + 1]
            else:
                journal = []
            journal.append((new_gen, new_v))
            self.status(201)
        else:
            logging.info("rejected %r" % hasNeverSynced)
            self.status(200)
        return


def send_entry(url, new_entry, curr_gen, curr_commit_index):
    values = {'@new': new_entry[0] + ":" + new_entry[1],
              '@curr_generation': curr_gen,
              '@curr_commit_index': curr_commit_index}
    r = requests.post(url, values)
    if not (r.status_code == 200 or r.status_code == 201):
        raise Exception("")
    return r.status_code == 201


def replicate_follower(url):
    logging.info("Replicating %s" % url)
    commit_index = len(journal)
    while True:
        commit_index -= 1
        if commit_index > 0:
            prev_commit_index = commit_index - 1
            accepted = send_entry(url, journal[commit_index], journal[prev_commit_index][0], prev_commit_index)
        else:
            #nothing synscronized, nothing committed
            accepted = send_entry(url, journal[0], 0, -1)

        hasCheckedInEverything = commit_index == len(journal) - 1
        if accepted and hasCheckedInEverything:
            return
        if accepted:
            commit_index += 2


def replicate(followers):
    for follower in followers:
        replicate_follower(follower)
    logging.info("Done")


def verify_replication(followers):
    for url in followers:
        res = requests.get(url)
        if res.status_code != 200:
            raise Exception("FAILED")
        logging.info("%s %s" % (url, res.text))


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    logging.info('Started httpserver on port %d' % port)
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t

=======
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
>>>>>>> master

parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required=True, type=int)
parser.add_argument("-l", help="Existing log")
parser.add_argument("-t", help="Term number", required=True, type=int)
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
>>>>>>> master
    time.sleep(1)
