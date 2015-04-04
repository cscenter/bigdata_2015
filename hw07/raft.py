#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen
from string import strip

class RaftHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.send_header('Content-type','text/plain')
    self.end_headers()
    # Send the html message
    self.wfile.write("Hello World !")
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

start_server(args.p)
if args.f:
  print "Leader is starting. Trying to contact followers..."
  follower_ports = [int(f.strip()) for f in args.f.split(",")]
  for port in follower_ports:
    resp = urlopen(url = "http://localhost:%d" % port)
    if resp.getcode() != 200:
      raise Exception("ERROR: can't get response from follower on port %d" % port)
    print "Follower on port %d is ok:" % port
    print resp.read().decode(encoding='UTF-8')
else:
  print "Started follower"
while True:
    time.sleep(1)