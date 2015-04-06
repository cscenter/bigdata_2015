#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import time
import threading
import argparse
from urllib2 import urlopen

log = []

class RaftHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        if self.path == '/?get_journal':
            self.wfile.write(log)
        else:
            raise Exception("ERROR: unknown command")
        
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()  
 

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

log = args.l
start_server(args.p)
if args.f:
  print "Leader is starting. Trying to contact followers..."
  follower_ports = [int(f.strip()) for f in args.f.split(",")]
  for port in follower_ports:
    resp = urlopen(url = "http://localhost:%d?get_journal" % port)
    if resp.getcode() != 200:
      raise Exception("ERROR: can't get response from follower on port %d" % port)
    print "Follower on port %d is ok:" % port
    print resp.read().decode(encoding='UTF-8')
else:
  print "Started follower"
while True:
    time.sleep(1)
    
'''
python raft.py -p 8001 -t 4 -l 1:A,2:E,2:G,3:M,4:F,4:C
python raft.py -p 8002 -t 3 -l 1:A,2:E,2:G,3:M
python raft.py -p 8003 -t 2 -l 1:A,2:E
python raft.py -p 8004 -t 6 -l 1:A,2:E,2:G,3:M,4:F,4:C,4:Z,5:X,5:W,6:U
python raft.py -p 8005 -t 5 -l 1:A,2:E,2:G,3:M,4:F,4:C,4:Z,5:X,5:W
python raft.py -p 8000 -t 6 -l 1:A,2:E,2:G,3:M,4:F,4:C,4:Z,5:X,5:W,6:U -f 8001,8002,8003,8004,8005

'''    