#!/usr/bin/python
# encoding: utf8
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import threading
import time
import argparse
import requests
import cgi
from urllib2 import urlopen
from string import strip
import urlparse

#просто на те же адреса что и в примере шлём post и get запросы, т.к. их функционала нам достаточно для реализации репликации
#на get запрос ведомый возвращает свой журнал
#в post запросе передаём поколение и значение реплицируемой записи, а так же поколение и индекс предыдущей записи, вернётся добавил ("added") или нет ("deniend) ведомый к себе эту запись

journal = []

class RaftHandler(BaseHTTPRequestHandler):
#на запрос выдаёт свой журнал
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()

        journal_in_str = ""
        for rec in journal:
            journal_in_str += str(rec[0]) + ':' + str(rec[1]) + ','
        journal_in_str = journal_in_str[0:len(journal_in_str) - 1]
        self.wfile.write(journal_in_str)

        return
#на запрос пытается добавить запись в журнал, если не выходит, отсылает "denied", если успешно то "added"
    def do_POST(self):
        global journal

        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })

        self.send_response(200)
        self.end_headers()

        repl_gen = form['@replicate_genetation'].value
        repl_val = form['@replicate_value'].value
        prev_gen = form['@prev_rec_gen'].value
        prev_ind = int(form['@prev_rec_ind'].value)


        if (prev_ind >= len(journal)) or (prev_ind == -1) or (journal[prev_ind][0] != prev_gen):
            self.wfile.write("denied")
        else:
            journal = journal[0:prev_ind + 1]
            journal.append((repl_gen, repl_val))
            self.wfile.write("added")
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

if args.l:
    for record in args.l.split(","):
        (gen, val) = record.split(":")
        journal.append((gen, val))

start_server(args.p)

def try_add(port, repl_rec, prev_gen, prev_ind):
    r = requests.post("http://localhost:%d" % port,
                              data={'@replicate_genetation': repl_rec[0],
                                    '@replicate_value': repl_rec[1],
                                    '@prev_rec_gen': prev_gen,
                                    '@prev_rec_ind': prev_ind})
    if r.status_code != 200:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    return r.text

def replicate_journal_to_follower(journal, port):
    replicated = False
    currInd = len(journal)
    while (not replicated):
        currInd -= 1
        if currInd > 0:
            response = try_add(port, journal[currInd], journal[currInd - 1][0], currInd - 1)
        else:
            response = try_add(port, journal[currInd], 0, -1)

        if ((response == "added") and (currInd == len(journal) - 1)):
            replicated = True
            return
        if (response == "added"):
            currInd += 2

def get_journal_from_follower(port):
    r = requests.get("http://localhost:%d" % port)
    if r.status_code != 200:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    return r.text.decode(encoding='UTF-8')

if args.f:
  print "Leader is starting. Log=%s" % str(log)
  follower_ports = [int(f.strip()) for f in args.f.split(",")]
  #реплицируем журнал всем ведомым
  print "Replication started..."
  for port in follower_ports:
    replicate_journal_to_follower(journal, port)
  print "Replication ended"
  #запрашиваем журналы у всех ведомых
  print "Getting journals..."
  for port in follower_ports:
      print(port, get_journal_from_follower(port))
  print "All journals getted"
else:
  print "Started follower"

while True:
    time.sleep(1)
