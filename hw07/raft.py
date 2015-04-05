#!/usr/bin/python
#encoding: UTF-8
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen
from string import strip
import urlparse
import ast

journal = []
generation = None
is_main = False

class RaftHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """
        все узлы принимают запрос /journal
        ответом является список имеющихся в журнале записей.

        фолловер принимает запрос /add со следующими параметрами:
            old - предыдущая известная запись в журнале
            new - новая запись в журнале
        параметры кодируются в виде пары поколение:запись
        пример: /add?new=2:E&old=1:H
        ответы:
            ok - если запись успешно добавлена
            -1 в первой строке - если фолловер отказывается добавлять. во второй строке будет человеческое разъяснение причин.

        """

        parsed_url = urlparse.urlparse(self.path)
        params = urlparse.parse_qs(parsed_url.query)

        if any(len(vals) > 1 for vals in params.values()): # запрещаем передавать значение-массив
            self.error()
            return
        params = {k:v[0] for k,v in params.items()}
        # Send the html message
        if parsed_url.path == "/journal":
            self.show_journal()
        elif parsed_url.path == "/add":
            if is_main:
                self.error("unsupported operation for leader")
                return
            self.try_to_add(parsed_url.path, params)
        return

    def normal_headers(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()

    def error(self, msg="wrong parameters"):
        self.send_response(500)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write(msg)

    def show_journal(self):
        self.normal_headers()
        self.wfile.write(journal)

    def try_to_add(self, path, params):
        global journal
        if "old" not in params:
            self.error("'old' value needed")
            return
        if "new" not in params:
            self.error("'new' value needed")
            return
        params['new'] = self.decode_row(params['new'])
        params['old'] = self.decode_row(params['old'])

        if params['old'][0] == 0: # вставка самой первой записи в журнал
            journal = [params["new"]]
            self.normal_headers()
            self.wfile.write("ok")
            return
        if params['old'] in journal: # вставка записи в нужное место с удалением из журнала последующих записей
            idx = journal.index(params['old'])
            journal = journal[:idx + 1]
            journal.append(params['new'])
            self.normal_headers()
            self.wfile.write("ok")
        else: # мы не можем принять запись, о чём и сообщаем
            self.normal_headers()
            self.wfile.write("-1\n")
            self.wfile.write("unknown predecessor")

    @staticmethod
    def encode_row(row):
        return "%d:%s" % row

    @staticmethod
    def decode_row(row):
        row = tuple(x for x in row.split(':'))
        row = (int(row[0]), row[1])
        return row

def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print 'Started httpserver on port %d' % port
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t

def replicate_to_follower(port):
    global generation
    if not journal or journal[-1][0] != generation: # в нашем поколении не было утверждённых записей
                                                    # => все предыдущие записи считаем неутверждёнными
        return
    complete = False
    total_len = len(journal)
    curr_idx = -1
    prev_idx = -2
    while not complete: # выполняем, пока хвост журнала рассинхронизирован
        curr_val = journal[curr_idx]
        if abs(prev_idx) > total_len: # рассинхронизирован весь журнал. вместо "минус первой" записи обозначение (0, 0)
            prev_val = (0,0)
        else:
            prev_val = journal[prev_idx]
        resp = None
        try:
            resp = urlopen(url = "http://localhost:%d/add?old=%s&new=%s" %
                                 (port, RaftHandler.encode_row(prev_val), RaftHandler.encode_row(curr_val)))
        except:
            raise Exception("ERROR: can't get response from follower on port %d" % port)

        if resp.getcode() != 200:
            print("Unable to get response from follower on port %d" % port)
            return

        ans = resp.readline().decode(encoding="UTF-8").rstrip()
        if  ans == "-1": # если вставка не удалась, переходим к предыдущей записи
            curr_idx -= 1
            prev_idx -= 1
        elif ans == "ok": # если вставка удалась, переходим к следующей записи
            curr_idx += 1
            prev_idx += 1
        else:
            raise RuntimeWarning("Unknown answer from follower on port %d: '%s' " % (port, ans))
        complete = curr_idx == 0 # следующее значение после -1. если мы до него дошли, значит удалось вставить все записи.




parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required = True, type = int)
parser.add_argument("-l", help="Existing log", required = True)
parser.add_argument("-t", help="Term number", required = True, type = int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

if args.l:
    for row in args.l.split(","):
        gen, val = row.split(":")
        journal.append((int(gen), val))
is_main = bool(args.f)
generation = args.t

start_server(args.p)

if args.f:
    print "Leader is starting. Trying to contact followers..."
    follower_ports = [int(f.strip()) for f in args.f.split(",")]
    for port in follower_ports:
        try:
            replicate_to_follower(port)
        except Exception as e:
            print(e)

while True:
    time.sleep(1)