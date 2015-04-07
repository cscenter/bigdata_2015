# encoding: utf8
#!/usr/bin/python

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen
from string import strip
import json

"""
Отправка новых значений происходит на ресурc /append через POST.
Данные кодируется в json
"""

journal = []

class RaftHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        serialized_journal = ",".join(["%d:%s" % (record[0], record[1]) for record in journal])
        self.wfile.write(serialized_journal)
        return

    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

        content_length = int(self.headers.getheader("Content-Length"))
        data = self.rfile.read(content_length)

        try:
            if self.path.endswith("append"):
                self.append(data)
            return
        except IOError:
            self.send_error(404, "File Not Found: %s" % self.path)


    def append(self, data):
        global journal
        data = json.loads(data, encoding="utf-8")
        new_term, new_value = data["new"]
        prev_commit = data["prev_commit"]
        prev_term = data["prev_term"]
        prev_value = data["prev_value"]

        if prev_commit < len(journal) and journal[prev_commit][0] == prev_term:
            if prev_commit == -1:
                # нет ни одного совпадения в журнале, создадим новый
                journal = []
            else:
                # оставляем только ту часть журнала, которая точно известна, что совпадает
                journal = journal[:prev_commit + 1]
            journal.append((new_term, new_value))
            self.wfile.write("accepted")
        else:
            self.wfile.write("rejected")


def replicate_follower(port, commit):
    if commit <= 1:
        return
    url = "http://localhost:%d/append" % port
    prev_term, prev_value = journal[commit - 1]
    data = {
        "new": journal[commit],
        "prev_commit": commit - 1,
        "prev_term": prev_term,
        "prev_value": prev_value
    }
    response = urlopen(url = ("http://localhost:%d/append" % port), data = json.dumps(data, encoding="utf-8"))
    result = response.read().decode(encoding = "utf-8")
    if result == "accepted":
        commit += 1
        if commit >= len(journal):
            return
    replicate_follower(port, commit - 1)


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print("Started httpserver on port %d" % port)
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t


parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required = True, type = int)
parser.add_argument("-l", help="Existing log", required = True)
parser.add_argument("-t", help="prev_term number", required = True, type = int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

#  Сохраняем исходный лог
if args.l:
    records = args.l.split(",")
    for rec in records:
        prev_term, prev_value = rec.split(":")
        journal.append((int(prev_term), prev_value))

prev_term = args.t

start_server(args.p)

if args.f:
    print("Leader is starting. Trying to contact followers...")
    follower_ports = [int(f.strip()) for f in args.f.split(",")]

    for port in follower_ports:
        replicate_follower(port, commit = len(journal) - 1)

    for port in follower_ports:
        resp = urlopen(url = "http://localhost:%d" % port)
        if resp.getcode() != 200:
            raise Exception("ERROR: can't get response from follower on port %d" % port)
        print("journal of follwer on port %d:" % port)
        print(resp.read().decode(encoding='UTF-8'))
else:
    print("Started follower")
while True:
        time.sleep(1)
