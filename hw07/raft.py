#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen


JR = []
is_forward = False

##В общем... реплика принимает запросы /a?pre=fads&new=dfasdf на добаление
# /j  на получение журнала
# все через GET


class RaftHandler(BaseHTTPRequestHandler):
    @staticmethod
    def encode_row(row):
        return "{}:{}".format(row[0], row[1])

    @staticmethod
    def decode_row(row):
        row = row.split(':')
        return int(row[0]), row[1]

    def show_journal(self):
        if len(JR) > 0:
            self.headers()
            self.wfile.write(JR)
        else:
            self.error()

    def add_log(self, params):
        global JR
        if "pre" not in params:
            return
        if "new" not in params:
            return
        params['new'] = self.decode_row(params['new'])
        params['pre'] = self.decode_row(params['pre'])

        if params['pre'][0] == 0:
            JR = [params["new"]]
            self.headers()
            self.wfile.write("200")
            return
        if params['pre'] in JR:
            idx = JR.index(params['pre'])
            JR = JR[:idx + 1]
            JR.append(params['new'])
            self.headers()
            self.wfile.write("200")
        else:
            self.headers()
            self.wfile.write("404\n")

    def do_GET(self):
        self.in_path = urlparse.urlparse(self.path)
        self.paraments = urlparse.parse_qs(self.in_path.query)
        params = {m: n[0] for m, n in self.paraments.items()}
        if self.in_path.path == "/j":
            self.show_journal()
        elif self.in_path.path == "/a":
            if not is_forward:
                self.add_log(params)
        return

    def error(self, msg="Internal Server Error"):
        self.send_response(500)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(msg)

    def headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()


def make_replica(port_in):
    global generation
    if not JR or JR[-1][0] != generation:
        return
    complete = False
    JR_len = len(JR)
    this_index = -1
    pre = -2
    while not complete:
        curr_val = JR[this_index]
        if abs(pre) > JR_len:
            prev_val = (0, 0)
        else:
            prev_val = JR[pre]

        try:
            responce = urlopen(
                url="http://localhost:{}/a?pre={}&new={}".format(port_in,
                                                                 RaftHandler.encode_row(
                                                                     prev_val),
                                                                 RaftHandler.encode_row(
                                                                     curr_val)))
        except:
            pass

        if responce.getcode() != 200:
            print("SMTH went wrong")
            return

        ans = responce.readline().decode(encoding="UTF-8").rstrip()

        if ans == "200":
            this_index += 1
            pre += 1
        elif ans == "404":
            this_index -= 1
            pre -= 1
        else:
            pass
        if this_index == 0:
            complete = True
        else:
            complete = False


def get_journal_from_follower(port_in):
    responce = urlopen("http://localhost:{}".format(port_in))
    if responce.getcode() != 200:
        raise Exception(
            "No responce from replica {}".format(port_in))
    return responce.readline().decode(encoding="UTF-8")


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print('Started httpserver on port %d' % port)
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t


parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required=True, type=int)
parser.add_argument("-l", help="Existing log", required=True)
parser.add_argument("-t", help="Term number", required=True, type=int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

if args.l:
    for record in args.l.split(","):
        JR.append(record.split(":"))
generation = args.t
is_forward = bool(args.f)

start_server(args.p)
if args.f:
    print("Leader is starting. Trying to contact followers...")
    for port in args.f.split(","):
        try:
            make_replica(int(port.strip()))
        except Exception as e:
            print(e)
    for port in args.f.split(","):
        try:
            print(port, get_journal_from_follower(port))
        except Exception as e:
            print(e)
else:
    print("Started follower")
while True:
    time.sleep(1)