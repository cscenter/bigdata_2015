#!/usr/local/bin/python
# coding=utf-8
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import argparse
import cgi
import requests


OK_CODE = 200
FAIL_CODE = 201


class RaftHandler(BaseHTTPRequestHandler):
    def parse_POST_query(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        query = {}
        if ctype == 'multipart/form-data':
            query = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            query = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)

        parameters = ['prev_ind', 'prev_gen', 'new_rec']
        if not all(parameter in query for parameter in parameters):
            return None

        query['prev_ind'] = int(query['prev_ind'][0])
        query['prev_gen'] = int(query['prev_gen'][0])
        query['new_rec'] = decode_record(query['new_rec'][0])

        return query

    def do_GET(self):
        """ Узел принимает пустой GET запрос (без каких-либо путей или параметров) и возвращает свой журнал."""
        self.send_response(OK_CODE)
        self.send_header('content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(encode_log())

    def do_POST(self):
        """
        Ведомый принимает POST запрос вида 'new_rec=3:H&prev_ind=4&prev_gen=5'
        и пытается добавить запись new_rec в журнал.
        Возвращает код 200 в случае успеха, и 201 в случае неудачи.
        """
        if is_leader:
            self.send_response(FAIL_CODE, 'unsupported operation for leader')
            return

        global log

        query = self.parse_POST_query()

        if not query:
            self.send_response(FAIL_CODE, 'wrong POST query. Query does not contain some necessary keys')
            return

        prev_ind = query['prev_ind']
        prev_gen = query['prev_gen']
        new_gen, new_val = query['new_rec']

        index_is_within_bounds = 0 <= prev_ind < len(log)
        generations_are_consistent = index_is_within_bounds and prev_gen == log[prev_ind][0]
        was_never_synced = not index_is_within_bounds and prev_gen == 0

        if index_is_within_bounds and generations_are_consistent or was_never_synced:
            self.send_response(OK_CODE)
            log = [] if was_never_synced else log[0:prev_ind + 1]
            log.append((new_gen, new_val))
        else:
            self.send_response(FAIL_CODE)


def replicate_to_follower(port):
    curr_ind = len(log)
    while True:
        curr_ind -= 1

        prev_ind = curr_ind - 1
        prev_gen = log[prev_ind][0] if curr_ind > 0 else 0
        new_record = log[curr_ind] if curr_ind > 0 else log[0]

        was_added = try_to_add(port, new_record, prev_gen, prev_ind)
        completely_replicated = was_added and curr_ind == len(log) - 1

        if completely_replicated:
            return

        if was_added:
            curr_ind += 2


def try_to_add(port, new_record, prev_gen, prev_ind):
    data = {'new_rec': encode_record(new_record), 'prev_gen': prev_gen, 'prev_ind': prev_ind}
    r = requests.post('http://localhost:{}'.format(port), data)
    return r.status_code == OK_CODE


def encode_log():
    return ','.join([encode_record(record) for record in log])


def decode_log(str_log):
    unpack = lambda x: (x[0], x[1])
    return [unpack(record.split(':')) for record in str_log.split(',')]


def encode_record(record):
    return '{}:{}'.format(record[0], record[1])


def decode_record(record):
    gen, val = record.split(':')
    return int(gen), val


def get_log(port):
    r = requests.get('http://localhost:%d' % port)
    if r.status_code != OK_CODE:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    return r.text.decode(encoding='UTF-8')


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print 'Started httpserver on port %d' % port
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

log = decode_log(args.l)
is_leader = False

start_server(args.p)
if args.f:
    is_leader = True
    print 'Leader is starting. Trying to contact followers...'
    follower_ports = [int(f.strip()) for f in args.f.split(",")]
    for port in follower_ports:
        replicate_to_follower(port)
        print 'Successfully replicated follower at %d, it\'s log: %s' % (port, get_log(port))
else:
    print "Started follower"
while True:
    time.sleep(1)