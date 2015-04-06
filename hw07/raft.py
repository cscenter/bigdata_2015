#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals

import argparse
import json
import sys
import threading
import time
import urllib
import urllib2
import urlparse
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from math import ceil


def do_append_entries(query):
    global log, current_term, commit_index

    # Получаем сообщение в формате json посредством GET-параметра ``message``
    message = json.loads(query['message'][0])
    leader_term = message['leader_term']
    prev_log_index = message['prev_log_index']
    prev_log_term = message['prev_log_term']
    entries = message['entries']
    leader_commit = message['leader_commit']

    # если период лидера старше, то увеличиваем свой
    if leader_term > current_term:
        current_term = leader_term

    # отвечаем false, если период лидера младше или если по ``log_prev_index`` не лежит объект
    # периода ``log_prev_term``
    # иначе добавлям новые записи, при необоходимости удаляя некорректные, если потребуется
    if leader_term < current_term or \
            len(log) - 1 < prev_log_index or \
            log[prev_log_index][0] < prev_log_term:
        result = False
    else:
        result = True
        log = log[:prev_log_index + 1] + entries
        if leader_commit > commit_index:
            commit_index = min(leader_commit, len(log) - 1)

    log_str = ','.join(str(term) + ':' + record for term, record in log)
    print('Result = %s ;; Log = %s' % (result, log_str))

    return json.dumps((current_term, result))


class RaftHandler(BaseHTTPRequestHandler):

    # обработчик путей только один -- AppendEntriesRPC
    paths = {
        '/append_entries': do_append_entries,
    }

    def do_GET(self):
        request = urlparse.urlparse(self.path)
        if request.path not in self.paths:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            return
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        content = self.paths[request.path](urlparse.parse_qs(request.query))
        self.wfile.write(content)
        return


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print('Started httpserver on port %d' % port)
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t


parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', help='Port number', required=True, type=int)
parser.add_argument('-l', '--log', help='Existing log', required=True)
parser.add_argument('-t', '--term', help='Term number', required=True, type=int)
parser.add_argument('-f', '--followers', help='Comma-separated list of follower node ports')

args = parser.parse_args()

start_server(args.port)


# -- State --
# persistent state
current_term = args.term
log = [(int(term), record) for term, record in (x.split(':') for x in args.log.split(','))]

# volatile params
commit_index = -1  # index of highest log entry known to be committed


if args.followers:
    leader = True
    print('Leader is starting. Trying to contact followers...')

    # leader state
    followers = [int(f.strip()) for f in args.followers.split(',')]
    majority = int(ceil((len(followers) + 1) / 2))
    next_index = {}
    match_index = {}
    for port in followers:
        # считаем, что последняя запись на лидере не закоммичена нигде пока (только что поступила
        # от клиента) -- начиаем репликацию с нееs
        next_index[port] = len(log) - 1
        match_index[port] = -1

    # initiate log replication
    # пока majority (т. е. больше половины серверов не примет изменения)
    succeded = set()
    while len(succeded) < majority:
        for port in followers:
            if len(log) - 1 >= next_index[port]:
                if port in succeded:
                    continue
                message = {
                    'leader_term': current_term,
                    'prev_log_index': next_index[port] - 1,
                    'prev_log_term': log[next_index[port] - 1][0],
                    'entries': log[next_index[port]:],
                    'leader_commit': commit_index,
                }
                encoded = urllib.quote(json.dumps(message))
                resp = urllib2.urlopen('http://localhost:%d/append_entries?message=%s' % (port,
                                                                                          encoded))
                # в случае ошибки сети повторяем
                if resp.getcode() != 200:
                    next_index[port] -= 1
                    continue
                print('Follower on port %d is ok:' % port)
                response = resp.read().decode('utf-8')
                print(response)
                term, success = json.loads(response)
                # если период последователя старше -- то лидер больше не лидер
                if term > current_term:
                    print('Not a leader!')
                    sys.exit(1)
                if success:
                    succeded.add(port)
                    next_index[port] += len(message['entries'])
                    match_index[port] = next_index[port]
                else:
                    next_index[port] -= 1
                    continue
            else:
                if port not in succeded:
                    succeded.add(port)
    commit_index = len(log) - 1
else:
    leader = False
    print('Started follower')

while True:
    time.sleep(1)
