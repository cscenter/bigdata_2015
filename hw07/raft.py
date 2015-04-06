#!/usr/bin/python
# encoding: utf8
from string import strip
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import argparse
import cgi
import urllib2
import urllib
log = []


# запрос на обновление, который лидер отправляет ведомому
# в запрос входит: поколение записи из журнала мастера(repl_term);
# само значение записи(repl_val);
# поколение предшествующей записи(prev_term);
# значение предшествующей записи(prev_value);
# значение предыдущего индекса(prev_ind)
def try_to_add_record(port, repl_rec, prev_term, prev_val, prev_ind):
    url = "http://localhost:%d" % port
    values = {'repl_term': repl_rec[0], 'repl_val': repl_rec[1],
    'prev_term': prev_term, 'prev_val': prev_val, 'prev_ind': prev_ind}
    data = urllib.urlencode(values)
    req = urllib2.Request(url, data)
    response = urllib2.urlopen(req)
    if response.getcode() != 200:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    return response.read()


# репликация журналов пользователя
def follower_log_replication(log, port):
    cur_ind = len(log)
    is_replicated = False
    while (not is_replicated):
        cur_ind -= 1
        if cur_ind > 0:
            response = try_to_add_record(port, log[cur_ind], log[cur_ind - 1][0], log[cur_ind - 1][1], cur_ind - 1)
        else:
            response = try_to_add_record(port, log[cur_ind], 0, '', -1)
        if response =="accept":
            if cur_ind == len(log) - 1:
                is_replicated = True
                return
            else:
                cur_ind += 2

# получаем всё содержимое журнала пользователя
def get_follower_log(port):
    resp = urllib2.urlopen(url="http://localhost:%d" % port)
    if resp.getcode() != 200:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    return resp.read().decode(encoding='UTF-8')


class RaftHandler(BaseHTTPRequestHandler):
    # узел возвращает содержимое своего лога
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        global log
        str_of_log = ''
        for rec in log:
            str_of_log += str(rec[0]) + ':' + str(rec[1]) + ','
        str_of_log = str_of_log[0:len(str_of_log) - 1]
        self.wfile.write(str_of_log)
        return
    # ведомый принимает запрос на обновление от лидера, параметры запроса указал выше
    def do_POST(self):
        global log
        message = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
        environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': self.headers['Content-Type']})
        self.send_response(200)
        self.end_headers()
        repl_term = message['repl_term'].value
        repl_val = message['repl_val'].value
        prev_term = message['prev_term'].value
        prev_val = message['prev_val'].value
        prev_ind = int(message['prev_ind'].value)
        if (prev_ind < len(log)) and (prev_ind != -1) and (log[prev_ind][0] == prev_term):
            log = log[0:prev_ind + 1]
            log.append((repl_term, repl_val))
            if (log[prev_ind][1] != prev_val) and (prev_val != ''):
                log[prev_ind][1] = prev_val
            self.wfile.write("accept")
        else:
            self.wfile.write("denied")
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
# составляем журнал
if args.l:
    for record in args.l.split(","):
        (temp, val) = record.split(":")
        log.append((temp, val))
start_server(args.p)
if args.f:
    print "Leader is starting. Trying to contact followers..."
    follower_ports = [int(f.strip()) for f in args.f.split(",")]
    #реплицируем журналы всем ведомым
    print "Replication is started..."
    for port in follower_ports:
        follower_log_replication(log, port)
    print "Replication is completed"
    #запрашиваем журналы у всех ведомых
    print "Followers' logs..."
    for port in follower_ports:
        print 'For follower with port %s log is %s' % (port, get_follower_log(port))
    print "All followers' logs are getted."
else:
    print "Started follower"
while True:
    time.sleep(1)

