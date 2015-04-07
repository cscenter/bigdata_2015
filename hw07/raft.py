# encoding: utf-8

#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
import time
import threading
import argparse
from urllib2 import urlopen

# Ведомый принимает GET запрос get_journal, для которого возвращает свой журнал
# Ведомый принимает POST запрос (add, поколение пред. записи, индекс пред. записи, поколение текущей записи, текущее значение)
#         на добавление    
# Ведомый принимает POST запрос (confirmation), на подтверждение 

log = [] # будем хранить журнал ввиде (номер поколения, значение)
term = 0 

class RaftHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        if self.path == '/?get_journal':
            self.wfile.write(','.join(['{0}:{1}'.format(l[0], l[1]) for l in log]))
        else:
            raise Exception("ERROR in GET: unknown command")
        
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()  
        command_line = self.rfile.read(int(self.headers.getheader('content-length', 0))).split('&')
        command = command_line[0].split('=')[1]
        if (command == 'add'):
            prev_term = int(command_line[1].split('=')[1])
            prev_index = int(command_line[2].split('=')[1])
            cur_term = int(command_line[3].split('=')[1])
            cur_value = command_line[4].split('=')[1]
            success_add = self.add_value(prev_term, prev_index, cur_term, cur_value) # возвращает True если удалось добавить элемент
            if (success_add):
                self.wfile.write('ok')
            else:
                self.wfile.write('fail')
        elif command == 'confirmation':
            print 'I am confirm now' # пришло подтверждение, записываем изменения из журнала в хранилище
        else:
            raise Exception("ERROR in POST: unknown command") 
    
    def add_value(self, prev_term, prev_index, cur_term, cur_value):
        global log
        global term
        if prev_index == -1: 
            log = [(cur_term, cur_value)]
            return True
        if prev_index < len(log) and log[prev_index][0] == prev_term:
            log = log[:prev_index + 1]
            log.append((cur_term, cur_value))
            term = cur_term
            return True    
 

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

term = args.t

for rec in args.l.split(','):
    term_num, value = rec.split(':')
    log.append((int(term_num), value))

def process_followers_log(port):
    leader_index = len(log) - 1
    follower_prev_index = leader_index - 1
    while follower_prev_index < leader_index:
        # check follower_index, if it's allready done increment follower_index
        if follower_prev_index == -1:
            prev_term = -1
            prev_index = -1 
        else:
            prev_term = log[follower_prev_index][0]
            prev_index = follower_prev_index
        cur_term = log[follower_prev_index + 1][0]
        cur_value = log[follower_prev_index + 1][1]
        data = "command=add&prev_term=%d&prev_index=%d&cur_tem=%d&cur_value=%s" % (prev_term, prev_index, cur_term, cur_value)
        try:
            resp = urlopen("http://localhost:%d" % port, data)        
        except:
            return False
        if resp.read() == 'ok':
            follower_prev_index += 1
        else:
            follower_prev_index -= 1
    return True       

start_server(args.p)
if args.f:
    print "Leader is starting. Trying to contact followers..."
    follower_ports = [int(f.strip()) for f in args.f.split(",")]
    follower_num = len(follower_ports)
    quorum_num = 0
    success_ports = set()
    while quorum_num < follower_num / 2:
        for port in follower_ports:
            if port not in success_ports:  
                if process_followers_log(port):
                    success_ports.add(port)
                    quorum_num += 1    
                resp = urlopen(url = "http://localhost:%d?get_journal" % port)
                if resp.getcode() != 200:
                    raise Exception("ERROR: can't get response from follower on port %d" % port)
                print "Follower on port %d has log:" % port
                follower_log = resp.read()
                print follower_log
              #  print ",".join(["{0}:{1}".format(l[0], l[1]) for l in follower_log]) - read возвращает строку, не прокатит
    for ports in list(success_ports):
        resp = urlopen('http://localhost:%d' % ports, 'command=confirmation')            
                
    print 'Leader has log:'
    print (','.join(['{0}:{1}'.format(l[0], l[1]) for l in log]))           
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