#!/usr/bin/python
# encoding: utf-8
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen
import urlparse
from string import strip

"""
При взаимодейтсвии ведомого и ведущего возможны следующие запросы:

1. Ведомый принимает GET запросы на ресурс /notify который посылается при старте лидера, он оповещяет всех ведомых
о текущем состоянии лога.
Запрос имеет следующие параметры:
    index - Последний индекс в цепочке лога лидера гарантирующий что все записи в местах с индексами
    меньшими либо равными ему, консистентны.
    term - Текущее поколенеи лидера.
Ответом на запрос могут быть:
    "success::" - В случае когда лог ведомого становистя идентичен логу ведущего
    "can_sync:n1:n2" - Где n1 - идентификатор поколения, n2 - идентификатор индекса. В случае когда ведомый имеет
    несовпадающий лог с ведущим. Он посылает пары (идентификатор поколения и индекс) из его лога сверя с какой позиции
    логи ведущего и ведомого совпадают.
    "fail::" - В случае если проищошла ошибка.
Пример запроса и ответа:
GET /notify?term=6&index=9
can_sync:4:5

2. Ведомый принимает GET запросы на ресурс /reject_sync который посылается ведущим после очередного ответа в виде
can_sync от ведомого. Этот запрос посылатся в случае если переданная пара (идентификатор поколения и индекс) из лога
ведомого не содержится в логе ведущего, тоесть нужна более ранняя запись.
Запрос имеет следующие параметры:
    index - Соответсвует индексу переданному с ответом can_sync.
    term - Соответсвует поколению переданному с ответом can_sync.
Ответом на запрос могут быть:
    "success::" - В случае когда лог ведомого становистя идентичен логу ведущего
    "can_sync:n1:n2" - Где n1 - идентификатор поколения, n2 - идентификатор индекса. В случае когда ведомый имеет
    несовпадающий лог с ведущим. Он посылает пары (идентификатор поколения и индекс) из его лога сверя с какой позиции
    логи ведущего и ведомого совпадают.
    "fail::" - В случае если проищошла ошибка.
Пример запроса и ответа:
/reject_sync?term=1&index=1
can_sync:1:0

3. Ведомый принимает GET запрос на ресурс /confirm который посылается в случае когда ведущий обнаруживает общую часть
лога с ведомым после очередного ответа в виде can_sync от ведомого.
Запрос имеет следующие параметры:
    index - Соответсвует индексу переданному с ответом can_sync.
    term - Соответсвует поколению переданному с ответом can_sync.
    log_part - Недостающая часть лога для ведомого. Передается в формате строки где через запятую перечислены попорядку
    пары представляющие записи лога, где в каждой записи разделенные двоиточием сначала идет номер поколения, потом значение.
    Пример: 1:H,1:E,1:L

Пример запроса и ответа:
/confirm?term=4&index=5&log_part=4:C,4:Z,5:X,5:W,6:U
success::


Алгоритм реализует базоывае рекомендации из статьи http://ramcloud.stanford.edu/raft.pdf.
В начале работы ведущий оповещяет всех ведомых о текущем состоянии лога, если ведомый обнаруживает несовпадения,
 то он начинает последовательно искать общую часть лога с вещушим, после того когда она будет найдена, ведущий отсылает
 недостающю часть после чего логи синхронизируются.
"""

def handle_notify(params):
    global sync_index, sync_term, log
    try:
        leader_index = int(params['index'])
        leader_term = int(params['term'])
        if leader_index == -1:
            del log[:]
            sync_index = leader_index
            sync_term = leader_term
            print_syncronised_log()
            return "success::"
        else:
            if len(log) > 0:
                final_index = len(log) - 1
                final_term, final_value = log[final_index]
                if leader_index == final_index and leader_term == final_term:
                    print_syncronised_log()
                    return "success::"
                elif final_index > leader_index:
                    sync_index = leader_index
                    sync_term, _ = log[sync_index]
                    if sync_term == leader_term:
                        del log[sync_index + 1:final_index + 1]
                        print_syncronised_log()
                        return "success::"
                    else:
                        return "can_sync:%d:%d" % (sync_term, sync_index)
                else:
                    sync_index = final_index
                    sync_term, _ = log[sync_index]
                    return "can_sync:%d:%d" % (sync_term, sync_index)
            else:
                return "can_sync:%d:%d" % (-1, -1)
    except Exception as e:
        print(e)
        return "fail::"


def handle_reject_sync(params):
    global sync_index, sync_term, log
    try:
        last_index = int(params['index'])
        last_term = int(params['term'])
        if sync_index == last_index:
            if sync_index > 0:
                sync_index -= 1
                sync_term, _ = log[sync_index]
            else:
                sync_index = -1
                sync_term = -1
            return "can_sync:%d:%d" % (sync_term, sync_index)
        else:
            print("Unexpected index from leader")
            return "fail::"
    except Exception as e:
        print(e)
        return "fail::"


def handle_confirm(params):
    global sync_index, sync_term, log
    try:
        last_index = int(params['index'])
        last_term = int(params['term'])
        log_part = params['log_part']
        if sync_index == last_index:
            log_part_list = []
            for str_pair in log_part.split(","):
                t, v = str_pair.split(":")
                t = int(t)
                log_part_list.append((t, v))
            log.extend(log_part_list)
            sync_index = len(log) - 1
            sync_term, _ = log[sync_index]
            print_syncronised_log()
            return "success::"
        else:
            print("Unexpected index from leader ")
            return "fail::"
    except Exception as e:
        print(e)
        return "fail::"


def print_syncronised_log():
    global sync_index, sync_term, log
    print("Term:" + str(sync_term))
    print("Index:" + str(sync_index))
    print(",".join(["%d:%s" % (it[0], it[1]) for it in log]))


class RaftHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        parsed_url = urlparse.urlparse(self.path)
        params = dict(urlparse.parse_qsl(parsed_url.query))
        if parsed_url.path == '/notify':
            response = handle_notify(params)
        elif parsed_url.path == '/reject_sync':
            response = handle_reject_sync(params)
        elif parsed_url.path == '/confirm':
            response = handle_confirm(params)
        # Send the html message
        print "Send to leader:" + response
        print_syncronised_log()
        self.wfile.write(response)
        return


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print 'Started httpserver on port %d' % port
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t


def follower_behaviour():
    pass


def leader_behaviour(follower_ports):
    global log, term_f_map, term
    if len(log) > 0:
        if term in term_f_map:
            committed_index = term_f_map[term] - 1
        else:
            committed_index = len(log) - 1
    else:
        committed_index = - 1
    for port in follower_ports:
        result, follower_term, follower_index = send_notify(port, term, committed_index)
        inf_counter = 100000
        while inf_counter > 0:
            inf_counter -= 1
            if result == "success":
                print "Follower on port %d synchronised" % port
                break
            elif result == "fail":
                print "Follower on port %d failed" % port
                break
            elif result == "can_sync":
                if follower_index == -1:
                    result, follower_term, follower_index = send_confirm_sync(port, follower_term, follower_index, log)
                elif follower_index < len(log) and log[follower_index][0] == follower_term:
                    result, follower_term, follower_index = send_confirm_sync(port, follower_term, follower_index,
                                                                              log[follower_index:committed_index + 1])
                else:
                    result, follower_term, follower_index = send_reject_sync(port, follower_term, follower_index)
    print "Complete"


def send_notify(port, term, committed_index):
    resp = urlopen(url="http://localhost:%d/notify?term=%d&index=%d" % (port, term, committed_index))
    return parse_response(resp, port)


def send_reject_sync(port, follower_term, follower_index):
    resp = urlopen(url="http://localhost:%d/reject_sync?term=%d&index=%d" % (port, follower_term, follower_index))
    return parse_response(resp, port)


def send_confirm_sync(port, follower_term, follower_index, log_part):
    log_part_str = ",".join(["%d:%s" % (it[0], it[1]) for it in log_part])
    resp = urlopen(url="http://localhost:%d/confirm?term=%d&index=%d&log_part=%s" % (
        port, follower_term, follower_index, log_part_str))
    return parse_response(resp, port)


def parse_response(resp, port):
    if resp.getcode() != 200:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    response = resp.read().decode(encoding='UTF-8')
    print "Follower on port %d answer:%s" % (port, response)
    split_result = response.split(":")
    if len(split_result) == 3:
        result, follower_term, follower_index = response.split(":")
        if follower_index:
            follower_index = int(follower_index)
        else:
            follower_index = None
        if follower_term:
            follower_term = int(follower_term)
        else:
            follower_term = None
        return result, follower_term, follower_index
    else:
        return split_result[0], None, None


def extract_log(log_str):
    index = 0
    current_term = -1
    for l in log_str.split(","):
        t, v = l.strip().split(":")
        t = int(t)
        if t != current_term:
            if current_term != -1:
                term_f_map[current_term] = index
            current_term = t
            term_s_map[t] = index
        log.append((t, v))
        index += 1
    term_f_map[current_term] = index


parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required=True, type=int)
parser.add_argument("-l", help="Existing log", required=True)
parser.add_argument("-t", help="Term number", required=True, type=int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

self_port = args.p
start_server(self_port)

term_s_map = {}
term_f_map = {}
log = []
term = int(args.t)
extract_log(args.l)
sync_index = -1
sync_term = -1

if args.f:
    print "Leader is starting. Trying to contact followers..."
    follower_ports = [int(f.strip()) for f in args.f.split(",")]
    leader_behaviour(follower_ports)
else:
    print "Started follower"
    follower_behaviour()
while True:
    time.sleep(1)
