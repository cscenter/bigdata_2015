# encoding: utf8

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time
import argparse
from urllib2 import urlopen
from string import strip
import requests

#
# описание запросов в файле description.txt
#

log = []            # журнал, каждая запись -- (номер поколения, значение)
term = -1           # номер поколения
accepted = -1       # номер записи, уже принятой кворумом


class RaftHandler(BaseHTTPRequestHandler):
    # возвращает журнал узла
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

        # собираем журнал в привычный формат
        self.wfile.write(",".join(["{0}:{1}".format(r[0], r[1]) for r in log]))
        return

    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

        try:
            content_len = int(self.headers.getheader('content-length', 0))
            message = self.rfile.read(content_len)

            type, parameters = message.split("\n", 1)

            if type == "add":
                self.add_rec(parameters)
            elif type == "accept":
                self.accept(parameters)
            else:
                raise Exception("Wrong format for POST request! Must be like "
                                "\"type\nparameters\", type = \"add\" or \"accept\"")
        except:
            raise Exception("Wrong format for POST request! Must be like "
                            "\"type\nparameters\", type = \"add\" or \"accept\"")

    def add_rec(self, parameters):
        try:
            prev_term, prev_index, cur_term, cur_val = parameters.split(",")
            prev_term, prev_index, cur_term = int(prev_term), int(prev_index), int(cur_term)
        except:
            raise Exception("Wrong format for add request! Must be like \"prev_term,prev_index,cur_term,cur_val\"")

        global log, term
        if prev_index == -1:
            # значит, нет ни одной синхронизированной записи
            log = [(cur_term, cur_val)]
            term = cur_term
            self.wfile.write("ok")
        elif prev_index < len(log) and log[prev_index][0] == prev_term:
            # нашли совпадение в журналах
            # раз это так, значит, все предшествующие также синхронизированы, добавляем новую
            log = log[:prev_index + 1]
            log.append((cur_term, cur_val))

            # поддерживаем акутальный номер поколения
            term = cur_term
            self.wfile.write("ok")
        else:
            self.wfile.write("not ok")

    def accept(self, parameters):
        try:
            index = int(parameters)
        except:
            raise Exception("Wrong format for accept request! Must be like \"cur_index\"")

        if index < len(log):
            accepted = index
            self.wfile.write("ok")
        else:
            self.wfile.write("not ok")


def start_server(port):
    server = HTTPServer(('', port), RaftHandler)
    print 'Started httpserver on port %d' % port
    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)
    t.start()
    return t


# сихнонизирует журналы лидера и фолловера на порте port
# предполагается, что журнал лидера не пуст
# возвращает True, если журналы синхронизированы, False, если сделать это не удалось (например, отсутствует соединение)
def replicate_log(port):
    done = False

    # индекс предыдущей записи
    prev_index = len(log) - 2
    while not done:
        record = log[prev_index + 1]        # запись, которую хотим добавить

        # формируем запрос POST
        # "add \n prev_term,prev_index,cur_term,cur_val"
        data = "add\n{0},{1},{2},{3}".format(log[prev_index][0], prev_index, record[0], record[1])

        try:
            resp = requests.post("http://localhost:%d" % port, data=data)
        except:
            # не смогли подключиться
            return False

        if resp.status_code != 200:
            # с этим фолловером что-то не так
            return False

        if resp.text.decode(encoding='UTF-8') == "ok":
            # добавили текущую запись, теперь она становится предыдущей
            prev_index += 1
            if prev_index == len(log) - 1:
                # все записи из журнала лидера есть в журнале фолловера
                done = True
        else:
            # не смогли найти предыдущую запись, снова шагаем назад
            prev_index -= 1

    return True

# печатает весь журнал узла
# предполагается, что узел доступен
def print_log(port):
    # формируем запрос GET
    resp = requests.get("http://localhost:%d" % port)
    if resp.status_code != 200:
        raise Exception("ERROR: can't get response from follower on port %d" % port)
    print("Journal of follower on port #{0}:".format(port))
    print(resp.text.decode(encoding='UTF-8'))


parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required=True, type=int)
parser.add_argument("-l", help="Existing log", required=True)
parser.add_argument("-t", help="Term number", required=True, type=int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

# запускаем сервер
start_server(args.p)

# парсим журнал текущего узла
for row in args.l.split(","):
    term, value = row.split(":")
    log.append((int(term), value))

# записываем номер поколения
term = args.t

# работа лидера
if args.f:
    print "Leader is starting. Trying to contact followers..."
    follower_ports = [int(f.strip()) for f in args.f.split(",")]
    num = len(follower_ports)
    not_replicated_ports = follower_ports[:]

    # ждем, пока наберем половину
    while len(not_replicated_ports) > num / 2:
        for port in not_replicated_ports:
            # синхронизируем журналы
            if replicate_log(port):
                not_replicated_ports.remove(port)

    # ура, набрали половину, теперь хотим разослать всем accept
    # в этот момент нужно сообщить клиенту, что его изменения вступили в силу

    accepted = len(log) - 1
    not_accepted_ports = [f for f in follower_ports if f not in not_replicated_ports]
    while len(not_accepted_ports) + len(not_replicated_ports) > 0:
        for port in not_accepted_ports:
            # формируем запрос POST
            # "accept \n index"
            data = "accept\n{0}".format(accepted)
            try:
                resp = requests.post("http://localhost:%d" % port, data=data)
                if resp.status_code == 200 and resp.text.decode(encoding='UTF-8') == "ok":
                    not_accepted_ports.remove(port)
            except:
                # не смогли подключиться
                pass

        # пытаемся все же реплицировать оставшихся
        for port in not_replicated_ports:
            # синхронизируем журналы
            if replicate_log(port):
                not_accepted_ports.append(port)
                not_replicated_ports.remove(port)

    # все синхронизированы, все приняли последнюю запись, работа окончена
    for port in follower_ports:
        print_log(port)

else:
    print "Started follower"
while True:
        time.sleep(1)