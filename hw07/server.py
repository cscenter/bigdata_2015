#!/usr/bin/python
#Using python 3.4
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import urlopen
from urllib.parse import urlparse, parse_qs
import threading
import time
import argparse

log = []
step = None
class RaftHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    global log, step
    parsed = urlparse(self.path)
    query = parse_qs(parsed.query)
    params = {}
    path = parsed.path
    f = False
    for k in query:
        if len(query[k]) > 1:
            f = True
            break
        else:
            params[k] = query[k][0]

    if f:
        self.send_response(500)
        self.send_header('Content-type','text/plain')
        self.end_headers()

        self.wfile.write("invalid parameters")

    self.send_response(200)
    self.send_header('Content-type', 'text/plain')
    self.end_headers()
    if path == "/add":
        for x in ["itemg", "itemv", "prev", "prevg"]:
            if x not in params:
                f = True
                break
        if f:
            self.send_response(500)
            self.send_header('Content-type','text/plain')
            self.end_headers()

            self.wfile.write(b"invalid parameters")

        cur = (int(params["itemg"]), params["itemv"])
        prev = int(params["prev"])
        prevg = int(params["prevg"])


        # Журналы полностью рассинхронизоывнны
        if prev < 0:
            log = [cur]
            step = cur[0]
            self.wfile.write(b"success")
            # Если ведомый находит у себя предыдущуюзапись, то он принимает новую
        elif prev < len(log) and log[prev][0] == prevg:
            log = log[:prev+1]
            log.append(cur)
            self.wfile.write(b"success")
        else:
            self.wfile.write(b"fail")
    if path == "/log":
        self.wfile.write(bytes(",".join(["{}:{}".format(*x) for x in log]), encoding='UTF-8'))

    return


def start_server(port):
  server = HTTPServer(('', port), RaftHandler)
  print('Started httpserver on port %d' % port)
  t = threading.Thread(target=server.serve_forever)
  t.setDaemon(True)
  t.start()
  return t

#минхронизует журнал с заданным ведомым
def process_follower(port):
    last = len(log)-1
    while last < len(log):
        cur = log[last]
        # посылает itemg - поколение текущей записи, itemv - значние текущей записи, prev - индекс предыдущей записи
        # а также prevg - поколение предыдущей записи
        try:
            res = urlopen("http://localhost:{}/add?itemg={}&itemv={}&prev={}&prevg={}".format(port, cur[0], cur[1], last-1,
                                                                                              log[last-1][0]))
        except:
            return False

        if res.getcode() != 200:
            return False
        status = res.readline().decode(encoding="UTF-8").strip()
        if status == "success":
            last += 1
        else:
            last -= 1
    return True





parser = argparse.ArgumentParser()
parser.add_argument("-p", help="Port number", required = True, type = int)
parser.add_argument("-l", help="Existing log", required = True)
parser.add_argument("-t", help="Term number", required = True, type = int)
parser.add_argument("-f", help="Comma-separated list of follower node ports")

args = parser.parse_args()

start_server(args.p)
# parse provided log

log = [(int(x.split(":")[0]), x.split(":")[1]) for x in args.l.split(",")]
step = int(args.t)


if not log or log[-1][0] != step:
    print("Nothing to apply in current generation")
    exit()

if args.f:
  print("Leader is starting. Trying to contact followers...")
  follower_ports = [int(f.strip()) for f in args.f.split(",")]
  ports_left = set(follower_ports)
  # Пытаемся набрать кворум
  while len(ports_left) >= len(follower_ports)/2:
      for port in list(ports_left):
        if process_follower(port):
            ports_left.remove(port)

 # запись можно производить. Тут надо было бы разослать пинги с принятием записей, но у нас все принимается
 #  на этапе рассылки данных
  print("Replication ended with a SUCCESS!!!!!111111")

  #пытаемся на всякий случай таки достучаться до тех, кто нас отверг =)
  for port in list(ports_left):
      process_follower(port)

  for port in follower_ports:
      try:
        res = urlopen("http://localhost:{}/log".format(port))
        print("Server on port {} log status:".format(port))
        print(res.readline().decode(encoding="UTF-8").strip())

      except:
        print("Couldn't access log from server at port:{}".format(port))
      print("\n")


else:
  print("Started follower")
while True:
    time.sleep(1)