# encoding: utf-8
import mincemeat


import sys
sys.path.append("../dfs/")

import client as dfs

def mapfn(k, v):
  import sys
  sys.path.append("../dfs/")
  import client as dfs
  N = 10000

  for l in dfs.get_file_content(v):
    l = l.strip()
    if len(l) == 0:
      continue
    cols = l.split(" ", 2)
    if len(cols) != 2 and len(cols) != 4:
      sys.stderr.write("Malformed record: " + l)
      continue

    docid = cols[0]
    outlinks = [] if cols == "==" else cols[1].split(",")
    if len(cols) == 4:
      rank = float(cols[2])
      iter_num = int(cols[3])
    else:
      rank = 1.0/N
      iter_num = 0

    if len(outlinks) == 0:
      for d in range(0, N):
        yield str(d), ("rank", rank/N)
    else:
      for d in outlinks:
        yield str(d), ("rank", rank / len(outlinks))
    yield docid, ("outlinks", cols[1])
    yield docid, ("iter", iter_num + 1)


def reducefn(k, vs):
  import sys
  sys.path.append("../dfs/")
  import client as dfs
  N = 10000
  HOSTNAME = socket.gethostname()

  new_rank = 0.0
  iter_num = None
  for v in vs:
    if v[0] == "rank":
      new_rank += v[1]
    elif v[0] == "outlinks":
      outlinks = v[1]
    elif v[0] == "iter":
      iter_num = v[1]
    else:
      sys.stderr.write("Malformed reduce task: key=%s value=%s" % (k, str(vs)))

  if iter_num is None:
    sys.stderr.write("Malformed reduce tas (no iter num): key=%s value=%s" % (k, str(vs)))
  new_rank = 0.85 * new_rank + 0.15 / N
  out_filename = "/class04/iter%d/%s" % (iter_num, HOSTNAME)
  with dfs.file_appender(out_filename) as buf:
    buf.write("%s %s %f %d" % (k, outlinks, new_runk, iter_num))
    print "%s: %s" % (out_filename, "%s %s %.15f %d" % (k, outlinks, new_rank, iter_num))

  return out_filename

s = mincemeat.Server() 

# читаем список файлов, из которых состоят матрицы
parser = argparse.ArgumentParser()
parser.add_argument("--toc", required = True)
args = parser.parse_args()

graph_files = [l for l in dfs.get_file_content(args.toc)]
# и подаем этот список на вход мапперам
s.map_input = mincemeat.MapInputSequence(graph_files) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="") 
