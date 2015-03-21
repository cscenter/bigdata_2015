import mincemeat
import sys
sys.path.append('../dfs/')
import client as dfs


def mapfn(k, v):
  import math
  import util
  import sys
  sys.path.append("../dfs/")
  import client as dfs

  name, title = v.strip().split(' ', 1)
  print name, " ", title

  def normalize(term):
    term = term.lower()
    return term

  term_count = {}
  for line in dfs.get_file_content(name):
    for term in line.encode('utf-8').strip().split(" "):
      term = normalize(term)
      term_count[term] = term_count[term] + 1 if term in term_count else 1

  for term in term_count:
    tf = 1 + math.log(float(term_count[term]))
    yield util.encode_term(term), (name, tf)


def reducefn(k, vs):
  import math

  if k:
    with open("tmp/plist/%s" % k, 'w') as f:
      f.write('\n'.join('%s %.3f' % (v[0], v[1]) for v in vs))

  return {}


s = mincemeat.Server()
wikipedia_files = [l for l in dfs.get_file_content("/wikipedia/__toc__")]
s.map_input = mincemeat.MapInputSequence(wikipedia_files)
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="")
