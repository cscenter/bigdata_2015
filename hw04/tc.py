# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
from collections import defaultdict
from random import randint
import sys

vertices = {}
num_workers = 1

def main(filename):
  global vertices
  global num_vertices
  global num_iterations
  # читаем граф из файла, используя конструкток TCVertex
  vertices = read_graph(filename, TCVertex)

  # Инициализируем начальные значения вершин
  num_iterations = num_vertices = len(vertices)
  for v in vertices.values():
    v.value = set()

  # Запускаем подсчет
  transitive_closure(vertices.values())

def transitive_closure(vertices):
  """Computes the transitive closure vertices, using
  Pregel."""
  p = Pregel(vertices,num_workers,num_iterations)
  p.run()
  G = defaultdict(list)
  for vertex in p.vertices:
    for reachable in vertex.reachable:
      G[reachable].append(str(vertex.id))
  for vertex in p.vertices:
    if len(G[vertex.id]) > 0:
      print "%s %s" % (vertex.id, ",".join(G[vertex.id]))
    else:
      print "%s ==" % vertex.id


class TCVertex(Vertex):
  def __init__(self, id):
    Vertex.__init__(self, id, None, [])
    self.reachable = set()

  def update(self):
    global vertices
    diff = set()
    # на каждом шаге кроме 0-го получаем сообщения от других вершин
    if self.superstep > 0:
      received = set()
      if len(self.incoming_messages) > 0:
        for (_, gained) in self.incoming_messages:
          received |= gained
        # находим вершины полученные впервые
        diff = received - self.reachable
        if len(diff) > 0:
          self.active = True
          self.reachable |= diff
    else:
      # на 0-ом шаге каждая вершина рассылает свой id соседним вершинам
      self.active
      diff.add(self.id)
    if self.active:
      self.outgoing_messages = [(vertex,diff)
                                for vertex in self.out_vertices]

if __name__ == "__main__":
  main(sys.argv[1])
