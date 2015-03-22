#encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
max_supersteps = 50

def main(filename):
  global vertices
  vertices = read_graph(filename, TransClosureVertex) 
  p = Pregel(vertices.values(), num_workers, max_supersteps)
  p.run()
  print "Completed in %d supersteps" % p.superstep
  for vertex in p.vertices:
    print "#%s: %s" % (vertex.id, ','.join([str(v.id) for v in sorted(vertex.out_vertices, key = lambda(v): int(v.id))]))


class TransClosureVertex(Vertex):
  def __init__(self, id):
    Vertex.__init__(self, id, None, [])
    self.new_vertices = set()

  def update(self):
    global vertices

    #первый шаг: уведомляем потомков о своём существовании
    if self.superstep == 0:
      self.outgoing_messages = [(v, self) for v in self.out_vertices]

    #далее шаги делятся на два типа: запрос предков и ответ потомков
    
    #потомок посылает всем предкам информацию о своих потомках, если у него предков нет, он "умирает"
    elif self.superstep % 2 != 0:
      if self.incoming_messages:
        self.links = [v for (_, v) in self.incoming_messages]
        for v in self.links:
          self.outgoing_messages = [(v, i) for i in self.out_vertices]
      else:
        self.active = False


    #предок получает новых потомков, выбирает из них тех, о которых он не знает,
    #и кто не он сам, отправляет новеньким запрос, и сохраняет информацию о них
    else:
      if self.incoming_messages:
        self.links = [v for (_, v) in self.incoming_messages]
        self.new_vertices = set(self.links) - self.out_vertices - set([self])
        self.outgoing_messages = [(v, self) for v in self.new_vertices]

        for i in self.new_vertices:
          self.out_vertices.add(i)


if __name__ == "__main__":
    main(sys.argv[1])