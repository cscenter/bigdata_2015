# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
num_iterations = 50 * 4

def main(filename):
  global vertices

  # Читает граф и запускает pregel.
  vertices = read_graph(filename, PageRankVertex)
  p = Pregel(vertices.values() ,num_workers, num_iterations)
  p.run()

  # Выводит получившийся граф.
  for vertex in vertices:
    s = list(sorted(map(lambda x: x.id, vertices[vertex].out_vertices)))
    s = "==" if len(s) == 0 else str(s)[1: -1]
    print("{} {}".format(vertex, s))


class PageRankVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        self.active = True

        # Первая стадия - каждая вершина отправляет свой номер вершинам, соединенным с ней.
        if self.superstep % 4 == 0:
          self.outgoing_messages = [(vertex, self.id) for vertex in self.out_vertices]

        # Вторая стадия - каждая вершина перенаправляет полученные собщения вершинам,
        # которые соединены с ней.
        elif self.superstep % 4 == 1:
          self.outgoing_messages = []
          for _, source in self.incoming_messages:
            self.outgoing_messages.extend([(vertex, source) for vertex in self.out_vertices])

        # Третья стадия - у вершины во входящих сообщениях лежат номера вершин, соединненых с
        # с данной через одного. Этим вершинам она отправляет сообщение со своим номером.
        elif self.superstep % 4 == 2:
          l = list(set(map(lambda (x, y): y, self.incoming_messages)))
          l = map(lambda x: vertices[x], l)
          self.outgoing_messages = [(vertex, self.id) for vertex in l]

        # Четвертая стадия - если у вершины нет ребра, соединящего ее с вершиной через одну,
        # то это ребро появляется.
        else:
          self.active = False
          for _, dist in self.incoming_messages:
            if vertices[dist] not in self.out_vertices:
              self.out_vertices.append(vertices[dist])
              self.active = True


if __name__ == "__main__":
    main(sys.argv[1])
