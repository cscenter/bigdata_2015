# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
import sys

__author__ = 'Flok'

vertices = {}
num_workers = 1
max_supersteps = 50

def main(filename):
  global vertices
  global num_vertices
  vertices = read_graph(filename, GraphVertex)
  find_new_edges(vertices.values())

def find_new_edges(vertices):
    p = Pregel(vertices,num_workers,max_supersteps)
    p.run()
    for vertex in p.vertices:
        out_vertices = ",".join(str(vertex.id) for vertex in vertex.out_vertices)
        if not out_vertices:
            out_vertices = "=="
        print "%s: %s %s" % (vertex.id, out_vertices, vertex.value)

class GraphVertex(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def update(self):
        """
        В процессе общения вершины обмениваются двумя типами сообщений:
        (адресат, None) - "дай мне список своих детей"
        (адресат, [список_детей]) - "вот мой список детей"
        """

        global vertices
        if not hasattr(self, "original_out_vertices"):
            self.original_out_vertices = self.out_vertices[:]

        ask_adj = set() # список вершин, у которых мы будем спрашивать список соседей
        #получение сообщений
        if self.superstep > 0:
            # получаем список соседей наших соседей и заводим новые рёбра, указывающие на них.
            for (vertex, adjacent_vs) in self.incoming_messages:
                if adjacent_vs is not None:
                    ask_adj.update(adjacent for adjacent in adjacent_vs if adjacent not in self.out_vertices)

            self.out_vertices.extend(ask_adj)
            self.active = bool(ask_adj)
        else:
            ask_adj = self.out_vertices

        # рассылка сообщений
        # запрашиваем у наших соседей их соседей
        self.outgoing_messages = [(vertex, None) for vertex in ask_adj]
        # и отвечаем тем, кто запросил у нас
        if self.superstep > 0:
            for asking_vertex in [vertex for (vertex, adjacent_vs) in self.incoming_messages if adjacent_vs is None]:
                self.outgoing_messages.append((asking_vertex, self.original_out_vertices))

if __name__ == "__main__":
    main(sys.argv[1])
