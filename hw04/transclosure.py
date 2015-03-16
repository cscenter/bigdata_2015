# -*- coding: utf-8 -*-

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
max_supersteps = 50


def main(filename):
    global vertices
    global num_vertices

    # читаем граф из файла, используя конструктор TransitiveClosureVertex
    vertices = read_graph(filename, TransitiveClosureVertex)

    # Запускаем подсчет, ограничивая количеством итераций
    p = Pregel(vertices.values(), num_workers, max_supersteps)
    p.run()

    print 'Completed in %d supersteps' % p.superstep
    for vertex in p.vertices:
        print '%s %s' % (vertex.id, ','.join(str(v.id) for v in vertex.out_vertices) or '==')


class TransitiveClosureVertex(Vertex):

    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices

        if self.superstep > 0:
            # (конец, начало) для каждого нового ребра
            new_edges = []

            # по умолчанию эта вершина будет пассивной
            self.active = False

            if self.incoming_messages:
                # Если входящие сообщения есть, то попробуем найти ребра, содержащие текущую
                # вершину в качестве переходной
                for _, start_vertex in self.incoming_messages:
                    for end_vertex in self.out_vertices:
                        if end_vertex not in start_vertex.out_vertices:
                            self.active = True
                            start_vertex.out_vertices.append(end_vertex)
                            new_edges.append((end_vertex, start_vertex))

            self.outgoing_messages = new_edges

        else:
            # на первом шаге отсылаем всем вершинам, в которые можно перейти
            self.outgoing_messages = [(vertex, self)
                                      for vertex in self.out_vertices]

if __name__ == '__main__':
    main(sys.argv[1])
