# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
from random import randint
import sys

vertices = {}
num_workers = 1
max_supersteps = 50

def main(filename):
    global vertices
    global num_vertices
    # читаем граф из файла, используя конструктор ClosureVertex
    vertices = read_graph(filename, ClosureVertex)

    # Запускаем подсчет, ограничивая количеством итераций
    p = Pregel(vertices.values(),num_workers,max_supersteps)
    p.run()
    print "Completed in %d supersteps" % p.superstep
    for vertex in p.vertices:
        children = ", ".join(str(child.id) for child in vertex.out_vertices)
        if not children:
            children = "=="
        print "#%s: %s" % (vertex.id, children)



class ClosureVertex(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        # по умолчанию эта вершина станет пассивной
        self.active = False
        if self.superstep == 0:
            # Первый шаг рассылаем сообщения всем смежным вершинам на исходящих дугах
            self.outgoing_messages = [(adj_vertex, None) for adj_vertex in self.out_vertices]
        else:
            if len(self.incoming_messages) > 0:
                # обрабатываем сообщения входящих дуг
                # in_vertex присылает свою предыдущую вершину
                for (in_vertex, in_adj_vertex) in self.incoming_messages:
                    if in_adj_vertex is not None:
                        if in_adj_vertex not in self.out_vertices:
                            # in_vertex сообщила нам о своей смежной вершине in_adj_vertex
                            self.out_vertices.append(in_adj_vertex)
                            # Запросим есть ли другие смежные вершины в in_adj_vertex
                            # self.outgoing_messages.append((in_adj_vertex, None))
                            self.outgoing_messages += [(in_vertex, None)]
                    else:
                        for adj_vertex in self.out_vertices:
                            if in_vertex != adj_vertex:
                                # Передаём дёрнущей нас вершине все свои смежные вершины
                                # self.outgoing_messages.append((in_vertex, adj_vertex))
                                self.outgoing_messages += [(in_vertex, adj_vertex)]


if __name__ == "__main__":
    main(sys.argv[1])
