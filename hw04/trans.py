# encoding: utf-8

    #на нулевой итерации каждая вершина сообщает своим детям, что они достижимы из нее
    #на каждой следующей иттерации, если есть входящие сообщения, вершина проверяет, не появились ли
    #неизвестные ранее предки:
    #если появились, то активизируемся, записываем новых предков в value и сообщаем о них детям

from pregel import Vertex, Pregel
from hw04util import *
from random import randint
import sys

vertices = {}
num_workers = 1

def main(filename):
    global vertices
    # читаем граф из файла, используя конструктор TransVertex
    vertices = read_graph(filename, TransVertex)
    num_vertices = len(vertices)

    for v in vertices.values():
        v.value = set()

    # Запускаем подсчет,
    # кол-во иттераций == длина самого длинного пути в графе <= количество вершин
    p = Pregel(vertices.values(), num_workers, num_vertices)
    p.run()

    # для каждой вершины в values лежат номера всех вершин, достижимых из нее
    print("Completed in %d supersteps" % p.superstep)

    trans_graph = {}
    for v in p.vertices:
        trans_graph[v.id] = []
    for vertice in p.vertices:
        for p in vertice.value:
            trans_graph[p].append(str(vertice.id))
    for vertice in trans_graph:
        if trans_graph[vertice]:
            print("{0} {1}".format(vertice, ",".join(trans_graph[vertice])))
        else:
            print("{0} ==".format(vertice))

class TransVertex(Vertex):
        def __init__(self, id):
            Vertex.__init__(self, id, None, [])
            self.value = set()          # мн-во известных предков
            self.new_parents = set()    # новые предки последней иттерации

        def update(self):
                global vertices

                if self.superstep == 0:
                    self.active = True
                    # сообщаем всем детям, что они достигаются из текущей вершины
                    self.new_parents.add(self.id)
                else:
                    self.active = False
                    if self.incoming_messages:
                        # объединим полученных новых предков
                        message_parents = set()
                        for (v, upd_parents) in self.incoming_messages:
                            message_parents.update(upd_parents)

                        # убираем уже известные текущей вершине
                        self.new_parents = message_parents.difference(self.value)
                        if self.new_parents:
                            # делаем активной, т.к. добавились ранее неизвестные предки
                            self.active = True
                            self.value.update(self.new_parents)

                if self.active:
                    # активная вершина должна рассказать всем детям о новых предках
                    self.outgoing_messages = [(vertex,self.new_parents) for vertex in self.out_vertices]


if __name__ == "__main__":
        main(sys.argv[1])