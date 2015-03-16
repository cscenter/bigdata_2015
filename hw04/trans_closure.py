# encoding: utf-8
"""
Описание алгоритма:
    на первом шаге каждая вершина сообщает своим предкам, что те достижимы из нее
    на каждом следующем шаге, если есть входящие сообщения, вершина проверяет, есть ли среди
        полученных сообщений информация о неизвестных ранее ее предках (все известные хранятся в
        value), и если есть, активизируется, запоминает этих предков и рассылает информацию о них
        своим ближайшим потомкам
    так как на каждом шаге известные ранее вершины отсекаются, максимальное число шагов -- длина самого
        длинного пути в графе, это в худшем случае количество вершин.
"""

from pregel import Vertex, Pregel
from hw04util import *
from random import randint
import sys

vertices = {}
num_workers = 1

def main(filename):
    global vertices
    # читаем граф из файла, используя конструктор TransClosureVertex
    vertices = read_graph(filename, TransClosureVertex)
    num_vertices = len(vertices)

    # для каждой вершины в values() хранятся все вершины, из которых она достижима
    # инициализируем пустым множеством
    for v in vertices.values():
        v.value = set()

    # Запускаем подсчет,
    # максимальное количество итераций -- длина самого длинного пути в графе, в худшем случае это количество вершин
    p = Pregel(vertices.values(), num_workers, num_vertices)
    p.run()

    # в итоге для каждой вершины в values имеем все вершины, из которых она достижима, то есть
    # для каждой вершины из этого списка нужно добавить исходящее ребро к текущей
    print("Completed in %d supersteps" % p.superstep)

    # теперь собираем это в исходное представление графа
    out_links = {}
    for v in p.vertices:
        out_links[v.id] = []
    for vertex in p.vertices:
        # print("#{0} is achievable from: {1}".format(vertex.id, " ".join([str(val) for val in vertex.value])))
        for parent in vertex.value:
            out_links[parent].append(str(vertex.id))
    # выводим
    for vertex in out_links:
        if len(out_links[vertex]) > 0:
            print("{0} {1}".format(vertex, ",".join(out_links[vertex])))
        else:
            print("{0} ==".format(vertex))

class TransClosureVertex(Vertex):
        def __init__(self, id):
            Vertex.__init__(self, id, None, [])
            self.new_parents = set()  # используется для хранения новых вершин, из которых наша достигается
                                      # (грубо говоря, предков)
            self.value = set()

        def update(self):
                global vertices

                if self.superstep == 0:
                    # на нулевой итерации еще нет входящих, нужно всем по исходящим дугам сообщить,
                    # что они достигаются из текущей
                    self.active = True
                    self.new_parents.add(self.id)
                else:
                    # по умолчанию эта вершина станет пассивной
                    self.active = False
                    if len(self.incoming_messages) > 0:
                        # если входящие сообщения есть, то объединяем полученных новых предков
                        received_parents = set()
                        for (_, parents) in self.incoming_messages:
                            received_parents.update(parents)

                        # теперь выделяем из них тех, про которых мы еще не знали
                        self.new_parents = received_parents.difference(self.value)
                        if len(self.new_parents) > 0:
                            # появились новые предки
                            self.active = True
                            self.value.update(self.new_parents)

                if self.active:
                    # активная вершина рассылает всех своих новых предков по исходящим дугам
                    self.outgoing_messages = [(vertex,self.new_parents) for vertex in self.out_vertices]


if __name__ == "__main__":
        main(sys.argv[1])
