#encoding: utf-8
from pregel import Vertex,Pregel
from hw04util import *
import sys
vertices = {}
num_workers = 1
num_iterations = 50

def main(filename):
    global vertices
    global num_vertices
    # читаем граф из файла используя конструктор TransitiveClosure
    vertices = read_graph(filename, TransitiveClosure)

    num_vertices = len(vertices)

    transitive_closure_pregel(vertices.values())

def transitive_closure_pregel(vertices):
    p = Pregel(vertices, num_workers, num_iterations)
    p.run()
    #вывод рёбер транзитивного замыкания (начало и конец ребра)
    for vertex in p.vertices:
        for val in vertex.value:
            print "%s, %s" % (vertex.id, val)


class TransitiveClosure(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, [], [])

    def update(self):
        global vertices
        self.outgoing_messages = []
        if self.superstep > 0:
            for (vertex, value) in self.incoming_messages:
                #отсылаем рекомендации на добавление ребра предыдущим вершинам
                if value[1] == 'notification':
                    aim = 'recommendation'
                    self.outgoing_messages += [(value[0], [val, aim]) for val in self.out_vertices]
                #рассматриваем рекомендации  от предыдущих вершин на добавление ребра,если такого ребра у нас нет, то добавляем
                if value[1] == 'recommendation':
                    ind = True
                    if value[0] != self:
                        for v in self.out_vertices:
                            if value[0] == v:
                                ind = False
                                break
                        if ind == True:
                            self.out_vertices.append(value[0])
                            self.value.append(value[0].id)
        #отсылаем вершинам, к которым у нас есть путь,состоящий из одного ребра, уведомление, что мы можем до них дойти из данной вершины
        if len(self.out_vertices) > 0:
            aim = 'notification'
            self.outgoing_messages += [(vertex, [self, aim]) for vertex in self.out_vertices]
if __name__ == "__main__":
    main(sys.argv[1])
