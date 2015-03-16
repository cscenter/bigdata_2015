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
    print p.superstep


class TransitiveClosure(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        self.outgoing_messages = []
        #показатель, были ли приписаны новые рёбра к данной вершине
        is_changing = False
        #показатель, были ли приписаны новые рёбра к вершинам, которые соединены  с данной посредством одного ребра
        is_changing_next = False
        if self.superstep > 0:
            #рассматриваем рекомендации  от предыдущих вершин на добавление ребра,если такого ребра у нас нет, то добавляем
            for (vertex, value) in self.incoming_messages:
                if value[1] == 'recommendation':
                    ind = True
                    if value[0] != self:
                        for v in self.out_vertices:
                            if value[0] == v:
                                ind = False
                                break
                        if ind == True:
                            #добавляем ребро и меняем идентификатор is_changing, отмечая таким образом, что к данной вершине на данном шаге было приписано ребро
                            self.out_vertices.append(value[0])
                            is_changing = True
                            #вывод рёбер транзитивного замыкания (начало и конец ребра)
                            print "%s,%s" % (self.id, value[0].id)
                    #проверяем, было ли приписано ребро к вершине, которая соединена с данной посредством одного ребра
                    if value[2] == True:
                        is_changing_next = True
            for (vertex, value) in self.incoming_messages:
                #отсылаем рекомендации на добавление ребра предыдущим вершинам вместе с показателем is_changing,
                # который показывает, приписывали ли мы новые рёбра к текущей вершине на это итерации
                if value[1] == 'notification':
                    aim = 'recommendation'
                    self.outgoing_messages += [(value[0], [val, aim, is_changing]) for val in self.out_vertices]
            if not is_changing and not is_changing_next and self.superstep > 2:
                self.active = False
        #отсылаем вершинам, к которым у нас есть путь,состоящий из одного ребра, уведомление, что мы можем до них дойти из данной вершины
        if self.active:
            if len(self.out_vertices) > 0:
                aim = 'notification'
                self.outgoing_messages += [(vertex, [self, aim]) for vertex in self.out_vertices]
if __name__ == "__main__":
    main(sys.argv[1])