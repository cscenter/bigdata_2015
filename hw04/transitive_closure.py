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
    # читаем граф из файлп используя конструктор TransitiveClosure
    vertices = read_graph(filename, TransitiveClosure)

    num_vertices = len(vertices)

    transitive_closure_pregel(vertices.values())

def transitive_closure_pregel(vertices):
    p = Pregel(vertices, num_workers, num_iterations)
    p.run()
    for vertex in p.vertices:
        for val in vertex.value:
            print "%s, %s" % (vertex.id, val)


class TransitiveClosure(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, [], [])
        self.value_notification = []
        self.value_recommendation = []

    def update(self):
        global vertices
        self.outgoing_messages = []
        if self.superstep > 0:
            for (vertex, value) in self.incoming_messages:
                if value[1] == 'notification':
                    self.value_notification.append((vertex, value[0]))
                if value[1] == 'recommendation':
                    self.value_recommendation.append((vertex,value[0]))
            #отсылаем рекомендации предыдущим вершинам
            for note in self.value_notification:
                aim = 'recommendation'
                self.outgoing_messages += [(note[1], [val, aim]) for val in self.out_vertices]
            for rec in self.value_recommendation:
                ind = True
                if rec[1] != self:
                    for v in self.out_vertices:
                        if rec[1] == v:
                            ind = False
                            break
                    if ind == True:
                        #добавили новое ребро в граф
                        self.out_vertices.append(rec[1])
                        #здесь надо ещё реализовать его вывод(можно воспользоваться self.value)
                        self.value.append(rec[1].id)
        if len(self.out_vertices) > 0:
            aim = 'notification'
            self.outgoing_messages += [(vertex, [self, aim]) for vertex in self.out_vertices]
if __name__ == "__main__":
    main(sys.argv[1])

