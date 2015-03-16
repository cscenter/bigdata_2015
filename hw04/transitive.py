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
  
    # читаем граф из файла, используя конструктор TransitiveVertex
    vertices = read_graph(filename, TransitiveVertex)

    # Запускаем подсчет, ограничивая количеством итераций
    p = Pregel(vertices.values(),num_workers,max_supersteps)
    p.run()
    print "Completed in %d supersteps" % p.superstep
    for vertex in p.vertices:
        children = ", ".join(str(child.id) for child in vertex.out_vertices)
        if not children:
            children = "=="
        print "#%s: %s" % (vertex.id, children)

class TransitiveVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices

        # Спокойно засыпаем - если придёт запрос о детях, или придёт ответ на наш предыдущий запрос - нас разбудят
        self.active = False

        # Шаг 0: Спрашиваем детей об их детях
        if self.superstep == 0:
            self.outgoing_messages = [(child, None) for child in self.out_vertices]

        # Последующие шаги: получаем информацию о "внуках", "усыновляем" их и тут же спрашиваем их об их детях
        else:
            for (sender, request) in self.incoming_messages:
                if request is not None:
                    if request not in self.out_vertices:
                        self.out_vertices.append(request)
                        self.outgoing_messages += [(request, None)]
                else:
                    for child in self.out_vertices:
                        if sender != child:
                            self.outgoing_messages += [(sender, child)]

               

if __name__ == "__main__":
    main(sys.argv[1])
