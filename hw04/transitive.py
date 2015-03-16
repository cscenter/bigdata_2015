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

        # Спокойно засыпаем - если надо, нас разбудят
        self.active = False

        ### В python-е нет switch. Я страдаю и терзаюсь над каждым последующим if-elif.

        # Шаг 1: Спрашиваем "детей" о "внуках"
        if self.superstep % 3 == 0:
            self.outgoing_messages = [(child, self) for child in self.out_vertices]

        # Шаг 2: Рассказываем "родителям" о своих "детях", т.е. об их "внуках"
        elif self.superstep % 3 == 1:
            for (_this, father) in self.incoming_messages:
                for child in self.out_vertices:
                    # Проверяем на сложные семейные ситуации (необходимо для ненаправленных графов)
                    if father != child:
                        self.outgoing_messages += [(father, child)]

        # Шаг 3: "Усыновляем" своих "внуков", если таковые имеются и остаёмся активными,
        # чтобы спросить своих новых детей уже об их детях.
        elif self.superstep % 3 == 2:
            for (_this, grandchild) in self.incoming_messages:
                if grandchild not in self.out_vertices:
                    self.out_vertices.append(grandchild)
                    self.active = True # чтобы финальный граф был весь транзитивен
                

if __name__ == "__main__":
    main(sys.argv[1])
