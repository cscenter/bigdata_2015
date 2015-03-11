__author__ = 'phil'
# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
from random import randint
import sys
import time

vertices = {}
num_workers = 1
max_supersteps = 50

def main(filename):
  global vertices
  global num_vertices
  # читаем граф из файла, используя конструктор MaxValueVertex
  vertices = read_graph(filename, Transitive_Vertex)

  # Заполняем случайными значениями
  for v in vertices.values():
    v.value = randint(1, len(vertices) * 2)

  # Запускаем подсчет, ограничивая количеством итераций
  start = time.time()
  p = Pregel(vertices.values(),num_workers,max_supersteps)
  p.run()
  print "Completed in %d supersteps" % p.superstep
  finish = time.time()
  # print(finish - start)
  for vertex in p.vertices:
      print vertex.id, sorted([out_vertex.id for out_vertex in vertex.out_vertices])

class Transitive_Vertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        # print self.id
        global vertices
        #на нулевом шаге "звоним" достижимым , тем самым "подписываясь" на их обновления
        if self.superstep == 0:
            self.active = True
            self.outgoing_messages = [(vertex, self) for vertex in self.out_vertices]

        #на первом шаге запоминаем наших подписчиков, а так же отсылаем свои достижимые вершины
        if self.superstep == 1:
            self.active = True
            self.subscribers = [subscriber for (_, subscriber) in self.incoming_messages]
            self.outgoing_messages = [(subscriber, vertex) for subscriber in self.subscribers for vertex in self.out_vertices]

        #на последующих шагах мы добавялем новые достижимые вершины, и отсылаем их своим подписчикам
        #активны, пока появляются новые достижимые вершины, которыми и делимся с подписчиками
        if self.superstep > 1:
            self.active = False
            if len(self.incoming_messages) > 0:
                new_friends = []
                for (_, maybe_new) in self.incoming_messages:
                    if maybe_new not in self.out_vertices:
                        new_friends.append(maybe_new)
                        # self.out_vertices.append(maybe_new)
                        self.out_vertices.add(maybe_new) #оптимизация, допустимо если только одно ребро ведёт из вершины в вершину

                if len(new_friends) > 0:
                    self.active = True
                    self.outgoing_messages = [(subscriber, friend) for subscriber in self.subscribers for friend in new_friends]

                else:
                    self.active = False


if __name__ == "__main__":
    main(sys.argv[1])