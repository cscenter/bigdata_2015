# encoding: utf-8
"""maxvalue.py демнострирует работу алгоритма, прекращающегося не по условию 
достижения ограничения на число итераций

Алгоритм находит для каждой вершины V графа G максимальное значение среди вершин,
из которых есть направленный путь в V. Если граф G сильно связанный, то в каждой
вершине будет записано максимальное значение во всём графе

"""

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
  # читаем граф из файла, используя конструктор MaxValueVertex
  vertices = read_graph(filename, MaxValueVertex)

  # Заполняем случайными значениями
  for v in vertices.values():
    v.value = randint(1, len(vertices) * 2)

  # Запускаем подсчет, ограничивая количеством итераций
  p = Pregel(vertices.values(),num_workers,max_supersteps)
  p.run()
  print "Completed in %d supersteps" % p.superstep
  for vertex in p.vertices:
    print "#%s: %s" % (vertex.id, vertex.value)

class MaxValueVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        # На нулевой итерации еще нет входящих
        if self.superstep > 0:
          # по умолчанию эта вершина станет пассивной
          self.active = False
          if len(self.incoming_messages) > 0:
            # Если входящие сообщения есть то находим максимальное значение и если оно больше, чем свое,
            # то активизируемся
            max_incoming = max(value for (_, value) in self.incoming_messages)
            if max_incoming > self.value:
              self.active = True
              self.value = max_incoming

        if self.active:
          # Активная вершина рассылает свое значение по исходящим дугам
          self.outgoing_messages = [(vertex,self.value) for vertex in self.out_vertices]

if __name__ == "__main__":
    main(sys.argv[1])
