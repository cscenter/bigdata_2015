# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 18:11:24 2015

@author: andrey
"""

from pregel import Vertex, Pregel
from hw04util import *

vertices = {}
num_workers = 1
max_supersteps = 50

def main(filename):
  global vertices
  global num_vertices
  # читаем граф из файла, используя конструктор MaxValueVertex
  vertices = read_graph(filename, MrTransitiveClosure)
  
  p = Pregel(vertices.values(),num_workers,max_supersteps)
  p.run()
  
class MrTransitiveClosure(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])
      
    def update(self):
        EXPLORE_OPERATION = 0
        ADD_OPERATION = 1
        global vertices
        # На нулевой итерации еще нет входящих
        if self.superstep > 0:
          # по умолчанию эта вершина станет пассивной
      #    self.active = False
          if len(self.incoming_messages) > 0:
            # Если входящие сообщения есть то находим максимальное значение и если оно больше, чем свое,
            # то активизируемся
            if                 
                for vertex in self.incoming_messages:
                    for edge in vertex.out_vertices:
                        self.outgoing_messages.append(vertex, edge) #Здесь может не всегда нужно пересылатьь? 
                    #объяснить тем, что тип может быть намного меньше итераций и все равно выйграем по скорости
                    #потестить на тысяче вершинах

        if self.active:
          # Активная вершина рассылает себя по исходящим дугам
          self.outgoing_messages = [(vertex, fiction_vertex) for vertex in self.out_vertices]

if __name__ == "__main__":
    main(sys.argv[1])
  