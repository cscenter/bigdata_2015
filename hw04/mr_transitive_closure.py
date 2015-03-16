# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
max_supersteps = 50
fiction_vertex = -1

def main(filename):
  global vertices
  global num_vertices
  global fiction_vertex
  # читаем граф из файла, используя конструктор MaxValueVertex
  vertices = read_graph(filename, MrTransitiveClosure)
  fiction_vertex = MrTransitiveClosure(-1)  
  
  p = Pregel(vertices.values(),num_workers,max_supersteps)
  p.run()
  

class MrTransitiveClosure(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])
      
    def update(self):
        global vertices
        # На нулевой итерации еще нет входящих
        if self.superstep > 0:
          # по умолчанию эта вершина станет пассивной
      #    self.active = False
          if len(self.incoming_messages) > 0:
            # Если входящие сообщения есть то находим максимальное значение и если оно больше, чем свое,
            # то активизируемся            
    #        self.outgoing_messages = []
            for (vertex, v) in self.incoming_messages:
              #  print(v)
                if v == fiction_vertex:
                    for e in self.out_vertices:
                  #      print e.id
                        self.outgoing_messages.append((vertex, e)) #Здесь может не всегда нужно пересылатьь? 
                else:
           #         print(v.id)
                    if v not in self.out_vertices and v.id != self.id:
                        print(str(self.id) + ',' + str(v.id))
                        self.out_vertices.append((v))
                    #объяснить тем, что тип может быть намного меньше итераций и все равно выйграем по скорости
                    #потестить на тысяче вершинах
        #else:            
        for vertex in self.out_vertices:
            self.outgoing_messages.append((vertex, fiction_vertex))

if __name__ == "__main__":
    main(sys.argv[1])
  