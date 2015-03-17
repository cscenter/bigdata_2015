# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
max_supersteps = 100000
fiction_vertex = -1 #используется как заглушка когда мы не хотим добавлять ребро, а хотим просто гулять по графу

def main(filename):
  global vertices
  global num_vertices
  global fiction_vertex
  # читаем граф из файла, используя конструктор MaxValueVertex
  vertices = read_graph(filename, MrTransitiveClosure)
  fiction_vertex = MrTransitiveClosure(-1)  
  
  p = Pregel(vertices.values(),num_workers,max_supersteps)
  p.run()
  print 'Количество итераций: ' + str(p.superstep)
  

class MrTransitiveClosure(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, set())
      
    def doHaveEdgeToVertex(self, v):
        return (self.id == v.id) or (v in self.out_vertices)
      
    def update(self):
        global vertices
        # На нулевой итерации еще нет входящих
        addNeighborEdge = False
        addEdge = False
        if self.superstep > 0:
          # по умолчанию эта вершина станет пассивной
      #    self.active = False
          if len(self.incoming_messages) > 0:
            # то активизируемся            
    #        self.outgoing_messages = []
            for (vertex, v) in self.incoming_messages:
              #  print(v)
                if v[0].id != fiction_vertex.id:
           #         print(v.id)
                    if self.doHaveEdgeToVertex(v[0]) == False:
                        print(str(self.id) + ',' + str(v[0].id))
                        self.out_vertices.add((v[0]))
                        addEdge = True
                    #объяснить тем, что тип может быть намного меньше итераций и все равно выйграем по скорости
                    #потестить на тысяче вершинах
            for (vertex, v) in self.incoming_messages:
                if v[1] == True:
                    addNeighborEdge = True
                if v[0].id == fiction_vertex.id:
                    for e in self.out_vertices:
                  #      print e.id
 #                       if vertex.doHaveEdgeToVertex(e) == False: 
                        self.outgoing_messages.append((vertex, (e, addEdge))) #Здесь может не всегда нужно пересылатьь? 
        #else:           
        if self.superstep > 4:
            if addNeighborEdge == False:
                self.active = False
        if self.active:        
            for vertex in self.out_vertices:
                self.outgoing_messages.append((vertex, (fiction_vertex, addEdge)))

if __name__ == "__main__":
    main(sys.argv[1])
  