# encoding: utf-8

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
max_supersteps = 1000 # Более чем вероятно, что мы прервемся много раньше

def main(filename):
  global vertices
  global num_vertices
  # читаем граф из файла, используя конструктор MrTransitiveClosure
  vertices = read_graph(filename, MrTransitiveClosure)
  
  p = Pregel(vertices.values(),num_workers,max_supersteps)
  p.run()

  print 'Количество итераций: ' + str(p.superstep)
  

class MrTransitiveClosure(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, set())
      
    def update(self):
        global vertices
        addNeighborEdge = False # Добавил ли хоть один из соседей ребро
        weAddEdge = False # Добавили ли мы ребро
        if self.superstep > 0: 
          if len(self.incoming_messages) > 0:
            for (vertex, v) in self.incoming_messages:
                if v[0] == "add": # сначала выполняем операции по добавлению ребра, делаем это в отдельном порядке
                                  # чтобы узнать, а добавили
           #         print(v.id)
                    if v[1] - self.out_vertices != set():
                        weAddEdge = True
                    dif = v[1] - self.out_vertices
                    if self in dif:                    
                        dif.remove(self)
                    for a in dif:
                        print(str(self.id) + "," + str(a.id))
                    self.out_vertices |= v[1]    
               #     if self in self.out_vertices:
                #        self.out_vertices.remove(self)
         #           if self.doHaveEdgeToVertex(v[0]) == False:
          #              print(str(self.id) + ',' + str(v[0].id))
           #             self.out_vertices.add((v[0]))
            #            addEdge = True
                    #объяснить тем, что тип может быть намного меньше итераций и все равно выйграем по скорости
                    #потестить на тысяче вершинах
            for (vertex, v) in self.incoming_messages:
                if v[2] == True:
                    addNeighborEdge = True
                if v[0] == "walk":
           #         for e in self.out_vertices:
                  #      print e.id
 #                       if vertex.doHaveEdgeToVertex(e) == False: 
                        self.outgoing_messages.append((vertex, ("add", self.out_vertices, weAddEdge))) #Здесь может не всегда нужно пересылатьь? 

        if self.superstep > 4:
            if addNeighborEdge == False:
                self.active = False
        if self.active:        
            for vertex in self.out_vertices:
                self.outgoing_messages.append((vertex, ("walk", set(), weAddEdge)))

if __name__ == "__main__":
    main(sys.argv[1])
  