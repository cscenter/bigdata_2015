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
        addNeighborEdge = False # добавил ли хоть один из соседей ребро
        weAddEdge = False # добавили ли мы ребро
        if self.superstep > 0: 
          if len(self.incoming_messages) > 0:
            for (vertex, v) in self.incoming_messages:
                if v[0] == "add": # сначала выполняем операции по добавлению ребра, делаем это в отдельном порядке
                                  # чтобы узнать, а добавили ли мы хоть одно ребро, чтобы далее оповестить об этом
                                  # наших предков
                    if v[1] - self.out_vertices != set(): # если есть что добавлять
                        weAddEdge = True 
                    dif = v[1] - self.out_vertices
                    if self in dif:   # петли нам не нужны                 
                        dif.remove(self)
                    for a in dif:
                        print(str(self.id) + "," + str(a.id)) # вывожу ребра, ктороые будут добавлены
                    self.out_vertices |= v[1] # update   

            for (vertex, v) in self.incoming_messages: # второй забег, теперь мы точно знаем добавляли ли мы ребра 
                                                       # т.е. weAddEdge определен и уже не изменится 
                if v[2] == True: # добавил ли хоть один наш сосед ребро
                    addNeighborEdge = True
                if v[0] == "walk":
                    self.outgoing_messages.append((vertex, ("add", self.out_vertices, weAddEdge))) 

        # добавление ребер может породить возможности для появления новых ребер
        # однако если наши соседи не добавили ни одного ребра, и при этом мы сами не можем добавить ребер
        # то делаем вершину неактивной
        if self.superstep > 4:
            if addNeighborEdge == False:
                self.active = False
        if self.active:        
            for vertex in self.out_vertices:
                self.outgoing_messages.append((vertex, ("walk", set(), weAddEdge)))

if __name__ == "__main__":
    main(sys.argv[1])
  