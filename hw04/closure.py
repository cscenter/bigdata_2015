# encoding: utf-8


from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
num_iterations = 50

def main(filename):
  global vertices
  global num_vertices
  # читаем граф из файла, используя конструкток PageRankVertex
  vertices = read_graph(filename, PageRankVertex)

  # Инициализируем начальное значения списков верши
  num_vertices = len(vertices)
  for v in vertices.values():
    v.value = set([v.id])

  # Запускаем подсчет
  closure_pregel(vertices.values())

def closure_pregel(vertices):
    """Computes the pagerank vector associated to vertices, using
    Pregel."""
    p = Pregel(vertices,num_workers,num_iterations)
    p.run()
    #Вес ребер не учитываем так как он может быть произовольным
    print("Edges to be added:\n")
    print(p.vertices)
    for vertex in p.vertices:
      for v in list(vertex.value):
          if v != vertex.id and vertex not in list(p.vertices)[v].out_vertices:
            print("{} -> {}".format(v, vertex.id))

class PageRankVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        self.active = True
        # На нулевой итерации у нас еще нет никаких входящих сообщений, поэтому мы не меняем PR вершины
        if self.superstep > 0:
          # получаем список всех вершин, для которых данная является потомком
          for _, inc in self.incoming_messages:
              if inc.issubset(self.value):
                  self.active = False
              else:
                  self.active = True
                  self.value = set(self.value.union(inc))

        if len(self.out_vertices):
          #если вершина не тупиковая - она пересылает значения своим потомкам
          outgoing_pages = self.value
          self.outgoing_messages = [(vertex,outgoing_pages)
                                    for vertex in self.out_vertices]




if __name__ == "__main__":
    main(sys.argv[1])
