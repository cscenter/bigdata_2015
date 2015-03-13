# encoding: utf-8
"""pagerank.py illustrates how to use the pregel.py library, and tests
that the library works.

"""

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

  # Инициализируем начальное распределение pagerank
  num_vertices = len(vertices)
  for v in vertices.values():
    v.value = 1.0/num_vertices

  # Запускаем подсчет
  pagerank_pregel(vertices.values())

def pagerank_pregel(vertices):
    """Computes the pagerank vector associated to vertices, using
    Pregel."""
    p = Pregel(vertices,num_workers,num_iterations)
    p.run()
    for vertex in p.vertices:
      print "#%s: %s" % (vertex.id, vertex.value)
    print "Sum=%f" % sum(v.value for v in p.vertices)

class PageRankVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        # На нулевой итерации у нас еще нет никаких входящих сообщений, поэтому мы не меняем PR вершины
        if self.superstep > 0:
          # Вероятность перехода с любой страницы + вероятность перехода со ссылающихся страниц
          self.value = 0.15 / num_vertices + 0.85*sum(
              [pagerank for (vertex,pagerank) in self.incoming_messages])
        if len(self.out_vertices) == 0:
          # тупиковая страница рассылает часть своего PR всем
          outgoing_pagerank = self.value / num_vertices
          self.outgoing_messages = [(vertex,outgoing_pagerank) 
                                    for vertex in vertices.values()]              
        else:
          # нетупиковая только тем, на кого ссылается
          outgoing_pagerank = self.value / len(self.out_vertices)
          self.outgoing_messages = [(vertex,outgoing_pagerank) 
                                    for vertex in self.out_vertices]

if __name__ == "__main__":
    main(sys.argv[1])
