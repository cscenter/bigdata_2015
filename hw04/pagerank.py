"""pagerank.py illustrates how to use the pregel.py library, and tests
that the library works.

"""

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 2


def main(filename):
  global vertices
  global num_vertices
  vertices = read_graph(filename, PageRankVertex)
  num_vertices = len(vertices)
  for v in vertices.values():
    v.value = 1.0/num_vertices
  pagerank_pregel(vertices.values())

def pagerank_pregel(vertices):
    """Computes the pagerank vector associated to vertices, using
    Pregel."""
    p = Pregel(vertices,num_workers)
    p.run()
    for vertex in p.vertices:
      print "#%s: %s" % (vertex.id, vertex.value)
    print "Sum=%f" % sum(v.value for v in p.vertices)

class PageRankVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices
        if self.superstep < 50:
            if self.superstep > 0:
              self.value = 0.15 / num_vertices + 0.85*sum(
                  [pagerank for (vertex,pagerank) in self.incoming_messages])
            if len(self.out_vertices) == 0:
              outgoing_pagerank = self.value / num_vertices
              self.outgoing_messages = [(vertex,outgoing_pagerank) 
                                        for vertex in vertices.values()]              
            else:
              outgoing_pagerank = self.value / len(self.out_vertices)
              self.outgoing_messages = [(vertex,outgoing_pagerank) 
                                        for vertex in self.out_vertices]
        else:
            self.active = False

if __name__ == "__main__":
    main(sys.argv[1])
