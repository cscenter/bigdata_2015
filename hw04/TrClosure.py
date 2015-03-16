# encoding: utf-8
"""
Будем действовать по принципу водоворота. Нашли вершинку, в которую хотим добавить ребро. пропускаем ее по всем вершинкам по которым мы к ней шли,
ну и смежных с ними.

"""

from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
max_supersteps = 510

def main(filename):
  global vertices
  global num_vertices
  vertices = read_graph(filename, TrClosure)

  max_supersteps = len(vertices.values()) + 1
  p = Pregel(vertices.values(), num_workers, max_supersteps)
  p.run()
  print "Completed in %d supersteps" % p.superstep
  for vertex in p.vertices:
    print '#', vertex.id, ','.join(map(str, sorted([ver.id for ver in vertex.out_vertices])))

class TrClosure(Vertex):
	def __init__(self, id):
		Vertex.__init__(self, id, None, [])

	def update(self):

		if self.superstep == 0:
			self.outgoing_messages = [(v, self) for v in self.out_vertices]
		elif self.superstep == 1:
			self.ancestor = [v for (_, v) in self.incoming_messages]
			self.outgoing_messages = [(an, ver) for an in self.ancestor for ver in self.out_vertices]
		else:
			Fl = False
			if len(self.incoming_messages) > 0:
				for (_, in_vertex) in self.incoming_messages:
					if in_vertex not in self.out_vertices:
						self.out_vertices.add(in_vertex)
						Fl = True
						for an in self.ancestor:
							self.outgoing_messages.append((an, in_vertex))
			self.active = Fl	

if __name__ == "__main__":
    main(sys.argv[1])
