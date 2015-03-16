# encoding: utf-8

import sys
import time

from pregel import Vertex, Pregel
from hw04util import *


vertices = {}
num_workers = 1
max_supersteps = 50


def main(filename):
    global vertices
    vertices = read_graph(filename, TransitiveClosureVertex)

    start = time.time()
    p = Pregel(vertices.values(), num_workers, max_supersteps)
    p.run()
    end = time.time()
    print end - start
    print "Completed in %d supersteps" % p.superstep
    # for vertex in p.vertices:
        # print "#%s: %s" % (vertex.id, ' '.join([str(v.id) for v in vertex.out_vertices]))
        # print "#%s: %s" % (vertex.id)


class TransitiveClosureVertex(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])
        self.new_out_vertices = set()

    def update(self):
        # Considering vertex u, let's call vertex v a 'distant_vertex' if it connected to u by two consecutive edges
        # Superstep consists of two step
        # In a first step vertex sends itself to all of its distant vertices
        if self.superstep % 2 == 0:
            self.active = False
            if self.new_out_vertices:
                self.active = True
                self.out_vertices |= self.new_out_vertices
                self.outgoing_messages = [(distant_vertex, self)
                                          for vertex in self.new_out_vertices
                                          for distant_vertex in vertex.new_out_vertices | vertex.out_vertices - self.out_vertices]
                self.new_out_vertices.clear()

        # In a second step vertex connects itself to all vertices to which it is a distant vertex
        else:
            for _, new_distant_vertex in self.incoming_messages:
                if self not in new_distant_vertex.out_vertices:
                    new_distant_vertex.new_out_vertices.add(self)

if __name__ == "__main__":
    main(sys.argv[1])
