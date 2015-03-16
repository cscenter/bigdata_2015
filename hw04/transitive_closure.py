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
    for vertex in p.vertices:
        print "#%s: %s" % (vertex.id, ' '.join([str(v.id) for v in vertex.out_vertices]))


class TransitiveClosureVertex(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])
        self.new_out_vertices = set()

    def __hash__(self):
        return self.id

    def update(self):
        if self.id % 100 == 0:
            # Makes waiting much less boring
            print 'processing {}th superstep for {}th vertex'.format(self.superstep, self.id)
        # Superstep consists of two step
        # In a first step vertex sends itself to all of its new direct successors
        # In a second step vertex adds new edges which leads from its direct predecessors to its direct successors
        if self.superstep % 2 == 0:
            self.active = False
            if self.new_out_vertices:
                self.active = True
                self.out_vertices |= self.new_out_vertices
                self.outgoing_messages = [(vertex, self) for vertex in self.new_out_vertices]
                self.new_out_vertices.clear()
        else:
            if self.incoming_messages:
                for _, in_vertex in self.incoming_messages:
                    for out_vertex in self.out_vertices - in_vertex.out_vertices:
                        in_vertex.new_out_vertices.add(out_vertex)

if __name__ == "__main__":
    main(sys.argv[1])
