# encoding: utf-8
from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
num_iterations = 3


def main(filename):
    global vertices
    global num_vertices

    vertices = read_graph(filename, TransitiveClosureVertex)
    transitive_closure_pregel(vertices.values())


def transitive_closure_pregel(vertices):
    """
    Computes transitive closure for given graph
    """
    p = Pregel(vertices, num_workers, num_iterations)
    p.run()

    # write transitive closure of graph in standard output
    for vertex in p.vertices:
        print "#%s: " % vertex.id
        for to_vertex in vertex.out_vertices:
            print "%s " % to_vertex.id


class TransitiveClosureVertex(Vertex):
    def __init__(self, id):
      Vertex.__init__(self, id, None, [])

    def update(self):
        global vertices

        # 1) tell all out vertexes about in vertexes
        if self.superstep == 0:
            self.outgoing_messages = [(vertex, self) for vertex in self.out_vertices]

        # 2) compose pairs to be united by edge
        elif self.superstep == 1:
            out_list = []
            for (this_vertex, in_vertex) in self.incoming_messages:
                for out_vertex in self.out_vertices:
                    out_list.append((in_vertex, out_vertex))

            self.outgoing_messages = out_list

        # 3) add missing edges
        elif self.superstep == 2:
            for (this_vertex, to_vertex) in self.incoming_messages:
                if to_vertex not in self.out_vertices and self != to_vertex:
                    self.out_vertices.append(to_vertex)

if __name__ == "__main__":
    main(sys.argv[1])