# encoding: utf-8
""".py
"""

from pregel import Vertex, Pregel
from hw04util import *
from random import randint
import sys

vertices = {}
num_workers = 1
max_supersteps = 50


def main(filename):
    global vertices
    # читаем граф из файла, используя конструктор MaxValueVertex
    vertices = read_graph(filename, MaxValueVertex)

    # Запускаем подсчет, ограничивая количеством итераций
    p = Pregel(vertices.values(), num_workers, max_supersteps)
    p.run()
    print "Completed in %d supersteps" % p.superstep
    print "Minimal edge set:"
    for vertex in p.vertices:
        for v in vertex.get_answer():
            print "(%s,%s)" % (vertex.id, v)


class MaxValueVertex(Vertex):
    accumulator = set()
    handled_vertices = set()
    new_vertices= set()

    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def get_answer(self):
        return self.accumulator.difference((vertex.id for vertex in self.out_vertices)).difference([self.id])

    def update(self):
        global vertices

        if self.superstep == 0:
            remote_vertex_requests = []
            for out_vertex in self.out_vertices:
                self.handled_vertices.add(out_vertex)
                if out_vertex != self.id:
                    remote_vertex_requests.append((out_vertex, ("Q", self)))
            self.outgoing_messages = remote_vertex_requests
            self.value = {}
        else:
            answers = []
            if len(self.incoming_messages) > 0:
                for _, msg in self.incoming_messages:
                    if msg[0] == "Q":
                        _, requester = msg
                        answers.append((requester, ("A", (vertex.id for vertex in self.out_vertices))))
                    elif msg[0] == "A":
                        _, neighbour_vertices = msg
                        self.accumulator = self.accumulator.union(neighbour_vertices)
                    else:
                        raise Exception("Unexpected msg type: %s" % msg[0])
                if len(answers) > 0:
                    self.outgoing_messages = answers
            else:
                self.active = False


if __name__ == "__main__":
    main(sys.argv[1])
