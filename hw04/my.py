from random import randint
from pregel import Vertex, Pregel
from hw04util import *
import sys

vertices = {}
num_workers = 1
num_iterations = 50


def main(filename):
    global vertices
    global num_vertices

    vertices = read_graph(filename, Mylnikov)


    p = Pregel(vertices.values(), num_workers, num_iterations)
    p.run()
    for vertex in p.vertices:
        out_vertices = ",".join(str(vertex.id) for vertex in vertex.out_vertices)
        if not out_vertices:
            out_vertices = "=="
        print ("{}: {} {}".format(vertex.id, out_vertices, vertex.value))


class Mylnikov(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def update(self):

        global vertices
        if self.superstep == 0:
            self.outgoing_messages = [(v, self) for v in self.out_vertices]
        if self.superstep == 1:
            self.intro = [intro for (tmp, intro) in
                                self.incoming_messages]
            self.outgoing_messages = [(intro, v) for intro in
                                      self.intro for v in
                                      self.out_vertices]
        if self.superstep > 1:
            # print(self.active)
            self.active = False
            if len(self.incoming_messages) > 0:
                n_vertexes = []
                for (gent, income_vert) in self.incoming_messages:
                    if income_vert not in self.out_vertices:
                        self.out_vertices.append(income_vert)
                        n_vertexes.append(income_vert)
                # print (n_vertexes)
                if len(n_vertexes) > 0:
                    self.active = True
                    self.outgoing_messages = [(intro, linked) for
                                              intro in self.intro
                                              for linked in
                                              n_vertexes]

if __name__ == "__main__":
    # main("small_graph.txt")
    #main("random_1000.txt")
    main(sys.argv[1])
