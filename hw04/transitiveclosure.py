# encoding: utf-8

import sys

from pregel import Vertex, Pregel
from hw04util import *
import logging

logging.basicConfig(level=logging.INFO)

vertices = {}
num_workers = 1
num_iterations = 50


def main(filename):
    global vertices
    vertices = read_graph(filename, PageRankVertex)
    transitiveclosure_pregel(vertices.values())


def transitiveclosure_pregel(vertices):
    p = Pregel(vertices, num_workers, num_iterations)
    p.run()
    for vertex in p.vertices:
        vertices = sorted([v.id for v in vertex.out_vertices])
        if len(vertices) > 0:
            print vertex.id, ','.join(map(str, vertices))
        else:
            print vertex.id, "=="


class PageRankVertex(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])

    def update(self):
	#todo: remove redundant edges
        global vertices
        self.outgoing_messages = []

        if self.superstep == 0:
            self.outgoing_messages = [(v, "propagate") for v in self.out_vertices]
        else:
            for (sender, event_args) in self.incoming_messages:
                if event_args == "propagate":
                    # event_args is message type
                    # "propagate" means tunnel this event with sender's descendant
                    self.outgoing_messages.extend([(sender, v) for v in self.out_vertices if sender.id != v.id])
                else:
                    # event_args is vertex
                    if event_args not in self.out_vertices:
                        #logging.info(str(self.id) + " -> " + str(event_args.id))
                        self.out_vertices.append(event_args)
                        self.outgoing_messages += [(event_args, "propagate")]

if __name__ == "__main__":
    main(sys.argv[1])
