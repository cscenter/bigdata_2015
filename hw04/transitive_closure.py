# encoding: utf-8
"""transitive_closure.py
    Использует следующий алгоритм для построения транзитивного замыкания графа (вариант алгоритма Уоршелла):
    Каждая вершина старается добавить себе в список вершины соседей, до которых можно добраться из этой вершины.
    На первом шаге текущая обрабатываемая вершина посылает запрос в соседние с ней, спрашивая о соседях
    (потенциально достижимых), до которых тем самым можно добраться из начальной вершины.
    Список достижимых вершин сохраняется и на следующем шаге выбираются для опроса уже только новые вершины из списка
    потенциально достижимых у которых ещё не спрашивали. Алгоритм завершается в случае когда новые вершины перестают
    добавляется.
"""

from pregel import Vertex, Pregel
from hw04util import *
from random import randint
import sys

num_workers = 1


def main(filename):
    # читаем граф из файла, используя конструктор MaxValueVertex
    vertices = read_graph(filename, MaxValueVertex)

    # Запускаем подсчет, ограничивая количеством итераций на максимальное количество вершин, это худший сценарий.
    p = Pregel(vertices.values(), num_workers, len(vertices))
    p.run()
    print "Completed in %d supersteps" % p.superstep
    # Перечень дуг
    print "Minimal edge set:"
    empty_fl = True
    for vertex in p.vertices:
        for v in vertex.get_answer():
            empty_fl = False
            print "(%s,%s)" % (vertex.id, v)
    if empty_fl:
        print "empty"


class MaxValueVertex(Vertex):
    def __init__(self, id):
        Vertex.__init__(self, id, None, [])
        self.handled_vertex_ids = set()
        self.answers = []

    def update(self):

        if self.superstep == 0:
            self.ask_about_neighbours(self.out_vertices)
        else:
            if len(self.incoming_messages) > 0:
                self.start_collecting_answers()
                found_new_neighbours = False
                for _, msg in self.incoming_messages:
                    # Отвечаем на вопрос о соседях вершины
                    if self.is_question(msg):
                        _, requester = msg
                        self.tell_about_neighbours(requester)
                    # Нарашиваем потенциальные вершины из ответов
                    elif self.is_answer(msg):
                        _, neighbour_vertices = msg
                        new_vertices = self.extract_only_new(neighbour_vertices)
                        if len(new_vertices) > 0:
                            # Если есть что то новое то нужно узнать и об их соседях
                            self.ask_about_neighbours(new_vertices)
                            self.mark_as_handled(new_vertices)
                            found_new_neighbours = True

                    else:
                        raise Exception("Unexpected msg type: %s" % msg[0])
                self.send_answers()
                # Деактивируем если больше нет новых вершин
                if not found_new_neighbours:
                    self.active = False
            else:
                # Деактивируем если больше никто ни о чем не спрашивает
                self.active = False

    def mark_as_handled(self, vertices):
        self.handled_vertex_ids.union((vertex.id for vertex in vertices))

    def extract_only_new(self, neighbour_vertices):
        new_vertices = []
        for vertex in neighbour_vertices:
            if vertex.id not in self.handled_vertex_ids:
                new_vertices.append(vertex)
        return new_vertices

    def ask_about_neighbours(self, vertices):
        remote_vertex_requests = []
        for vertex in vertices:
            self.handled_vertex_ids.add(vertex.id)
            if vertex.id != self.id:
                remote_vertex_requests.append((vertex, ("Q", self)))
        self.outgoing_messages = remote_vertex_requests

    def get_answer(self):
        return self.handled_vertex_ids.difference((vertex.id for vertex in self.out_vertices)).difference([self.id])

    def tell_about_neighbours(self, requester):
        self.answers.append((requester, ("A", self.out_vertices)))

    def start_collecting_answers(self):
        self.answers = []

    def send_answers(self):
        if len(self.answers) > 0:
            self.outgoing_messages = self.answers

    @staticmethod
    def is_question(msg):
        return msg[0] == "Q"

    @staticmethod
    def is_answer(msg):
        return msg[0] == "A"


if __name__ == "__main__":
    main(sys.argv[1])
