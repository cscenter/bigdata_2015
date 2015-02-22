#!/usr/bin/python
# encoding: utf8

# Файл содержит простейщую реализацию структуры данных - "Дерево интервалов".
# Структура данных позволяет находить список интервалов по точке её сожержащую.
# Время построения O(n*log(n)), время запроса O(log(n)) где n - количество интервалов.

class Interval:
    def __init__(self, name, l, r):
        if l > r:
            raise Exception("Left part must be less or equals right. left:%s, right:%s)" % (l, r))
        self.name = name
        self.l = l
        self.r = r


class Node:
    def __init__(self, center, intervals, left_node, right_node):
        self.left_node = left_node
        self.right_node = right_node
        self.center = center
        self.intervals = intervals


class IntervalTree:
    def __init__(self, intervals):
        intervals = self.sort_by_left(intervals)
        self.root = self.build_tree(intervals)

    def build_tree(self, intervals):
        if not intervals:
            return None

        center = self.get_center(intervals)
        left_out, with_center, right_out = self.divide_by_left(center, intervals)

        left_node = self.build_tree(left_out)
        right_node = self.build_tree(right_out)

        return Node(center, with_center, left_node, right_node)

    def search(self, point):
        return self.search_for_node(self.root, point)

    @staticmethod
    def search_for_node(node, point):
        if node is None:
            return []
        center = node.center.l
        if point == center:
            return node.intervals
        else:
            interval_with_point = filter((lambda x: x.l <= point <= x.r ), node.intervals)
            if point < center:
                return IntervalTree.search_for_node(node.left_node, point) + interval_with_point
            else:
                return interval_with_point + IntervalTree.search_for_node(node.right_node, point)

    @staticmethod
    def sort_by_left(intervals):
        return sorted(intervals, key=lambda item: item.l)

    @staticmethod
    def get_center(intervals):
        return intervals[len(intervals) / 2]

    @staticmethod
    def divide_by_left(center_item, intervals):
        center = center_item.l
        left_out = []
        with_center = []
        right_out = []
        for item in intervals:
            if item.r < center:
                left_out.append(item)
            elif item.l <= center:
                with_center.append(item)
            else:
                right_out.append(item)
        return left_out, with_center, right_out


def test():
    intervals = [Interval("1", 1, 3), Interval("2", 2, 4), Interval("3", 3, 5),
                 Interval("4", 4, 6), Interval("5", 8, 9), Interval("6", 8, 10),
                 Interval("7", 1, 10), Interval("100", 100, 100)]
    tree = IntervalTree(intervals)
    assert_equals(IntervalTree.search_for_node(tree.root, 0), [])
    assert_equals(IntervalTree.search_for_node(tree.root, 1), ["1", "7"])
    assert_equals(IntervalTree.search_for_node(tree.root, 2), ["1", "7", "2"])
    assert_equals(IntervalTree.search_for_node(tree.root, 3), ["1", "7", "2", "3"])
    assert_equals(IntervalTree.search_for_node(tree.root, 4), ["7", "2", "3", "4"])
    assert_equals(IntervalTree.search_for_node(tree.root, 5), ["7", "3", "4"])
    assert_equals(IntervalTree.search_for_node(tree.root, 6), ["7", "4"])
    assert_equals(IntervalTree.search_for_node(tree.root, 7), ["7"])
    assert_equals(IntervalTree.search_for_node(tree.root, 8), ["7", "5", "6"])
    assert_equals(IntervalTree.search_for_node(tree.root, 9), ["7", "5", "6"])
    assert_equals(IntervalTree.search_for_node(tree.root, 10), ["7", "6"])
    assert_equals(IntervalTree.search_for_node(tree.root, 11), [])
    assert_equals(IntervalTree.search_for_node(tree.root, 99), [])
    assert_equals(IntervalTree.search_for_node(tree.root, 100), ["100"])
    assert_equals(IntervalTree.search_for_node(tree.root, 101), [])


def assert_equals(actual, expected):
    actual = map((lambda x: x.name), actual)
    if actual != expected:
        raise Exception("Expected [%s] but found [%s]" % (",".join(actual), ",".join(expected)))

#
# def to_str_names(intervals):
# names = ""
# for it in intervals:
# names = names + " " + it.name
# return names


test()