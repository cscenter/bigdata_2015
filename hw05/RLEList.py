# encoding: utf-8

import copy
import pprint

class RLEList(object):
    def __init__(self):
        pass

    def append(self, value):
        pass

    def insert(self, index, value):
        pass

    def get(self, index):
        pass

    def iterator(self):
        pass


class Record:
    def __init__(self, start_idx = 0, count = 0, value = None):
        self.start_idx = start_idx
        self.count = count
        self.value = value

    def end_idx(self):
        return self.start_idx + self.count - 1

    def __repr__(self):
        return "<%s instance at %s start_idx: %i, count: %i, value: %s>" % (self.__class__.__name__, hex(id(self)), self.start_idx, self.count, self.value)

    def __str__(self):
        return "start_idx: %i, count: %i, value: %s" % (self.start_idx, self.count, self.value)


class RLEListRefImpl(RLEList):
    """
    Реализация на листе с бинпоиском и пересчетом индексов
    """
    def __init__(self):
        self.impl = []

    def append(self, value):
        if len(self.impl) == 0:
            self.impl.append(Record(0, 1, value))
            return
        last_item = self.impl[-1]
        if last_item.value == value:
            last_item.count += 1
            self.impl[-1] = last_item
        else:
            next_idx = last_item.end_idx() + 1
            self.impl.append(Record(next_idx, 1, value))

    def increase_nexts(self, start_from):
        for i in xrange(start_from, len(self.impl)):
            self.impl[i].start_idx += 1

    def insert(self, index, value):
        if index > self.impl[-1].end_idx():
            # если index находится за пределами, то просто добавляем в конец
            self.append(value)
            return
        lb = 0
        rb = len(self.impl) - 1
        m = len(self.impl)
        while lb <= rb:
            m = (lb + rb) / 2
            if self.impl[m].start_idx < index and self.impl[m].end_idx() >= index:
                # Попали в промежуток (начало, конец]
                if self.impl[m].value == value:
                    self.impl[m].count += 1
                    self.increase_nexts(m + 1)
                    return
                else:
                    # split
                    item_before = copy.copy(self.impl[m])
                    self.impl[m].count = index - item_before.start_idx
                    self.impl.insert(m + 1, Record(index, 1, value))
                    self.impl.insert(m + 2, Record(index + 1, item_before.end_idx() + 1 - index, item_before.value))
                    self.increase_nexts(m + 3)
                    return
            elif self.impl[m].start_idx == index:
                # попали в начало диапазона
                if self.impl[m].value == value:
                    self.impl[m].count += 1
                    self.increase_nexts(m + 1)
                    return
                else:
                    self.impl.insert(m, Record(index, 1, value))
                    self.increase_nexts(m + 1)
                    return
            elif self.impl[m].start_idx > index:
                rb = m - 1
            else:
                lb = m + 1

    def get(self, index):
        lb = 0
        rb = len(self.impl) - 1
        while lb <= rb:
            m = (lb + rb) / 2
            if self.impl[m].start_idx <= index and self.impl[m].end_idx() >= index:
                return self.impl[m].value
            elif self.impl[m].start_idx > index:
                rb = m - 1
            else:
                lb = m + 1
        return None

    def iterator(self):
        return iter(self)

    def __iter__(self):
        self.last = 0
        self.sub_last = 0
        return self

    def next(self):
        if self.last == len(self.impl):
            raise StopIteration
        else:
            i = self.last
            j = self.sub_last
            self.sub_last += 1
            if (self.sub_last >= self.impl[i].count):
                self.last += 1
                self.sub_last = 0
            return self.impl[i].value


def demo():
    pp = pprint.PrettyPrinter(indent=2)

    list = RLEListRefImpl()
    list.append("foo")
    list.append("foo")
    list.append("foo")
    list.append("bar")
    list.append("ark")
    list.insert(2, "barman")
    print("after inserting barman at 2")
    pp.pprint(list.impl)
    list.insert(0, "cow")
    print("after inserting cow at 0")
    pp.pprint(list.impl)
    # print list.get(0)
    # print list.get(1)
    # print list.get(2)
    # print list.get(3)

    for v in list:
        print (v)

if __name__ == "__main__":
    demo()