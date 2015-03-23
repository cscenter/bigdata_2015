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


class RLEListRefImpl(RLEList):
    def __init__(self):
        self.impl = []

    def append(self, value):
        self.impl.append(value)

    def insert(self, index, value):
        self.impl.insert(index, value)

    def get(self, index):
        return self.impl[index]

    def iterator(self):
        return iter(self.impl)


def demo():
    list = RLEListRefImpl()
    list.append("foo")
    list.insert(0, "bar")
    print list.iterator().next()
    print list.get(1)


