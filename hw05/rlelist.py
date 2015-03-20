import itertools
import threading


class RLEList:

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


class RLEListTrueImpl(RLEList):

    def __init__(self):
        self.impl = []
        self.impl_lock = threading.Lock()

    def append(self, value):
        with self.impl_lock:
            if self.impl and self.impl[-1].value == value:
                self.impl[-1].count += 1
            else:
                self.impl.append(self.Elem(value))

    def insert(self, index, value):
        with self.impl_lock:
            real_index = 0
            for i, elem in enumerate(self.impl):
                if index >= real_index and index < real_index + elem.count:
                    if elem.value == value:
                        elem.increase()
                    else:
                        self.impl.insert(i, self.Elem(value))
                    return
                real_index += elem.count

    def get(self, index):
        with self.impl_lock:
            return next(itertools.islice(self.iterator(), index, None))

    def iterator(self):
        for elem in self.impl:
            for _ in range(elem.count):
                yield elem.value

    class Elem:

        def __init__(self, value):
            self.value = value
            self.count = 1

        def __str__(self):
            return 'Elem(%s, %d)' % (self.value, self.count)

        def increase(self):
            self.count += 1
