# encoding: utf-8

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
    """Реализация на листе расширена методами сжатия
    и восстановления значений и реализацией итератора
    """
    def __init__(self):
        self.impl = []

    def compress(self, value):
        """value будем хранить в виде массива 
        таплов (элемент, кол-во повторов)
        """
        if value is None or len(value) == 0:
            return None
        if len(value) == 1:
            return (value, 1)
        res = []
        counter = 1
        value_size = len(value)
        for i in xrange(1, value_size):
            if value[i] == value[i - 1]:
                counter += 1
            else:
                res.append((value[i - 1], counter))
                counter = 1
            # последний элемент записываем без сравнения
            if i == value_size - 1:
                res.append((value[i], counter))
        return res

    def decompress(self, value):
        res = ""
        for (c, count) in value:
            res += c * count
        return res

    def append(self, value):
        self.impl.append(self.compress(value))

    def insert(self, index, value):
        self.impl.insert(index, self.compress(value))

    def get(self, index):
        return self.decompress(self.impl[index])

    def iterator(self):
        return iter(self)

    def __iter__(self):
        self.last = 0
        return self

    def next(self):
        if self.last == len(self.impl):
            raise StopIteration
        else:
            i = self.last
            self.last += 1
            return self.decompress(self.impl[i])


def demo():
    list = RLEListRefImpl()
    list.append("foo")
    list.insert(0, "bar")
    list.append("aaaf23fffas33gggggf")
    print list.iterator().next()
    print list.get(2)
    
    for val in list:
        print val

if __name__ == "__main__":
    demo()