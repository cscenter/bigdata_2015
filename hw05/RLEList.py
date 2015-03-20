# encoding: utf-8
import sys

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


class MyRLEList(RLEList):
    def __init__(self):
        self._list = []
        self._length = 0

    def append(self, value):
        self._length += 1
        if len(self._list) == 0 or self._list[-1][0] != value:
            self._list.append((value, 1))
        else:
            self._list.append((lambda (x, y): (x, y + 1))(self._list.pop()))

    def insert(self, index, value):
        if index > self._length or index < 0:
            raise "bad index"
        if index == self._length:
            self.append(value)
            return
        c = None
        p = None
        it = self.iterator()
        for i in range(index + 1):
            p = c
            c = it.next()

        if c == value:
            self._list[it.index] = (lambda (x, y): (x, y + 1))(self._list[it.index])
        elif p == value:
            self._list[it.index - 1] = (lambda (x, y): (x, y + 1))(self._list[it.index - 1])
        elif self._list[it.index][1] - it.cycle - 1 != 0:
            tmp = self._list[it.index]
            self._list.insert(it.index + 1, (tmp[0], it.cycle + 1))
            self._list.insert(it.index + 1, (value, 1))
            self._list[it.index] = (tmp[0], tmp[1] - it.cycle - 1)
        else:
            self._list.insert(it.index, (value, 1))
        self._length += 1

    def get(self, index):
        if index >= self._length or index < 0:
            raise "bad index"
        it = self.iterator()
        for i in range(index):
            it.next()
        return it.next()

    def __iter__(self):
        class Iterator():
            def __init__(self, x):
                self.it = iter(x)
                self.index = -1
                self.cycle = 0
                self.value = None

            def __iter__(self):
                return self

            def next(self):
                if self.cycle == 0:
                    x = self.it.next()
                    self.index += 1
                    self.value = x[0]
                    self.cycle = x[1]
                self.cycle -= 1
                return self.value
        return Iterator(self._list)

    def iterator(self):
        return iter(self)



def demo():
	list = RLEListRefImpl()
	list.append("foo")
	list.insert(0, "bar")
	print list.iterator().next()
	print list.get(1)


if __name__ == "__main__":
    myRLEList = MyRLEList()
    myRLEList.append(1)
    myRLEList.append(2)
    myRLEList.append(2)
    myRLEList.append(2)
    myRLEList.append(3)
    myRLEList.append(3)
    myRLEList.append(4)
    myRLEList.append(4)
    assert(myRLEList._list == [(1, 1), (2, 3), (3, 2), (4, 2)])

    myRLEList.insert(8, 8)
    myRLEList.insert(1, 1)
    myRLEList.insert(4, 1)
    myRLEList.insert(8, 4)
    assert(myRLEList._list == [(1, 2), (2, 2), (1, 1), (2, 1), (3, 2), (4, 3), (8, 1)])

    for x in myRLEList:
        sys.stdout.write(str(x) + " ")
    print