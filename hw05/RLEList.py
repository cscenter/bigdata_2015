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
        self._array = []
        self._len = 0

    def append(self, value):
        if self._len == 0:
            self._array.append((1, value))
        else:
            if self._array[-1][1] == value:
                cnt, v = self._array.pop()
                self._array.append((cnt + 1, v))
            else:
                self._array.append((1, value))
        self._len += 1

    def insert(self, index, value):
        if index < 0 or index > self._len:
            raise "Wrong index in insert()"
        if index == self._len:
            self.append(value)
            return
        curr = None
        prev = None
        it = self.iterator()
        for i in range(index + 1):
            prev = curr
            curr = it.next()

        if curr == value:
            cnt, v = self._array[it.index]
            self._array[it.index] = (cnt + 1, v)
        elif prev == value:
            cnt, v = self._array[it.index - 1]
            self._array[it.index - 1] = (cnt + 1, v)
        else:
            if it.remain == self._array[it.index][0] - 1:
                self._array.insert(it.index, (1, value))
            else:
                cnt, v = self._array[it.index]
                self._array.insert(it.index + 1, (it.remain + 1, v))
                self._array.insert(it.index + 1, (1, value))
                self._array[it.index] = (cnt - it.remain - 1, v)
        self._len += 1

    def get(self, index):
        if index < 0 or index >= self._len:
            raise "Wrong index in get()"
        it = self.iterator()
        for i in range(index):
            it.next()
        return it.next()

    def __iter__(self):
        class Iterator():
            def __init__(self, x):
                self.it = iter(x)
                self.index = -1
                self.value = None
                self.remain = 0

            def __iter__(self):
                return self

            def next(self):
                if self.remain == 0:
                    cnt, v = self.it.next()
                    self.remain = cnt
                    self.value = v
                    self.index += 1
                self.remain -= 1
                return self.value
        return Iterator(self._array)

    def iterator(self):
        return iter(self)


def append_test():
    rle_list = MyRLEList()
    rle_list.append('a')
    rle_list.append('a')
    rle_list.append('a')
    rle_list.append('b')
    rle_list.append('b')
    rle_list.append('c')
    rle_list.append('d')
    rle_list.append('d')
    assert(rle_list._array == [(3, 'a'), (2, 'b'), (1, 'c'), (2, 'd')])
    print("Append - OK")

def insert_test():
    rle_list = MyRLEList()
    rle_list.append('a')
    rle_list.append('a')
    rle_list.append('a')
    rle_list.append('b')
    rle_list.append('b')
    rle_list.append('c')
    rle_list.append('d')
    rle_list.append('d')
    
    rle_list.insert(8, 'f')
    rle_list.insert(1, 'a')
    rle_list.insert(3, 'c')
    rle_list.insert(8, 'd')
    assert(rle_list._array == [(3, 'a'), (1, 'c'), (1, 'a'), (2, 'b'), (1, 'c'), (3, 'd'), (1, 'f')])
    print("Insert - OK")

if __name__ == "__main__":
    append_test()
    insert_test()




