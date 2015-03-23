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


class RLEListImpl(RLEList):
    def __init__(self):
        self.impl = []

    def append(self, value):
        if self.impl and self.impl[-1].value == value:
            self.impl[-1].count += 1
        elif self.impl:
            self.impl.append(Entry(value, self.sumOn(-1)))
        else:
            self.impl.append(Entry(value, 0))

    def insert(self, index, value):
        real_index = self.binarySearch(index)
        # insert in head of entry
        prev_sum = 0
        if index == self.impl[real_index].sum:
            if real_index != 0:
                prev_sum = self.sumOn(real_index - 1)
            self.impl.insert(real_index, Entry(value, prev_sum))
            self.sumIncFrom(real_index + 1)
        # insert in tail of entry
        else:
            # split entry on head-part, new-one and tail-part
            old_count = self.impl[real_index].count
            old_value = self.impl[real_index].value
            # fix head-part impl[real_index]
            head_count = index - self.impl[real_index].sum
            self.impl[real_index].count = head_count
            # head_sum will no change
            assert(self.impl[real_index].count >= 1)
            # insert new impl[real_index + 1]
            new_sum = self.sumOn(real_index)
            self.impl.insert(real_index + 1, Entry(value, new_sum))
            # insert a rest(tail) impl[real_index + 2]
            self.impl.insert(real_index + 2, Entry(old_value, new_sum + 1))
            self.impl[real_index + 2].count = old_count - head_count
            self.sumIncFrom(real_index + 3)

    def sumOn(self, index):
        '''calculate index of next entry'''
        return self.impl[index].sum + self.impl[index].count

    def sumIncFrom(self, index):
        '''updates sum in entris from index to end'''
        for e in self.impl[index:]:
            e.sum += 1

    def get(self, index):
        real_index = self.binarySearch(index)
        return self.impl[real_index].value

    def iterator(self):
        for entry in self.impl:
            for _ in range(entry.count):
                yield entry.value

    def binarySearch(self, index):
        first = 0
        last = len(self.impl)-1

        while first <= last:
            midpoint = (first + last) // 2
            if (self.impl[midpoint].sum <= index
                and index <= self.sumOn(midpoint) - 1):
                return midpoint
            else:
                if index < self.impl[midpoint].sum:
                    last = midpoint - 1
                else:
                    first = midpoint + 1

        raise Exception("Not found")


class Entry():
    def __init__(self, value, prev_sum):
        self.value = value
        self.count = 1
        self.sum = prev_sum

if __name__ == "__main__":
    def printList(alist):
        for e in alist.impl:
            print(e.value, e.count, e.sum)

    alist = RLEListImpl()
    elist = [1,1,1,2,2,2,5,5,7,7,7]
    group = [0,0,0,1,1,1,2,2,3,3,3]
    for x in elist:
        alist.append(x)

    for i, x in enumerate(elist):
        assert(alist.get(i) == x)

    actual = []
    for x in alist.iterator():
        actual.append(x)
    assert(actual == elist)

    assert(len(alist.impl) == 4)

    for i, x in enumerate(group):
        assert(alist.binarySearch(i) == x)

    alist.insert(8, 3)
    assert(len(alist.impl) == 5)
    assert(alist.get(8) == 3)

    alist.insert(2, 0)
    assert(alist.get(2) == 0)

    assert(alist.get(0)==1)
    assert(alist.get(7)==5)
    assert(alist.get(12)==7)
