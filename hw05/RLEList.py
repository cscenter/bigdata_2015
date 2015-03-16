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

class my_cool_iterator:
    def __init__(self, list):
        self.currVal = None
        self.listIter = iter(list)
        self.toLeft = 0

    def __iter__(self):
        return self

    def next(self):
        if (self.toLeft == 0):
            (v, r) = self.listIter.next()
            self.toLeft = r
            self.currVal = v
        self.toLeft -= 1
        return self.currVal



class RLEListImpl(RLEList):
    def __init__(self):
        self.size = 0
        self.impl = []

    def append(self, value):
        self.size += 1

        if (self.size == 1):
            self.impl.append((value, 1))
            return

        v, r = self.impl.pop()
        if (v == value):
            r += 1
            self.impl.append((v, r))
        else:
            self.impl.append((v, r))
            self.impl.append((value, 1))

    def insert(self, index, value):

        if index < 0 or index > self.size:
            raise "Index out of range"

        if index == self.size:
            self.append(value)
            return

        self.size += 1;
        currInd = -1
        currImlpB = []
        currImlpE = list(self.impl)

        i = self.impl
        for (v, r) in i:

            del currImlpE[0]

            currInd += r;
            if (index == currInd):
                if (value == v):
                    currImlpB.append((v, r + 1))
                    currImlpB.extend(currImlpE)
                    self.impl = list(currImlpB)
                    return
                else:
                    if (r - 1 > 0):
                        currImlpB.append((v, r - 1))

                    currImlpB.append((value, 1))
                    currImlpB.append((v, 1))
                    currImlpB.extend(currImlpE)
                    self.impl = list(currImlpB)
                    return
            elif (index < currInd):
                if (value == v):
                    currImlpB.append((v, r + 1))
                    currImlpB.extend(currImlpE)
                    self.impl = list(currImlpB)
                    return
                else:
                    t = index - (currInd - r) - 1
                    if (t > 0): currImlpB.append((v, t))

                    if ((len(currImlpB) > 0) and (t == 0)):
                        (vl, rl) = currImlpB.pop()
                        if (vl == value):
                            currImlpB.append((vl, rl + 1))
                        else:
                            currImlpB.append((vl, rl))
                            currImlpB.append((value, 1))
                    else:
                        currImlpB.append((value, 1))
                    if (currInd - index > 0): currImlpB.append((v, currInd - index + 1))
                    currImlpB.extend(currImlpE)
                    self.impl = list(currImlpB)
                    return
            else:
                currImlpB.append((v, r))


    def get(self, index):
        if ((index >= self.size) or (index < 0)):
            raise "Index out of range"

        currInd = -1
        for (v, r) in self.impl:
            currInd += r
            if currInd >= index:
                return v

    def __iter__(self):
        return my_cool_iterator(self.impl)

    def iterator(self):
        return iter(self)


def append_test():
    l = RLEListImpl()
    l.append('h')
    l.append('e')
    l.append('l')
    l.append('l')
    l.append('o')
    l.append(' ')
    l.append('w')
    l.append('o')
    l.append('r')
    l.append('l')
    l.append('d')
    assert(l.impl == [('h', 1), ('e', 1), ('l', 2), ('o', 1), (' ', 1),
                      ('w', 1), ('o', 1), ('r', 1), ('l', 1), ('d', 1)])

    print("Append - OK")

def iterator_test():
    original = ['h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd']
    l = RLEListImpl()
    l.append('h')
    l.append('e')
    l.append('l')
    l.append('l')
    l.append('o')
    l.append(' ')
    l.append('w')
    l.append('o')
    l.append('r')
    l.append('l')
    l.append('d')
    l2 = []
    for i in l:
        l2.append(i)
    assert (original == l2)
    print("Iterator - OK")

def insert_test():
    l = RLEListImpl()
    h = "qqwweerrttyy"
    for c in h:
        l.append(c)
        
    l.insert(2, 'q')
    l.insert(3, 'e')
    l.insert(5, 'e')
    l.insert(3, 'e')
    assert(l.impl == [('q', 3), ('e', 2), ('w', 1), ('e', 1), ('w', 1), ('e', 2), ('r', 2), ('t', 2),('y',2)])
    print("Insert - OK")


append_test()
iterator_test()
insert_test()