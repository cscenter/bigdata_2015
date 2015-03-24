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
        # array of runs (value, len, right_bound)
        # todo: create Node class
        # I'm sorry for indexes instead of Node clas
        # cause I caught the idea that I can optimize
        # calculation of run index by binary search too
        # close to deadline:)
        self.impl = []
        self.size = 0

    # O(1)
    def append(self, value):
        if len(self.impl) == 0 or self.impl[-1][0] != value:
            self.impl.append((value, 1, self.get_run_bound(-1) + 1))
        else:
            _, run_len, total_index = self.impl.pop()
            self.impl.append((value, run_len + 1, total_index + 1))
        self.size += 1

    # O(1) if index == len
    # O(n) worst case
    # would be O(log(n)) in avg (but O(n) in worst case)
    # if I used binary search for run index calculation
    def insert(self, index, value):
        if index > self.size:
            raise Exception("out of range")

        if index == self.size:
            self.append(value)
        else:
            run_index = self.get_run_index_using_bin_search(index)
            run_len = self.impl[run_index][1]
            self.impl[run_index] = (value, run_len + 1, self.get_run_bound(-1))
            self.update_run_bound()

    # O(n) worst case
    def get(self, index):
        if index > self.size:
            raise Exception("out of range")

        run_index = self.get_run_index_using_bin_search(index)
        return self.impl[run_index][0]

    def iterator(self):
        for i in self.impl:
            for _ in range(i[1]):
                yield i[0]

    # it can be O(log(n))
    def get_run_index_using_bin_search(self, index):
        run_index = 0
        total_index = 0
        prev_run_len = self.impl[run_index][1]
        # TODO: bin search is not implemented yet
        while total_index + prev_run_len <= index:
            total_index += prev_run_len
            run_index += 1
            prev_run_len = self.impl[run_index][1]

        return run_index

    def get_run_bound(self, run_index):
        if len(self.impl) == 0:
            return 0
        return self.impl[run_index][2]

    # calculates right index of runs (see unit tests)
    # todo: total array update is not optimal,
    # it would be better if I pass index to refresh
    def update_run_bound(self):
        total = 0
        for i in range(len(self.impl)):
            elem = self.impl[i]
            run_len = elem[1]
            total += run_len
            self.impl[i] = (elem[0], elem[1], total)