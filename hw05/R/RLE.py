class RLEList(object):
    def __init__(self):
        pass

    def iterator(self):
        pass


    def append(self, value):
        pass

    def get(self, index):
        pass

    def insert(self, index, value):
        pass


class RLEListRefImpl(RLEList):
    def __init__(self, it):
        self.list = self._make_block_list(it)

    def __getitem__(self, key):
        return self.list[self._get_b_id(key)][0]

    def __len__(self):
        if not self.list:
            return 0
        data, begin_id, count = self.list[-1]
        return begin_id + count

    def __iter__(self):
        for data, begin_id, count in self.list:
            for i in range(count):
                yield data

    def append(self, value):
        self.list.append((value, len(self), 1))
        self._merge()

    def get(self, index):
        return self[index]

    def iterator(self):
        return self.__iter__()

    def insert(self, idx, value):
        if not self.list or len(self) <= idx:
            self.append(value)
            return

        b_id = self._get_b_id(idx)
        in_block_id = idx - self.list[b_id][1]
        self._split(b_id, in_block_id)

        myl = b_id + 1
        while myl < len(self.list):
            next_data, next_begin_id, next_count = self.list[myl]
            self.list[myl] = (next_data, next_begin_id + 1, next_count)
            myl += 1

        data, begin_id, count = self.list[b_id]
        self.list.insert(b_id + 1, (value, begin_id + count, 1))

        self._merge(b_id + 1)


    def _make_block_list(self, it):
        list_inner = []
        myl = -1
        for add in it:
            myl += 1
            if not list_inner:
                list_inner.append((add, myl, 1))
                continue
            data, begin_id, count = list_inner[-1]
            if data == add:
                list_inner[-1] = (data, begin_id, count + 1)
                continue
            list_inner.append((add, myl, 1))
        return list_inner

    def _get_b_id(self, id):
        if len(self) <= id:
            raise IndexError("Index out of range!")
        myl = 0
        for data, begin_id, count in self.list:
            if begin_id <= id < begin_id + count:
                return myl
            myl += 1

    def _merge(self, block_id=-1):
        if len(self.list) < 2:
            return
        data, begin_id, count = self.list[block_id]
        prev_data, prev_begin_id, prev_count = self.list[block_id - 1]

        if data == prev_data:
            self.list[block_id - 1] = (
                prev_data, prev_begin_id, prev_count + count)
            del self.list[block_id]
            block_id -= 1

        if block_id < 0:
            return
        data, begin_id, count = self.list[block_id]
        next_data, next_begin_id, next_count = self.list[block_id + 1]
        if data == next_data:
            self.list[block_id] = (data, begin_id, count + next_count)
            del self.list[block_id + 1]

    def _split(self, block_id, in_block_id):
        data, begin_id, count = self.list[block_id]
        if count == in_block_id:
            return
        self.list[block_id] = (data, begin_id, in_block_id)
        self.list.insert(block_id + 1,
                         (data, begin_id + in_block_id, count - in_block_id))

