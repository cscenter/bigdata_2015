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


class MyIter:
    def __init__(self, arr):
        self.cur_group_no = 0
        self.cur_item = 0
        self.arr = arr

    def __iter__(self):
        return self

    def next(self):
        if not self.cur_group_no < len(self.arr):
            raise StopIteration

        group = self.arr[self.cur_group_no]

        result = group[0]
        self.cur_item += 1

        if not self.cur_item < self.arr[self.cur_group_no][1]:
            self.cur_group_no += 1
            self.cur_item = 0

        return result


class RLEListImpl(RLEList):
    def __init__(self):
        # основа: массив состоящий из описаний групп подряд идущих одинаковых элементов: (значение, количество)
        self.arr = []

        self.size = 0           # текущее количество элементов

    def get_group_no(self, index):
        if not 0 <= index < self.size:
            raise Exception("Out of border!")

        cur_group_no = 0                            # номер текущей группы
        cur_index = 0                               # исходный индекс первого элемента из текущей группы
        cur_length = self.arr[cur_group_no][1]      # длина текущей группы
        
        while cur_index + cur_length <= index:
            cur_index += cur_length
            cur_group_no += 1
            cur_length = self.arr[cur_group_no][1]

        # cur_index <= index < cur_index + cur_length

        return cur_group_no, cur_index

    def append(self, value):
        if len(self.arr) == 0:
            self.arr.append([value, 1])
        else:
            # проверяем, дополнит ли новое значение последнюю группу
            if self.arr[-1][0] == value:
                self.arr[-1][1] += 1
            else:
                self.arr.append([value, 1])

        self.size += 1

    def insert(self, index, value):
        group_no, cur_index = self.get_group_no(index)

        if self.arr[group_no][0] != value and cur_index != index:
            # придется разбивать группу
            old_group = self.arr[group_no]
            length = old_group[1]

            # преобразуем правую половинку
            l = (cur_index + length) - index
            self.arr[group_no][1] = l

            # вставляем новую группу
            self.arr.insert(group_no, [value, 1])

            # возвращаем в список левую половинку
            l = index - cur_index
            self.arr.insert(group_no, [old_group[0], l])

        elif cur_index == index:
            self.arr.insert(group_no, [value, 1])
        else:
            self.arr[group_no][1] += 1

        self.size += 1

    def get(self, index):
        group_no, _ = self.get_group_no(index)
        return self.arr[group_no][0]

    def iterator(self):
        return MyIter(self.arr)

def demo():
    list = RLEListImpl()
    list.append("foo")
    list.insert(0, "bar")
    for l in list.iterator():
        print(l)
    print(list.get(1))

demo()


