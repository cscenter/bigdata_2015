<<<<<<< HEAD
# encoding: UTF-8
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
        value = str(value)
        if(len(self.impl) == 0):
            self.impl.append(value)
        else:
            if (isinstance(self.impl[len(self.impl) - 1], int)):
                if (self.impl[len(self.impl) - 2] == value):
                    self.impl[len(self.impl) - 1] += 1
                else:
                    self.impl.append(value)
            else:
                if (self.impl[len(self.impl) - 1] == value):
                    self.impl.append(1)
                else:
                    self.impl.append(value)


    def insert(self, index, value):
        #значения храним в виде строк, а для RLE используем целочисленный тип данных
        value = str(value)
        #если база данных пуста, то просто добавляем в лист значение
        if (len(self.impl) == 0):
            self.impl.append(value)
        #если база данных не пуста
        else:
            #последнее встретившееся нам значение в бд
            place_of_prev_value = 0
            #следующее значение в бд
            place_of_next_value = 0
            i = 0
            #рассматриваем граничный случай вставки в начало
            if (index == 0):
                if(len(self.impl) >= place_of_prev_value + 2 and isinstance(self.impl[place_of_prev_value + 1],int)):
                    if self.impl[place_of_prev_value] == value:
                        self.impl[place_of_prev_value + 1] += 1
                    else:
                        self.impl.insert(0, value)
                else:
                    if self.impl[place_of_prev_value] == value:
                        self.impl.insert(1, 1)
                    else:
                        self.impl.insert(0, value)
            #рассматриваем граничный случай вставки на вторую позицию
            elif (index == 1):
                if(self.impl[place_of_prev_value] == value):
                    if(len(self.impl) >= place_of_prev_value + 2 and isinstance(self.impl[place_of_prev_value + 1],int)):
                        self.impl[place_of_prev_value + 1] += 1
                    else:
                        self.impl.insert(1,1)
                else:
                    if(len(self.impl) >= place_of_prev_value + 2 and isinstance(self.impl[place_of_prev_value + 1],int)):
                        second_interval_length = self.impl[place_of_prev_value + 1] - 1
                        prev_value = self.impl[place_of_prev_value]
                        k = 1
                        #self.impl.insert(place_of_prev_value + k, value)
                        self.impl[place_of_prev_value + k] = value
                        k += 1
                        self.impl.insert(place_of_prev_value + k, prev_value)
                        k += 1
                        if (second_interval_length != 0):
                            self.impl.insert(place_of_prev_value + k, second_interval_length)
                        else:
                            self.impl.insert(1, value)
            #если у нас не граничные случаи, действуем следующим образом:
            else:
                #пока мы не дошли до места вставки
                while(i < index-1):
                    place_of_prev_value = place_of_next_value
                    #если после предыдущего значения в бд шло число повторов
                    if (len(self.impl) >= place_of_prev_value + 2 and isinstance(self.impl[place_of_prev_value + 1], int)):
                        i += self.impl[place_of_prev_value + 1] + 1
                        if (len(self.impl) >= place_of_prev_value + 3):
                            place_of_next_value += 2
                        else:
                            break
                    else:
                        i += 1
                        if (len(self.impl) >= place_of_prev_value + 2):
                            place_of_next_value += 1
                        else:
                            break
                #если наш index не вторгается ни в чей интервал
                if (i == index-1):
                    #если после предыдущего значения в бд шло число повторов
                    if (len(self.impl) >= place_of_prev_value + 2 and isinstance(self.impl[place_of_prev_value + 1], int)):
                        #если значение, которое мы хотим вставить совпадает с предыдущим значением, то просто увеличиваем число повторов на единицу
                        if (self.impl[place_of_prev_value] == value):
                            self.impl[place_of_prev_value + 1] += 1
                        #если значение, которое мы хотим встатвить не совпадает с предыдущим значением
                        else:
                            self.impl.insert(place_of_prev_value + 2, value)
                    #если после предыдущего значения в бд не шло число повторов(т.е. оно было единственно)
                    else:
                        #если значение, которое мы хотим вставить совпадает с предыдущим значением то просто добавляем счётчик повторов со значением 1
                        if (self.impl[place_of_prev_value] == value):
                            self.impl.insert(place_of_prev_value + 1, 1)
                        #если значение, которое мы хотим встатвить не совпадает с предыдущим значением
                        else:
                            self.impl.insert(place_of_prev_value + 1, value)
                if (i > index - 1):
                    if(self.impl[place_of_prev_value] == value):
                        self.impl[place_of_prev_value + 1] += 1
                    else:
                        first_interval_length = self.impl[place_of_prev_value + 1] - i + index
                        second_interval_length = i - index - 1
                        prev_value = self.impl[place_of_prev_value]
                        k = 1
                        if (first_interval_length != 0):
                            self.impl[place_of_prev_value + k] = first_interval_length
                            k += 1
                        self.impl.insert(place_of_prev_value + k, value)
                        k += 1
                        self.impl.insert(place_of_prev_value + k, prev_value)
                        k += 1
                        if (second_interval_length != 0):
                            self.impl.insert(place_of_prev_value + k, second_interval_length)
                if (i < index - 1):
                    self.append(self, value)

    def get(self, index):
        #надо прописать ещё момент, когда мой список может быть пуст
        if (len(self.impl) == 0):
            print 'Database is empty'
            return None
        i = 0
        place_of_prev_value = 0
        place_of_next_value = 0
        while (i < index ):
            place_of_prev_value = place_of_next_value
            if (len(self.impl) >= place_of_prev_value + 2 and isinstance(self.impl[place_of_prev_value + 1], int)):
                i += self.impl[place_of_prev_value + 1] + 1
                if (len(self.impl) >= place_of_prev_value + 3):
                    place_of_next_value += 2
                else:
                    break
            else:
                if (len(self.impl) >= place_of_prev_value + 2):
                    place_of_next_value += 1
                else:
                    break
                i += 1
        if (i < index ):
            print 'Index is too large'
            return None
        return self.impl[place_of_next_value]


    def iterator(self):
        i = -1
        count = 0
        interval = 0
        first_iter = True
        while i < len(self.impl):
            if (count == interval):
                count = 0
                if (len(self.impl) >= i + 2 and isinstance(self.impl[i + 1], int)):
                    i += 2
                else:
                    i += 1
            if(count == 0 and len(self.impl) >= i + 2 and isinstance(self.impl[i + 1],int)):
                interval = 1 + self.impl[i + 1]
            else:
                interval = 1
            count += 1
            yield self.impl[i]


    def demonstrate(self):
        for i in self.impl:
            print i

def demo():
    list = RLEListRefImpl()
    list.append("foo")
    list.insert(0, "bar")
    print list.iterator().next()
    print list.get(1)
    print list.get(0)
    print 'demonstration'
    list.demonstrate()
demo()
=======
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
>>>>>>> master


