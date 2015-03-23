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
        else:
            place_of_prev_value = 0
            place_of_next_value = 0
            i = 0
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

            else:
                while(i < index-1):
                    place_of_prev_value = place_of_next_value
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
    '''def insert(self, index, value):
        self.count += 1
      #  print self.count
        if index == 0:
            if self.get(0) != value:
                self.impl.insert(0, value)
            else:
                if len(self.impl) > 1 and type(self.impl[1]) == int:
                    self.impl[1] += 1
                else:
                    self.impl.insert(1, 1)
            return
        if index == self.count - 1:
            self.append(value)
            return
        cur_index = -1
        prev_value = ""
        list_index = -1
        for i in self.impl:
            list_index += 1
            if type(i) == int:
                cur_index += i
            else:
                prev_value = i
                cur_index += 1
            if cur_index + 1 >= index:
                if cur_index + 1 == index: #если следующий после закодированной последовательности
                # НЕ ЗАБЫТЬ
                #           ОБЪЕДИНИТЬ если есть чо
                    next = 0
                    if type(self.impl[list_index]) == int:
                        next += 1
                    if self.impl[list_index + next] != prev_value:# list_index + 1 всегда существует т.к. случай когда его нет может быть
                                                            #только если элемент последний, а я это обрабатываю выше
                        self.impl.insert(list_index + next, value)
                    else:
                        if len(self.impl) > list_index + next and type(self.impl[list_index + next + 1]) == int:
                            self.impl[list_index + next + 1] += 1
                        else:
                            self.impl.insert(list_index + next + 1, 1)
                else: # иначе разбиваем наше иньожество на 2 части и вставляем значение посередине
                     if prev_value != value:
                        second_part = cur_index - index + 1
                        self.impl[list_index] = i - second_part
                        if self.impl[list_index] == 0:
                            self.impl[list_index] = value
                        else:
                            list_index += 1
                            self.impl.insert(list_index, value)
                            list_index += 1
                            self.impl.insert(list_index, prev_value)
                            if second_part != 1:
                                list_index += 1
                                self.impl.insert(list_index, second_part - 1)
                     else:
                        self.impl[list_index] += 1

                break

      #  self.impl.insert(index, value)
'''
    def get(self, index):
        #надо прописать ещё момент, когда мой список может быть пуст
        if (len(self.impl) == 0):
            print 'Database is empty'
            return None
        i = 0
        place_of_prev_value = 0
        place_of_next_value = 0
        while (i < index):
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
        if (i < index):
            print 'Index is too large'
            return None
        return self.impl[place_of_prev_value]


    def iterator(self):
        i = -1
        count = 0
        interval = 0
        first_iter = True
        while i < len(self.impl):
            '''if(isinstance(self.impl[place_of_prev_value + 1], int)):
                length_of_value_interval = 1 + self.impl[place_of_prev_value + 1]
            else:
                length_of_value_interval = 1
            small_i = 0
            while small_i < length_of_value_interval:
                small_i += 1
                yield self.impl[place_of_prev_value]
            i += small_i
            if(isinstance(self.impl[place_of_prev_value + 1], int)):
                place_of_prev_value += 2
            else:
                place_of_prev_value += 1'''
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



        #return iter(self.impl)
    def demonstrate(self):
        for i in self.impl:
            print i

def demo():
    list = RLEListRefImpl()
    list.append("foo")
    print 'demonstration'
    list.demonstrate()
    list.insert(0, "bar")
    print 'demonstration'
    list.demonstrate()
    list.append('foo')
    print 'demonstration'
    list.demonstrate()
    list.append('foo')
    print 'demonstration'
    list.demonstrate()
    list.insert(1,'bar')
    print 'demonstration'
    list.demonstrate()
    list.insert(0,'bar')
    print 'demonstration'
    list.demonstrate()
    list.insert(2,'bar')
    print 'demonstration'
    list.demonstrate()
    print 'work of iterator'
    for i in range(8):
        print list.iterator().next()
    #list.insert(6, 'bar')
    #print 'demonstration'
    #list.demonstrate()

    #print list.iterator().next()
    #print list.get(1)
    #print list.get(0)
demo()


