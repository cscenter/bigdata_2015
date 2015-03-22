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
    def __init__(self):
        self.impl = []
        self.count = 0

    def append(self, value):
        if type(value) == int:
            self.count += value
        else:
            self.count += 1
        if len(self.impl) == 0:
            self.impl.append(value)
        last_index = len(self.impl) - 1
        if type(self.impl[last_index]) == int:
            if self.impl[last_index - 1] != value:
                self.impl.append(value)
            else:
                self.impl[last_index] += 1
        else:
             if self.impl[last_index] != value:
                self.impl.append(value)
             else:
                self.impl.append(1)           


    def insert(self, index, value):
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
                    if self.impl[list_index + 1] != prev_value:# list_index + 1 всегда существует т.к. случай когда его нет может быть 
                                                            #только если элемент последний, а я это обрабатываю выше
                        self.impl.insert(list_index + 1, value)
                    else:
                        if len(self.impl) > list_index + 1 and type(self.impl[list_index + 2]) == int:
                            self.impl[list_index + 2] += 1
                        else:
                            self.impl.insert(list_index + 2, 1)
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

    def get(self, index):
        cur_index = -1
        return_string = ""
        for i in self.impl:
            if type(i) == int:
                cur_index += i
            else:
                return_string = i
                cur_index += 1
            if cur_index >= index:
                return return_string
                    

    def iterator(self):
        return iter(self.impl)

def demo():
    list = RLEListRefImpl()
    list.append("aaa")
    #list.append("aaa")
    list.append("foo")
    list.append(3)
    list.append("a")
    list.append(2)
    list.insert(0, "aaa")
    #list.insert(0, "aaa")
    list.insert(4, "foo") #Здесь типа, ев 2 части разбивать если че
   # list.append("mar")
   # list.append(2)
   # list.append("z")
   # list.append(1)
    list.insert(8, "a") 
   # print list.iterator().next()
 #   for i in range(0, 6):
   #     print list.get(i)
    for i in range(0, len(list.impl)):
        print list.impl[i]

demo()
