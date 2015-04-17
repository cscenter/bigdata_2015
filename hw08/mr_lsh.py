# encoding: utf-8
import mincemeat
import math
def mapfn(docid, docvector_and_num_of_bands):
    docvector, b, max_item = docvector_and_num_of_bands
    #вычисляю длину вектора в полосе
    r = len(docvector)/b
    count = r
    ind = 0
    list_of_items = []
    for item in docvector:
        list_of_items.append(item)
        count -= 1
        if count == 0 or item == docvector[len(docvector) - 1]:
            hash = sum(list_of_items) + ind * r * max_item
            hash = str(hash)
            ind += 1
            list_of_items = []
            count = r
            yield hash, docid


def reducefn(k, vs):
    return vs

s = mincemeat.Server()

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89,  63,  66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89,  63,  66, 96, 78, 19, 39, 53, 83, 20]
#вычисляем b, количество "лент"( частей, на которые я делю вектор документа),
#для этого сначала определяем порог сходства(threshold)
threshold = 0.75
#вычисляю длину одного из документов, предполагаю, что они все одной длины
for i in input0.keys():
    n = len(input0[i])
    break
ro = (math.log(n * math.log(1./threshold)) - math.log(math.log(n * math.log(1./threshold))))/ math.log(1./threshold)
b = float(n)/ro
b = int(b)

#значение максимального элемента во всех векторах документов(нужно для вычисления кэша),
#я на всякий случай его вычисляю, хотя скорее всего оно должно быть всегда известно из алгоритма,
#по которому строятся вектора документов.
#в данном случае, можно не проводить все эти вычисления и принять max_item = 100
max_item = 0
for doc in input0.values():
    max_item_next = max(doc)
    if max_item_next > max_item:
        max_item = max_item_next
#в mapfn вместе с векторами документов передаю количество полос,
#на которые буду их делить и максимально возможный элемент во всех векторах документов(нужен для вычисления хэша)
input0 = {k: (v, b, max_item) for k, v in input0.iteritems()}
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="")
#теперь в results у меня хранятся в отдельных списках кандидаты в близнецы,
#так как в формулировке задания указано выводить пары похожих документов я использую возможно не слишком оптимальный код,
#указанный ниже
past_values = []
print "Following documents are similar:"
for value in results.values():
    if len(value) == 2 and (value[0], value[1]) not in past_values and (value[1], value[0]) not in past_values:
        print "%s-%s" % (value[0], value[1])
        pair = (value[0], value[1])
        past_values.append(pair)
    elif len(value) > 2:
        for i in range(0, len(value)):
            for j in range(0, len(value)):
                if i != j and (value[i], value[j]) not in past_values and (value[j], value[i]) not in past_values:
                    print "%s-%s" % (value[i], value[j])
                    pair = (value[i], value[j])
                    past_values.append(pair)