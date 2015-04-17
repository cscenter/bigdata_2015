# encoding: utf-8
import mincemeat
import util

"""
Алгоритм поиска кандидатов в близнецы с помощью LSH + Min-Hash и Map-Reduce.

Правила выбора параметров: N,B,R
В представленной реализации N=R*B, поэтому достаточно указать N и B.

N - количество хэш функций. Выбирается из рачета допустимой ошибки при определении коэффициента Жаккара.
 Для ошибки существует следующая оценка e <= O(1/(sqrt(N))) где e - величина ошибки. Например для значения N=100 e = 0.1

B - количество корзин. Выбирается исходя из требуемого порога поъожести долкументов (коэффициент Жаккара) после которого
два документа считаются кандидатами в близнецы. Существует следующая оценка для порога threshold = (1/B)^(1/R).
Например для B = 10, N = 100, N=R*B => R = 10, threshold = 0,794

Состоить из 2-x проходов Map-Reduce.
"""

"""
Первый Map-Reduce подсчитывает вектор Min-Hash для документа и находит документы с совпадающими значениями
соответсвующих хэш функций по корзинам.

Мап читает последовательно большой файл документа и подсчитывает для каждого элемента вектор из результатов семейства
различных хэш функций, попутно поддерживаются минимальные значения. Тоесть подсчитывается вектор Min-Hash для документа.
Далее этот вектор разбивается на куски значений корзин и происходит поиск совпадающих векторов.
Выдает пары (ключ,значение) где ключ - вектор значений корзины и её идентификатор, значение - идентификатор документа
Предпологается что вектор значений Min-Hash помещяется в память.
"""


def mapfn(doc_id, docvector):
    import util

    N = 100
    B = 10
    R = 10
    some_random = 6153

    # Простенький детерменированный рандом чтобы сформировать таблицу случаныйх чисел идентичную на всех машинах кластера
    def lcg_random(x):
        return (x * 1103515245 + 12345) % 2147483647

    random_table = []
    for i in range(N):
        some_random = lcg_random(some_random)
        random_table.append(some_random)

    # Семейство случайных функций получается с помощью XOR результата какойлибо хэш функции и случайного числа
    # В реальном проекте, вероятно, качества нативной питоновской фукнции hash будет недостаточно,
    # придется взять что-то получше.
    def hash_family(id, x):
        mask = random_table[id]
        return hash(str(x)) ^ mask

    # Считаем что вектор минимумов помещается в память
    min_table = [None] * N
    for v in docvector:
        for hash_id in range(N):
            hash_val = hash_family(hash_id, v)
            if min_table[hash_id] is None:
                min_table[hash_id] = hash_val
            else:
                min_table[hash_id] = min(hash_val, min_table[hash_id])

    # Разделяем на вектора значений корзин
    for i in range(B):
        start = R * i
        finish = R * (i + 1)
        bucket_vals = min_table[start:finish]
        bucket_vals_str = ",".join(str(x) for x in bucket_vals)
        map_key = util.prepend_to_str_of_tuple(":", bucket_vals_str, i)
        yield map_key, doc_id


"""
Собираем только те элементы которые имеют совпадения
"""


def reducefn(key, doc_id_list):
    # if len(doc_id_list) >= 2:
    #     return doc_id_list
    return doc_id_list

"""
Второй Map-Reduce преобразует найденых кандидатов в пары.
"""


def mapfn4(key, doc_ids):
    import util

    if doc_ids:
        # Найдем все пары близнецов из имеющегося списка
        for doc_id_1 in doc_ids:
            for doc_id_2 in doc_ids:
                # Будем поддерживать лексикографический порядок именования документов в паре
                if doc_id_1 < doc_id_2:
                    doc_id_pair = util.arg_to_str(util.DEF_DELIM, doc_id_1, doc_id_2)
                    yield doc_id_pair, ""


def reducefn4(doc_id_pair, vs):
    return vs


s = mincemeat.Server()

input0 = {}
input0['doc1'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
input0['doc2'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
input0['doc3'] = [48, 25, 69, 36, 74, 100, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 50, 77, 30, 32]
input0['doc4'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89, 63, 66, 96, 78, 19, 39, 53, 83, 20]
# копия doc1
#input0['doc1_c'] = [48, 25, 69, 36, 22, 24, 88, 37, 71, 8, 68, 60, 20, 33, 96, 9, 50, 77, 30, 32]
# копия doc2
#input0['doc2_c'] = [48, 25, 69, 12, 22, 24, 45, 37, 71, 8, 68, 60, 63, 78, 12, 9, 50, 77, 30, 32]
# копия doc4
#input0['doc4_c'] = [22, 5, 34, 96, 31, 41, 14, 89, 18, 100, 89, 63, 66, 96, 78, 19, 39, 53, 83, 20]
# что то похожее на doc3
input0['doc3_s'] = [48, 25, 11, 36, 74, 100, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 50, 77, 30, 32]
# что то на грани похожее на doc3
input0['doc3_bs'] = [18, 25, 11, 36, 74, 39, 94, 14, 89, 18, 100, 89, 63, 66, 96, 9, 92, 45, 30, 64]

# phase 1
s.map_input = mincemeat.DictMapInput(input0)
s.mapfn = mapfn
s.reducefn = reducefn

# сохраняем в большые файлы в вооброжаемую файловую систему
results = s.run_server(password="")
#
# for key, value in sorted(results.items()):
#     print("%s: %s" % (key, value))

# phase 2
s.map_input = mincemeat.DictMapInput(results)
s.mapfn = mapfn4
s.reducefn = reducefn4

results = s.run_server(password="")

print("Candidates:")
for key, value in sorted(results.items()):
    print key
print("---")


