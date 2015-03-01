# encoding: utf-8
import mincemeat


# Преобразовывает ключи.
def mapfn(k, v):
    yield "+".join(k.split("+")[:2]), int(v)

# Суммирует.
def reducefn(k, vs):
    result = sum(vs)
    return result

# Получение результатов предыдущего шага.
d = dict()
f = open("tmp.txt", "r")
for l in f.readlines():
    key, value = l.split()
    d[key] = value
f.close()


# Запуск сервера.
s = mincemeat.Server() 
    
s.map_input = mincemeat.MapInputDictionary(d) 
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")

# Выводит результат. Можно было бы куда-нибудь его запистать, но нет интерфейса для DFS.
for key, value in sorted(results.items()):
    print("%s - %s" % (map(lambda x: int(x), key.split("+")), value) )