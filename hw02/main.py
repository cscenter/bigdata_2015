from dfs_client import *
import mincemeat


def mapfn(k, v):
    reduce_key = None
    i = 1
    for l in get_file_content(v):
        if reduce_key is None:
            matrix_num, start, end = l.split(" ", 2)
            reduce_key = matrix_num
            continue
        values = [int(v) for v in l.split(" ")]
        if matrix_num == 1:
            for value in values:
                line=i%one[1]+start
                # column = i%one[1]
                for m in range(1,two[1]+1):
                    yield (line, m), [1, start, i, [value]]

                i+=1
        if matrix_num == 2:
            for value in values:
                line=i%two[1]+start
                column = i%two[1]
                for h in range(1,one[0]+1):
                    yield (h, column), [2, start, i, [value]]
                i+=1



# редьюсер суммирует значения с одинаковым ключом
def reducefn(k, vs):
    arr1=[]
    arr2=[]
    res1=[]
    res2=[]
    for element in vs:
        if element[0]==1:
            arr1.append(element)
        if element[0]==2:
            arr2.append(element)
    chunks=set()
    for a in arr1:
        chunks.add(int(a[1]))
    for b in sorted(list(chunks)):
        for a in arr1:
            if int(a[1])==b:
                res1.extend(a[3])

    chunks=set()
    for a in arr2:
        chunks.add(int(a[1]))
    for b in sorted(list(chunks)):
        for a in arr2:
            if int(a[1])==b:
                res2.extend(a[3])

    return sum(map(lambda x, y: x*y, arr1, arr2))


matrix_files = [l for l in get_file_content("/matrix1")]
for l in get_file_content("/matrix2"):
    matrix_files.append(l)
# print(matrix_files)



def get_dimentions_of_matrixes(u):
    l = []
    y = {}
    for i in [l for l in get_file_content(u)]:
        t = i.replace("/", "").replace("matrix", "").split("_")[1].split(".")[0]
        l.append(t)
        y[t] = i
    # print(y[sorted(l, reverse=True)[0]])
    reduce_key = None
    li = []
    for l in get_file_content(y[sorted(l, reverse=True)[0]]):
        if reduce_key is None:
            matrix_num, start, end = l.split(" ", 2)
            reduce_key = matrix_num
            continue
        li.extend([int(v) for v in l.split(" ")])
    return int(end), int(len(li) / (int(end) - int(start) + 1))
    # print(matrix_files)


one = get_dimentions_of_matrixes("/matrix1")
two = get_dimentions_of_matrixes("/matrix2")

print(one, two)
assert one[1] == two[0]

print(list(get_file_content("/matrix1_1.dat")))
s = mincemeat.Server()

s.map_input = mincemeat.MapInputDFSFileName(matrix_files)
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="")
for key, value in sorted(results.items()):
    print("%s: %s" % (key, value))
