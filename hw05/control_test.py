# encoding: utf-8
from rlelist import RLEListTrueImpl as RLEListImpl

# создает новый экземпляр списка
def newInstance():
    return RLEListImpl()

# возвращает длину сжатого списка
def implLength(l):
    return len(l.impl)

# возвращает сжатый список как строку (используется в assert'е, если тест не падает то не требуется)
def implAsString(l):
    return ",".join(["%s (%d)" % (e.value, e.count) for e in l.impl])

# конкатенирует элементы списка и возвращает строку-результат
def concat(l):
    return "".join([c for c in l.iterator()])

def assertContent(expected, l):
    for i in xrange(len(expected)):
        assert expected[i] == l.get(i), "get(%d): Expected:%s actual:%s" % (i, expected[i], l.get(i))
    assert expected == concat(l), "Concat: Expected:%s actual:%s" % (expected, concat(l))

def test1():
    l = newInstance()
    l.append("h")
    l.append("e")
    l.append("l")
    l.append("l")
    l.append("o")
    assert 4 == implLength(l)
    assertContent("hello", l)

def test2():
    l = newInstance()
    l.insert(0, "o")
    l.insert(0, "l")
    l.insert(0, "l")
    l.insert(0, "e")
    l.insert(0, "h")
    assert 4 == implLength(l), implAsString(l)
    assertContent("hello", l)

def test3():
    l = newInstance()
    l.append("e")
    l.append("l")
    l.append("o")
    l.insert(1, "l")
    l.insert(0, "h")
    assert 4 == implLength(l), implAsString(l)
    assertContent("hello", l)

    l = newInstance()
    l.append("e")
    l.append("l")
    l.append("o")
    l.insert(2, "l")
    l.insert(0, "h")
    assert 4 == implLength(l), "impl=%s" % implAsString(l)
    assertContent("hello", l)

def test4():
    l = newInstance()
    l.append("h")        
    l.append("e")        
    l.append("e")        
    l.append("e")        
    l.insert(2, "e")
    l.insert(2, "E")
    assert 4 == implLength(l), "impl=%s" % implAsString(l)
    assertContent("heEeee", l)

    l = newInstance()
    l.append("h")        
    l.append("e")        
    l.append("e")        
    l.append("e")        
    l.insert(2, "E")
    l.insert(4, "E")

    assert 6 == implLength(l), "impl=%s" % implAsString(l)
    assertContent("heEeEe", l)

def test5():
    l = newInstance()
    l.insert(0, "a")
    l.insert(0, "a")
    l.insert(0, "a")
    assert 1 == implLength(l), "impl=%s" % implAsString(l)
    assertContent("aaa", l)

def test6():
    l = newInstance()
    l.append("h")
    l.append("h")
    l.append("h")
    l.append("e")
    l.append("e")
    l.append("e")
    l.insert(4, "e")
    assert 2 == implLength(l), "impl=%s" % implAsString(l)
    assertContent("hhheeee", l)

test1()
test2()
test3()
test4()
test5()
test6()
print "All passed"
