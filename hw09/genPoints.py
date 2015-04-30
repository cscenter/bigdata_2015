__author__ = 'contest.gulikov@gmail.com'

import matplotlib.pyplot as plt
import random

Xbound = 10000
YBound = 10000

class Point():

    def __init__(self, X, Y):
        self.x = X
        self.y = Y

    def getx(self):
        return self.x

    def gety(self):
        return self.y


#squares

points = [Point(random.randint(-Xbound, Xbound), random.randint(-YBound, YBound)) for x in xrange(10**5)]
X = []
Y = []

def sqLen(a, b):
    return max(abs(a.gety() - b.gety()), abs(a.getx() - b.getx()))

def sqr(x):
    return x * x

def cirLen(a, b):
    return sqr(a.x - b.x) + sqr(a.y - b.y)


for i in xrange(3):
    a = Point(random.randint(-Xbound, Xbound),random.randint(-Xbound, YBound))
    len = random.randint(100, 2000)
    for point in points:
        if sqLen(a, point) < len:
            X.append(point.getx())
            Y.append(point.gety())

points = [Point(random.randint(-Xbound, Xbound), random.randint(-YBound, YBound)) for x in xrange(10**5)]

for i in xrange(3):
    a = Point(random.randint(-Xbound, Xbound),random.randint(-Xbound, YBound))
    len = random.randint(100, 2000)
    for point in points:
        if cirLen(a, point) < len * len:
            X.append(point.getx())
            Y.append(point.gety())

points = [Point(random.randint(-Xbound, Xbound), random.randint(-YBound, YBound)) for x in xrange(10**5)]

for i in xrange(4):
    a = Point(random.randint(-Xbound, Xbound),random.randint(-Xbound, YBound))
    if (i == 3):
        len1 = 4000
    else:
        len1 = random.randint(500, 2000)
    len2 = random.randint(len1, len1 + 100)
    for point in points:
        if cirLen(a, point) > len1 * len1 and cirLen(a, point) < sqr(len2):
            X.append(point.getx())
            Y.append(point.gety())

points = [Point(random.randint(-Xbound, Xbound), random.randint(-YBound, YBound)) for x in xrange(10**5)]


def IsInCross(a, b, len):
    return (abs(a.x - b.x) < 10 and abs(a.y-b.y) < len) or (abs(a.y - b.y) < 10 and abs(a.x - b.x) < len)


for i in xrange(3):
    a = Point(random.randint(-Xbound, Xbound),random.randint(-Xbound, YBound))
    len1 = random.randint(500, 3000)
    for point in points:
        if IsInCross(a, point, len1):
            X.append(point.getx())
            Y.append(point.gety())

points = [Point(random.randint(-Xbound, Xbound), random.randint(-YBound, YBound)) for x in xrange(10**2)]


for i in xrange(3):
    a = Point(random.randint(-Xbound, Xbound),random.randint(-Xbound, YBound))
    len1 = random.randint(300, 700)
    for point in points:
        if cirLen(a, point) < sqr(len1):
            X.append(point.getx())
            Y.append(point.gety())

points = [Point(random.randint(-Xbound, Xbound), random.randint(-YBound, YBound)) for x in xrange(30)]

for point in points:
    X.append(point.x)
    Y.append(point.y)



plt.plot(X, Y, 'ro')
plt.show()

print "Y - for saving this dataset"
x = raw_input()

if x == 'Y':
    for x in X:
        print x,

    print

    for y in Y:
        print y,