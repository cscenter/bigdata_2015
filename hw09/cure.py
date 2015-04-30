__author__ = 'contest.gulikov@gmail.com'


def cure(data, steps = -1, fl = False):

    import math

    print 'Start Cure'

    if steps == -1:
        steps = len(data) / int(2 * math.log(len(data)))
        steps += 2

    state = [(i, data[i]) for i in range(len(data))]


    def dist(a, b):
        return (a[0] - b[0]) * (a[0] - b[0]) + (a[1] - b[1]) * (a[1] - b[1])

    position = [i for i in range(len(data))]

    N = len(data)
    cnt = N + 1
    import random
    while len(state) > steps + 5:
        minLen = 10**18
        mix = -1
        miy = -1
        if not fl or random.randint(1, 12) < 7:
          for i in xrange(len(state)):
              for j in xrange(i + 1, len(state)):
                  if minLen > dist(state[i][1], state[j][1]):
                      mix = i
                      miy = j
                      minLen = dist(state[i][1], state[j][1])
        else:
          q = [0] * (cnt + 2)
          for i in xrange(len(state)):
            q[state[i][0]] = i 

          for i in range(N):
            for j in range(N):
              if position[i] != position[j] and position[i] < position[j]:
                di = dist(data[i], data[j])
                if di < minLen:
                  minLen = di
                  mix = q[position[i]]
                  miy = q[position[j]]

        if mix == -1:
            break
        ns = []
        avx = 0
        avy = 0
        cnt += 1
        number = 0
        for i in range(N):
            if (position[i] == state[mix][0]) or (position[i] == state[miy][0]):
                number += 1
                avx += data[i][0]
                avy += data[i][1]

        for i in xrange(N):
            if (position[i] == state[mix][0]) or (position[i] == state[miy][0]):
                position[i] = cnt


        if mix < miy:
            mix, miy = miy, mix
        del state[mix]
        del state[miy]
        avx = float(avx) / float(number)
        avy = float(avy) / float(number)
        state.append((cnt, (avx, avy)))

    print 'Cure is Done'

    if not fl :
        state = [x[1] for x in state if len(filter(lambda q : q == x[0], position)) > 3]
        return state

    ret = []
    for x, y in state:
        ret.append(([data[i] for i in xrange(N) if position[i] == x], y))
    return ret

def cure2(data, steps = -1, fl = False):

    import math

    print 'Start Cure'

    if steps == -1:
        steps = len(data) / int(2 * math.log(len(data)))
        steps += 2

    state = [(i, data[i]) for i in range(len(data))]


    def dist(a, b):
        return (a[0] - b[0]) * (a[0] - b[0]) + (a[1] - b[1]) * (a[1] - b[1])

    position = [i for i in range(len(data))]

    N = len(data)
    cnt = N
    while cnt > steps:
        minLen = 10**18
        mix = -1
        miy = -1
        dista = {}
        for i in xrange(N):
            for j in xrange(i + 1, N):
                if position[i] != position[j] and position[i] < position[j]:
                    q = dist(data[i], data[j])
                    if q < minLen:
                        minLen = q
                        mix = position[i]
                        miy = position[j]


        if mix == -1:
            break
        cnt -= len(filter(lambda x : x == miy, position))
        for i in range(N):
            if position[i] == miy:
                position[i] = mix


    from collections import defaultdict

    avx = defaultdict(int)
    avy = defaultdict(int)

    for i in range(N):
        avx[position[i]] += data[i][0]
        avy[position[i]] += data[i][1]

    dif = set(position)

    print 'Cure is Done'

    if not fl:
        state = []
        for x in dif:
            state.append((avx[x] / float(len(filter(lambda y : y == x, position))), (avy[x] / float(len(filter(lambda y : y == x, position))))))
        return state

    ret = []
    for x in dif:
        ret.append(([data[i] for i in xrange(N) if position[i] == x], (x, x)))
    return ret



if __name__ == 'main':
    pass
