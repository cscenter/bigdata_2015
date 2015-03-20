#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import itertools
import random
import rlelist
import unittest


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.seq = list(range(10))
        self.length = random.randint(100, 200)
        self.list = [random.random() for _ in range(self.length)]
        self.rlelist = rlelist.RLEListTrueImpl()
        for x in self.list:
            self.rlelist.append(x)

    def test_append(self):
        tlist = rlelist.RLEListTrueImpl()
        xs = ['Foo', 'Bar', 'Foo', 'Foo']
        for x in xs:
            tlist.append(x)

        implit = iter(tlist.impl)
        e = next(implit)
        self.assertEqual(e.value, 'Foo')
        self.assertEqual(e.count, 1)
        e = next(implit)
        self.assertEqual(e.value, 'Bar')
        self.assertEqual(e.count, 1)
        e = next(implit)
        self.assertEqual(e.value, 'Foo')
        self.assertEqual(e.count, 2)

    def test_iterator(self):
        for x, y in itertools.zip_longest(self.list, self.rlelist.iterator()):
            self.assertIsNotNone(x)
            self.assertIsNotNone(y)
            self.assertEqual(x, y)

    def test_get(self):
        for _ in range(100):
            i = random.randrange(self.length)
            self.assertEqual(self.list[i], self.rlelist.get(i))

    def test_insert(self):
        for _ in range(100):
            v = random.random()
            i = random.randrange(len(self.list))
            self.list.insert(i, v)
            self.rlelist.insert(i, v)
            self.assertEqual(self.list[i], self.rlelist.get(i))


if __name__ == '__main__':
    unittest.main()
