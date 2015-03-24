from unittest import TestCase
import RLEList

class TestRLEListRefImpl(TestCase):
    def setUp(self):
        self.list = RLEList.RLEListRefImpl()

    def test_append(self):
        self.list.append('1')
        self.list.append('1')
        self.list.append('1')
        self.list.append('2')
        self.list.append('2')
        self.list.append('3')
        self.list.append('4')
        self.list.append('4')
        self.assertSequenceEqual(self.list.impl, [('1', 3, 3), ('2', 2, 5), ('3', 1, 6), ('4', 2, 8)])

    #todo: expected exception test for out of range
    def test_insert(self):
        self.list.insert(0, 'first')
        self.list.insert(1, 'second')
        self.list.insert(2, 'third')
        self.list.insert(1, 'second')
        self.assertSequenceEqual(self.list.impl, [('first', 1, 1), ('second', 2, 3), ('third', 1, 4)])

    def test_get(self):
        self.list.append('1')
        self.list.append('1')
        self.list.append('2')
        self.list.append('2')
        self.list.append('3')
        self.assertEquals(self.list.get(3), '2')

    def test_iterator(self):
        self.list.append('1')
        self.list.append('1')
        self.list.append('2')
        self.list.append('2')
        self.list.append('3')
        self.assertSequenceEqual(list(self.list.iterator()), ['1', '1', '2', '2', '3'])