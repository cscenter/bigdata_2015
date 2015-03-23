from RLE import RLEListRefImpl

import unittest

class MyTestCase(unittest.TestCase):
    def compare_list(self, a, b):
        self.assertEqual(len(a), len(b))
        for i in range(len(a)):
            self.assertEqual(a[i], b[i])

    def test_init(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl(l)
        self.compare_list(l, list)

    def test_len(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl(l)
        self.assertEqual(len(l), len(list))

    def test_append(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl([])
        for i in range(len(l)):
            list.append(l[i])
            self.assertEqual(list[i], l[i])

    def test_iter(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl(l)
        test_l = []
        for val in list:
            test_l.append(val)
        self.compare_list(l, list)

    def test_insert_begin(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl([])
        for i in range(len(l)):
            list.insert(0, l[::-1][i])
        self.compare_list(l, list)

    def test_insert_end(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl([])
        for i in range(len(l)):
            list.insert(i, l[i])
        self.compare_list(l, list)

    def test_insert_middle(self):
        l = [1,1,2,3,3,3]
        list = RLEListRefImpl(l)
        l.insert(1, 1)
        list.insert(1, 1)
        self.compare_list(l, list)
        l.insert(3, 2)
        list.insert(3, 2)
        self.compare_list(l, list)


if __name__ == '__main__':
    unittest.main()
