import unittest


class RLEList(object):
	def __init__(self):
		self.impl = []
		self.lenght = 0

	def append(self, value):
		if self.impl:
			if self.impl[-1][0] == value:
				self.impl[-1][1] += 1
			else:
				self.impl.append([value, 1])			
		else:
			self.impl.append([value, 1])
		self.lenght += 1			

	def insert(self, index, value):
		if self.lenght == index:
			self.append(value)

		elif self.lenght < index:
			self.impl.append([None, index - self.lenght])
			self.impl.append([value, 1])
			self.lenght += index - self.lenght + 1			

		else:
			real_index, counter = self.__get_real_index(index)
			if self.impl[real_index][0] == value:
				self.impl[real_index][1] += 1
			elif self.impl[real_index - 1][0] == value and ((real_index - 1) >= 0) and (index - counter) == 0:
				self.impl[real_index - 1][1] += 1
			else:
				diff = index - counter
				d1 = [self.impl[real_index][0], diff]
				d2 = [value, 1]
				d3 = [self.impl[real_index][0], self.impl[real_index][1] - diff]
				if diff == 0:
					self.impl[real_index:(real_index+1)] = [d2, d3]
				else:
					self.impl[real_index:(real_index+1)] = [d1, d2, d3]
			self.lenght += 1

	def get(self, index):
		real_index = self.__get_real_index(index)
		if real_index != -1:
			return self.impl[real_index[0]][0]
		else:
			return None

	def __get_real_index(self, index):
		counter = 0
		real_index = 0
		for word in self.impl:
			if counter + word[1] > index:
				return (real_index, counter)
			else:
				counter += word[1]
				real_index += 1
		return -1

	def iterator(self):
		for word in self.impl:
			for i in range(0, word[1]):
				yield word[0]


class TestRLEList(unittest.TestCase):
	def setUp(self):
		self.myList = RLEList()
		self.myList.append("foo")
		self.myList.append("foo")
		self.myList.append("bar")
		self.myList.append("foo")

	def test_get(self):
		self.assertEqual('foo', self.myList.get(0))
		self.assertEqual('foo', self.myList.get(3))
		self.assertEqual(None, self.myList.get(25))

	def test_iterator(self):
		myIter = self.myList.iterator()
		self.assertEqual('foo', myIter.next())
		self.assertEqual('foo', myIter.next())
		self.assertEqual('bar', myIter.next())

	def test_append(self):
		self.myList.append("bar")
		self.myList.append("bar")
		self.myList.append("bar")
		self.myList.append("bar")
		self.myList.append("bar")
		self.myList.append("bar")
		self.myList.append("bar")
		self.assertEqual([['foo', 2], ['bar', 1], ['foo', 1], ['bar', 7]], self.myList.impl)

	def test_insert(self):
		self.assertEqual([['foo', 2], ['bar', 1], ['foo', 1]], self.myList.impl)
		self.myList.insert(3,'foo')
		self.assertEqual([['foo', 2], ['bar', 1], ['foo', 2]], self.myList.impl)
		self.myList.insert(0,'dd')
		self.assertEqual([['dd', 1], ['foo', 2], ['bar', 1], ['foo', 2]], self.myList.impl)
		self.myList.insert(5,'foo')
		self.assertEqual([['dd', 1], ['foo', 2], ['bar', 1], ['foo', 3]], self.myList.impl)
		self.myList.insert(7,'bar')
		self.assertEqual([['dd', 1], ['foo', 2], ['bar', 1], ['foo', 3], ['bar', 1]], self.myList.impl)
		self.myList.insert(20,'bar')
		self.assertEqual([['dd', 1], ['foo', 2], ['bar', 1], ['foo', 3], ['bar', 1], [None, 12], ['bar', 1]], self.myList.impl)
		self.myList.insert(5,'bar')
		self.assertEqual([['dd', 1], ['foo', 2], ['bar', 1], ['foo', 1], ['bar', 1], ['foo', 2], ['bar', 1], [None, 12], ['bar', 1]], self.myList.impl)
		self.myList.insert(4,'bar')
		self.assertEqual([['dd', 1], ['foo', 2], ['bar', 2], ['foo', 1], ['bar', 1], ['foo', 2], ['bar', 1], [None, 12], ['bar', 1]], self.myList.impl)

class RLEListRefImpl(RLEList):
	def __init__(self):
		self.impl = []

	def append(self, value):
		self.impl.append(value)

	def insert(self, index, value):
		self.impl.insert(index, value)

	def get(self, index):
		return self.impl[index]

	def iterator(self):
		return iter(self.impl)

def demo():
	list = RLEListRefImpl()
	list.append("foo")
	list.insert(0, "bar")
	print list.iterator().next()
	print list.get(1)

unittest.main()
