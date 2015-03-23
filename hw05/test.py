import RLEList
import unittest
import random

class TestRLEListRefImpl(unittest.TestCase):
	class ListRef(RLEList.RLEList):
		def __init__(self):
			self.impl = []
		
		def __len__(self):
			return len(self.impl)

		def append(self, value):
			self.impl.append(value)

		def insert(self, index, value):
			self.impl.insert(index, value)

		def get(self, index):
			return self.impl[index]

		def iterator(self):
			return iter(self.impl)
	
	def test_append_iterator_eq(self):
		self.assertEqual([l for l in self.test_list.iterator()], [l for l in self.ref_list.iterator()])

	def test_append_get_eq(self):
		self.assertEqual([self.test_list.get(l) for l in xrange(len(self.test_list))], 
			[self.ref_list.get(l) for l in xrange(len(self.ref_list))])

	def test_insert_iterator_eq(self):
		for i in xrange(500):
			place = random.randint(0,len(self.ref_list))
			f_n = 0
			s_n = 1
			t_n = 2
			rlt = 0
			rnd = random.random()
			if rnd < 0.8:
				rlt = 0
			elif rnd < 0.99:
				rlt = s_n
			else:
				rlt = t_n

			self.ref_list.insert(place, rlt)
			self.test_list.insert(place, rlt)

		self.assertEqual([l for l in self.test_list.iterator()], [l for l in self.ref_list.iterator()])


	def setUp(self):
		self.test_list = RLEList.RLEListRefImpl()
		self.ref_list = self.ListRef()
		f_n = 0
		s_n = 1
		t_n = 2
		rlt = 0
		for i in xrange (0, 1000):
			rnd = random.random()
			if rnd < 0.8:
				rlt = 0
			elif rnd < 0.99:
				rlt = s_n
			else:
				rlt = t_n
			self.test_list.append(rlt)
			self.ref_list.append(rlt)


# RLEList.demo()
unittest.main()
