import random
import unittest
import RLEList

class TestSequenceFunctions(unittest.TestCase):
	
	def setUp(self):
		self.seq = RLEList.RLEListRefImpl()

	def test_add(self):
		self.seq.append('Anton')
    
	def test_get(self):
		self.seq.append('Anton')
		self.assertEqual(self.seq.get(0), 'Anton')

	def test_insert(self):	
		self.seq.insert(0, 'A')
		self.seq.insert(1, 'B')
		self.seq.insert(0, 'A')
		self.seq.insert(1, 'A')
		ret = ''
		for i in xrange(4):
			ret += self.seq.get(i)
		self.assertEqual(ret, 'AAAB')

	def test_1iter(self):
		self.seq.append('A')
#		print repr(self.seq.impl), self.seq.size, self.seq.allSize
		self.seq.append('B')
#		print repr(self.seq.impl), self.seq.size, self.seq.allSize	
		self.seq.insert(0, 'B')
#		print repr(self.seq.impl), self.seq.size, self.seq.allSize
		self.seq.insert(1, 'B')
#		print repr(self.seq.impl), self.seq.size, self.seq.allSize
		self.seq.append('A')
#		print repr(self.seq.impl), self.seq.size, self.seq.allSize
		self.seq.insert(3, 'A')
#		print repr(self.seq.impl), self.seq.size, self.seq.allSize
		a = self.seq.iterator()
		ret = ""
		for i in range(6):
			ret += a.next()
		self.assertEqual(ret, 'BBAABA')


	def test_2iter(self):
		self.seq.append(0)
		self.seq.append(1)
		self.seq.append(1)
		self.seq.append(1)
		self.seq.insert(0,0)
		self.seq.insert(2, 0)
		self.seq.insert(4, 1)
		self.seq.insert(3, 0)
		a = self.seq.iterator()
		b = self.seq.iterator()
		res1 = 0
		res2 = 0
		for i in range(8):
			res1 = res1 * 10 + a.next()
			res2 = res2 * 10 + b.next()
		self.assertEqual(True, (res1 == res2) and (res1 == 1111));

if __name__ == '__main__':
    unittest.main()