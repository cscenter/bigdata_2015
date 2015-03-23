class RLEList(object):
	def __init__(self):
		pass

	def append(self, value):
		pass

	def insert(self, index, value):
		pass

	def get(self, index):
		pass

	def iterator(self):
		pass

class MyIterator:
	def __init__(self, obj):
		self.curpos = 0
		self.curVal = 0
		self.obj = obj
		
        
	def __iter__(self):
		return self

	def next(self):
		if self.curpos >= self.obj.size:
			raise StopIteration()
		self.curVal += 1
		(a, b) = self.obj.impl[self.curpos]
		if b >= self.curVal:
			return self.obj.dic[a] 
		self.curpos += 1
		self.curVal = 1
		if self.curpos >= self.obj.size:
			raise StopIteration()
		(a, b) = self.obj.impl[self.curpos]
		return self.obj.dic[a]
				

class RLEListRefImpl(RLEList):
	def __init__(self):
		self.impl = []
		self.size = 0
		self.allSize = 0
		self.distinct = 0
		self.dic = {}
		self.dic2 = {}

	def append(self, value):
		self.insert(self.allSize, value)
	
	def add(self, index, value):
		self.impl.insert(index, (value, 1))
		self.size += 1

	def go(self, index):
		curpos = 0
		if index == 0:
			return 0
		pos = 1
		for (a,b) in self.impl:
			if curpos == index:
				return pos - 1
			np = curpos + b
			if np > index:
				l = np - index
				self.impl[pos - 1] = (a, b - l)
				self.impl.insert(pos, (a, l))
				self.size += 1
				return pos
			pos += 1
			curpos = np
		if curpos != index:
			return pos 
		return pos - 1

	def insert(self, index, value):

		if value in self.dic2:
			value = self.dic2[value]
		else:
			self.distinct += 1
			self.dic[self.distinct] = value
			self.dic2[value] = self.distinct
			value = self.distinct

		self.allSize += 1

		index = self.go(index)

		if index > self.size or index < 0:
			raise IndexError

		if self.size == 0:
			self.add(index, value)
			return

		if index == 0 and self.size > 0:
			(a, b) = self.impl[index]
			if a == value:
				b += 1
				self.impl[index] = (a, b)
				return
			self.add(index, value)
			return 

		if index == self.size and self.size > 0:
			(a, b) = self.impl[index - 1]
			if a == value:
				b += 1
				self.impl[index - 1] = (a, b)
				return
			self.add(index, value)
			return

		if index == 0:
			self.add(index, value)
			return

		(a, b) = self.impl[index - 1]
		if a == value:
			self.impl[index - 1] = (a, b + 1)
			return

		if index < self.size:
			(a, b) = self.impl[index]
			if a == value:
				self.impl[index] = (a, b + 1)
				return

		self.add(index, value)

	def get(self, index):
		index += 1
		curSum = 0
		for (a, b) in self.impl:
			curSum += b
			if curSum >= index:
				return self.dic[a]
		raise IndexError

	def iterator(self):
		return MyIterator(self)

if __name__ == '__main__':
	pass