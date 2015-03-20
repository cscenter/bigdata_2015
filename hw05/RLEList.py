from collections import namedtuple
# Record = namedtuple("Record", ["name", "number"])
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

class RLEListRefImpl(RLEList):
	def __init__(self):
		self.impl = []

	def append(self, value):
		if self.impl:
			if self.impl[-1][0] == value:
				self.impl[-1][1] += 1
			else:
				self.impl.append([value, 1])
		else:
			self.impl.append([value, 1])

	def insert(self, index, value):
		i = 0
		j = 0
		while i < index:
			i += self.impl[j][1]
			j += 1
		# insert same
		if (j > 0) and (value == self.impl[j-1][0]):
			self.impl[j-1][1] += 1
		elif (j < len(self.impl)) and (value == self.impl[j][0]):
			self.impl[j][1] += 1
		else:
			# insert after seria
			if index == i:
				self.impl.insert(j, [value, 1])
			else: 
				self.impl[j-1][1] -= i-index
				self.impl.insert(j, [value, 1])
				self.impl.insert(j+1, [self.impl[j-1][0], i-index])

	def get(self, index):
		i = 0
		j = 0
		while i <= index:
			i += self.impl[j][1]
			j += 1
		return self.impl[j-1][0] if j else self.impl[j][0]

	def iterator(self):
		c_st = [0, 0]
		while not((c_st[0] == len(self.impl)-1) and (c_st[1] == self.impl[-1][1])):
			if self.impl[c_st[0]][1] >= (c_st[1] + 1):
				yield self.impl[c_st[0]][0]
				c_st[1] += 1
			else:
				c_st[0] += 1
				c_st[1] = 1
				yield self.impl[c_st[0]][0]


def demo():
	list = RLEListRefImpl()
	list.append("rar")
	list.append("foo")
	list.append("foo")
	list.append("foo")
	list.append("foo")
	list.append("bar")
	list.append("rar")
	list.append("rar")
	list.append("rar")
	list.insert(0, "rar")
	list.insert(0, "bar")
	print list.impl
	# print list.get(0)
	# print list.get(1)
	# print list.get(2)
	# print list.get(3)
	# print list.get(4)
	# print list.get(5)
	# print list.get(6)
	# print list.get(7)
	# print list.get(8)
	# ite = list.iterator()
	for i in list.iterator():
		print i

if __name__ == '__main__':
	demo()


