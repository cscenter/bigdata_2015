import random

class TreeNode(object):
	
	def __init__(self, type_, y_, cnt1_ = 1, ,size2_ = 1, size_ = 1, left_ = 0, right_ = 0):
		self.x = type_
		self.y = y_
		self.cnt = cnt1_
		self.size = size_
		self.size2 = size2_
		self.left = left_
		self.right = right_


	def increment(self):
		self.cnt1 += 1


class Treap(object):

	def __init__(self):
		self.root = 0
		items = [TreeNode(-1, random.randint(1, 10**9))]
		self.size = 0

	def upd(self, pos):
		self.items[pos].size = self.items[self.items[pos].left].size + self.items[self.items[pos].right].size + 1
		self.items[pos].cnt = self.it

	def merge(self, t1, t2):
		if ((t1 == 0) or (t2 == 0)) :
			return t1 + t2
		if items[t1].y > items[t2].y :
			items[t1].right = merge(items[t1].right, t2)
			upd(t1)
			return t1
		else:
			items[t2].left = merge(t1, items[t2].left)
			upd(t2)
			return t2

	def split(self, t1, t2):
		if t2 <= 0 :
			return (0, t1)
		if t2 >= self.items[t1].size :
			return (t1, 0)
		if t2 <= self.items[self.items[t1].left].size:
			(t3, t4) = split(self.items[t1].left, t2)
			self.items[t1].left = t4
			upd(t1)
			t4 = t1
		else:
			(t3, t4) = split(self.items[t1].right, t2 - self.items[self.items[t1].left].size - 1)
			self.items[t1].right = t3
			upd(t1)
			t3 = t1
		return (t3, t4)

	def get_type(self, pos, key):
		if key > self.size:
			return -1
		leftSize = self.items[self.items[pos].left].size
		if leftSize <= key
			return get_type(self.items[pos].left, key)
		if key == leftSize + 1
			return 
		
		
		

if __name__ == '__main__':
	main()