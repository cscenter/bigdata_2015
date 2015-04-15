#encoding:utf-8
import unittest


class RLEList(object):
  def __init__(self):
    self.impl = []
    self.size = 0

  def append(self, value):
    if self.impl:
      if self.impl[-1][1] == value:
        self.impl[-1][0] += 1
      else:
        self.impl.append([1, value])
    else:
      self.impl.append([1, value])   
    self.size += 1  

  def insert(self, index, value):
    next_ind = self.size - 1
    if index == next_ind:
      self.append(value)
    elif index > next_ind:
      dif = index - next_ind - 1
      for i in range(0, dif):
        self.append(None)
      self.append(value)
      
    else:
      inf = self.get_more(index)
      if index > 0:
        prv = self.get(index-1)
      else:
        prv = None
      ind, nxt, rpt, grEndInd = inf
      grStartInd = grEndInd - rpt + 1
      
      if (prv == nxt == value) or (nxt == value):
        self.impl[ind][0] += 1
      elif prv == value:
        self.impl[ind-1][0] += 1       
      else:  
        if prv == nxt:
          prv_rpt = index - grStartInd
          nxt_rpt = grEndInd - index +1
          self.impl[ind][0] = prv_rpt 
          self.impl.insert(ind+1, [1, value])
          self.impl.insert(ind+2, [nxt_rpt,nxt])
        else:
          self.impl.insert(ind, [1, value])
    
    self.size += 1
       
  def get(self, index):
    if index > self.size -1:
      return None
    else:
      return self.get_more(index)[1]  
  
  
  def get_more(self, index):
    grEndInd = -1 #здесь будем хранить номер последнего элемента каждой группы одиаковых элементов
    for i in range(0, len(self.impl)-1):
      grEndInd += self.impl[i][0]
      if grEndInd >= index:
        value = self.impl[i][1]
        rpt = self.impl[i][0]
        return [i, value, rpt, grEndInd]

      
  def iterator(self):
    for w in self.impl:
			for i in range(0, w[0]):
				yield w[1]
               

class Test(unittest.TestCase):
  def setUp(self):
    self.lst = RLEList()
    self.lst.append('foo') 
    self.lst.append('foo') 
    self.lst.append('bar') 
    self.lst.append('bar') 
    self.lst.append('bar') 
    self.lst.append('foobar') 

  def test_append(self):
    self.lst.append('foobar')
    self.lst.append('bar')
    self.lst.append('bar')
    self.assertEqual([[2, 'foo'], [3, 'bar'], [2, 'foobar'], [2, 'bar']], self.lst.impl) 
   
  def test_insert(self):
    self.lst.insert(1, 'foo')
    self.assertEqual([[3, 'foo'], [3, 'bar'], [1, 'foobar']], self.lst.impl)
    self.lst.insert(3, 'bar')
    self.assertEqual([[3, 'foo'], [4, 'bar'], [1, 'foobar']], self.lst.impl)
    self.lst.insert(4, 'foo')
    self.assertEqual([[3, 'foo'], [1, 'bar'], [1, 'foo'], [3, 'bar'], [1, 'foobar']], self.lst.impl)
    self.lst.insert(6, 'bar')
    self.assertEqual([[3, 'foo'], [1, 'bar'], [1, 'foo'], [4, 'bar'], [1, 'foobar']], self.lst.impl)
    self.lst.insert(12, 'barfoo')
    self.assertEqual([[3, 'foo'], [1, 'bar'], [1, 'foo'], [4, 'bar'], [1, 'foobar'], [2, None], [1, 'barfoo']], self.lst.impl)
    self.lst.insert(0, 'bar')
    self.assertEqual([[1, 'bar'], [3, 'foo'], [1, 'bar'], [1, 'foo'], [4, 'bar'], [1, 'foobar'], [2, None], [1, 'barfoo']], self.lst.impl)
    self.lst.insert(14, 'brfoo')
    self.assertEqual([[1, 'bar'], [3, 'foo'], [1, 'bar'], [1, 'foo'], [4, 'bar'], [1, 'foobar'], [2, None], [1, 'barfoo'], [1, 'brfoo']], self.lst.impl)  
  
  def test_iter(self):
    itr = self.lst.iterator()
    self.assertEqual('foo', itr.next())
    self.assertEqual('foo', itr.next())
    self.assertEqual('bar', itr.next())    
    
 
  def test_get(self):
    self.assertEqual('foo', self.lst.get(0))
    self.assertEqual('bar', self.lst.get(3))
    self.assertEqual(None, self.lst.get(100))    
  
         
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
  
unittest.main()