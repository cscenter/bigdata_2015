#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
№import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
def demo():
  for f in dfs.files():
    print("File {0} consists of fragments {1}".format(f.name, f.chunks))

  for c in dfs.chunk_locations():
    print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  print("\nThe contents of chunk partitions:")
  with dfs.get_chunk_data("cs0", "partitions") as f:
    for line in f:
      # удаляем символ перевода строки 
      print(line[:-1])

#################################################################################
# Эту функцию надо реализовать. Функция принимает имя файла и 
# возвращает итератор по его строкам. 
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  looking_file = None
  for f in dfs.files():
    if f.name == filename:
      looking_file = f
      break
  if (looking_file == None): raise Exception("Can't find file")
  
  for chunk in looking_file.chunks: 
    server = None
    for c in dfs.chunk_locations(): 
      if  c.id == chunk :
      	server = c
      	break
    if (server == None): raise Exception("Can't find server")
    
    with dfs.get_chunk_data(server.chunkserver, chunk) as part_file:
      for line in part_file:
        yield(line[:-1])       # удаляем символ перевода строки 
  return        
####################################################################################
# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает 
# число
# я полагаю что ключи не отсортированы и надо бы их отсортировать.
# ну и пока надеюсь что весь файл с ключами вместится в память. 
# 
def calculate_sum(keys_filename = "./data/keys"): 
  f = open(keys_filename, "r")
  s = f.read()
  f.close()
  keys = sorted(list((set(s.split('\n')))))
  if len(keys[0]) < 1: keys = keys[1:]
  
  myCoolSum = 0;
  index = 0
  keysLen = len(keys)
  if (keysLen < 1) : raise Exception("Key is not exists")

  for diapazon in get_file_content("/partitions"):
    left, right, filename = diapazon.split()
    for i in range(index, keysLen):
      if (left <= keys[i]) and (keys[i] <= right) :
        for dictionary in get_file_content(filename):
       	  if (len(dictionary) < 2) : continue #считаем что это пустая строка
          key, value = dictionary.split()
          if (key == keys[i]):
            myCoolSum += int(value);
            break
      else :
        index = i
        break
  return myCoolSum;

#########           ################
print("sum = ", calculate_sum())




























































