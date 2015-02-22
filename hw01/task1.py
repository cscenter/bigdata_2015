#!/usr/bin/python
# encoding: utf8


#
# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
#import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
server = {}
dpz = {}
d1 = {}
def myFun(f, name_of_partitions):
    chunk_iterator = dfs.get_chunk_data(f.chunkserver, f.id);
    for line in chunk_iterator:
        words = line[:-1].split( )
        count = 0
        for w in words:
            count += 1
        if count == 3:
            if '/' in words[2]:
                i = words[2].index('/')
                s1 = words[2][:i]+words[2][i+1:]
            else:
                s1 = words[2]
            d1.setdefault(s1, []).append([words[0], words[1]] )
    i = 0
    for f in dfs.files():
        if '/' in f.name:
            i = f.name.index('/')
            s1 = f.name[:i] + f.name[i+1:]
        else:
            s1 = f.name
        if f.name != name_of_partitions:
            for t in f.chunks:
                 dpz.setdefault(s1, []).append(t)
        #if f.name[1] != 'p':
            #for c in d1[f.name]:
             #   dpz[{c[0], c[1]}] = f.chunks[i]
             #   i+=1
    for c in dfs.chunk_locations():
        server[c.id] = c.chunkserver;

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
  #chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  #print("\nThe contents of chunk partitions:")
  #for line in chunk_iterator:
    # удаляем символ перевода строки
   # print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  file = open(filename)
  for line in file:
     yield line

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def sum_key(key_name):
    sum = 0
    for d in d1:
        #print(d)
        #print(d1[d])
        i = 0
        for j in d1[d]:
            #print(key_name + " " + j[0] + " " + j[1])
            if key_name >= j[0] and key_name <= j[1]:
                print(server[dpz[d][i]], dpz[d][i])
                chunk_iterator = dfs.get_chunk_data(server[dpz[d][i]], dpz[d][i])
                for line in chunk_iterator:
                    mas = line[:-1].split( )
                    dv = 0
                    for m in mas:
                        dv += 1
                    if dv != 0:
                        if mas[0] == key_name:
                            sum += int(mas[1])
                            break
                break

            i += 1
    return sum

def calculate_sum(keys_filename):
   name_of_partitions = ''
   name_of_keys = ''
   server_of_keys = ''
   sum = 0
   for f in dfs.files():
        d = {}
        if f.name == '/partitions':
            name_of_partitions = f.chunks[0]

        if f.name == keys_filename:
            name_of_keys = f.chunks[0]
        d[f.name] = f.chunks
   for c in dfs.chunk_locations():
        if (c.id == name_of_partitions):
            myFun(c, name_of_partitions);
        if (c.id == name_of_keys):
            server_of_keys = c.chunkserver

   chunk_iterator = dfs.get_chunk_data(server_of_keys, name_of_keys)
   for line in chunk_iterator:
        key = line[:-1]
        sum += sum_key(key)
   print(sum)
   return sum

calculate_sum("/keys")



