#!/usr/bin/python
# encoding: utf8


# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
#import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
server = {}
list_of_chunks = {}
def partitions_handling(f):
    chunk_iterator = dfs.get_chunk_data(f.chunkserver, f.id);
    list_of_ranges = {}
    for line in chunk_iterator:
        words = line[:-1].split( )     #разбиваем строку файла
        count = 0
        for w in words:                  #считаем количество лексем
            count += 1                   #если лексем 3 (т.е. левая граница, правая граница и имя фрагмента),
        if count == 3:                   #то все ок, можем обрабатывать, иначе мусор
            if '/' in words[2]:          #удаляем / из имени файла, если он есть
                i = words[2].index('/')  #так как в реальной РФС в названиях файлов / то был, то его не было
                s1 = words[2][:i]+words[2][i+1:]      #а ключи должны быть одинаковые
            else:
                s1 = words[2]

            # словарь хранит для каждого имени файла список его диапазонов, отсортированных по возрастанию
            # левая и правая граница тоже составляют список из 2 элементов
            # i-й диапазон для файла находится в его i-м фрагменте.
            list_of_ranges.setdefault(s1, []).append([words[0], words[1]])
    return list_of_ranges

def files_handling(name_of_partitions):
    i = 0
    for f in dfs.files():   #обрабатываем файл files
        if '/' in f.name:   #удаляем /, если он есть
            i = f.name.index('/')
            s1 = f.name[:i] + f.name[i+1:]
        else:
            s1 = f.name
        if f.name != name_of_partitions:     #если это не файл partitions
            for t in f.chunks:
                 list_of_chunks.setdefault(s1, []).append(t)    # сохраняем для каждого файла список его фрагментов
                                                                # i-й элемент списка фрагментов файла d
                                                                # list_of_chunks[d][i] соответствует i-му элементу
                                                                # списку диапазонов для этого файла
                                                                # list_of_ranges[d][i]

    for c in dfs.chunk_locations():
        server[c.id] = c.chunkserver;

# Функция принимает имя файла и возвращает итератор по его строкам.

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
                chunk_iterator = dfs.get_chunk_data(server[list_of_chunks[d][i]], list_of_chunks[d][i])
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
        chunks_of_file = {}
        if 'partitions' in f.name:
            name_of_partitions = f.chunks[0] #запоминаем имя фрагмента файла partitions
        if f.name == keys_filename:
            name_of_keys = f.chunks[0]       #запоминаем имя фрагмента файла keys
        chunks_of_file[f.name] = f.chunks    #запоминаем для каждого файла список его фрагментов в словаре
   for c in dfs.chunk_locations():
        if (c.id == name_of_partitions):
            partitions_handling(c, name_of_partitions);    #обрабатываем файл partitions
        if (c.id == name_of_keys):
            server_of_keys = c.chunkserver    #запоминаем имя сервера, на котором лежит файл keys

   chunk_iterator = dfs.get_chunk_data(server_of_keys, name_of_keys)
   for line in chunk_iterator:
        key = line[:-1]
        sum += sum_key(key)
   print(sum)
   return sum

calculate_sum("/keys")
