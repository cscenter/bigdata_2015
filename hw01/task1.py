#!/usr/bin/python
# encoding: utf8


# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
#import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
# использованы исключительно для демонстрации)


def partitions_handling(f):        # Функция обрабатывает файл partitions
    chunk_iterator = dfs.get_chunk_data(f.chunkserver, f.id);
    list_of_ranges = {}
    for line in chunk_iterator:
        words = line[:-1].split( )     # разбиваем строку файла
        count = 0
        for w in words:                  # считаем количество лексем
            count += 1                   # если лексем 3 (т.е. левая граница, правая граница и имя фрагмента),
        if count == 3:                   # то все ок, можем обрабатывать, иначе мусор
            if '/' in words[2]:          # удаляем / из имени файла, если он есть
                i = words[2].index('/')  # так как в реальной РФС в названиях файлов / то был, то его не было
                s1 = words[2][:i]+words[2][i+1:]      # а ключи должны быть одинаковые
            else:
                s1 = words[2]

            # словарь хранит для каждого имени файла список его диапазонов, отсортированных по возрастанию
            # левая и правая граница тоже составляют список из 2 элементов
            # i-й диапазон для файла находится в его i-м фрагменте.
            list_of_ranges.setdefault(s1, []).append([words[0], words[1]])
    return list_of_ranges

def files_handling(name_of_partitions):     # Функция обрабатывает файл files
    i = 0
    list_of_chunks = {}
    for f in dfs.files():   # обрабатываем файл files
        if '/' in f.name:   # удаляем /, если он есть
            i = f.name.index('/')
            s1 = f.name[:i] + f.name[i+1:]
        else:
            s1 = f.name
        if f.name != name_of_partitions:     # если это не файл partitions
            for t in f.chunks:
                 list_of_chunks.setdefault(s1, []).append(t)    # сохраняем для каждого файла список его фрагментов
                                                                # i-й элемент списка фрагментов файла d
                                                                # list_of_chunks[d][i] соответствует i-му элементу
                                                                # списку диапазонов для этого файла
                                                                # list_of_ranges[d][i]
    return list_of_chunks

def chunk_locations_handling():
    list_of_servers = {}
    for c in dfs.chunk_locations():                      # сохраняем для каждого фрагмента сервер, на котором
        list_of_servers[c.id] = c.chunkserver;           # он лежит
    return list_of_servers

# Функция принимает имя файла и возвращает итератор по его строкам.

def get_file_content(filename):
  file = open(filename)
  for line in file:
     yield line

# Функция находит значение ключа
def key_value(key_name, list_of_ranges, list_of_chunks, list_of_servers):
    value = 0
    for d in list_of_ranges:
        i = 0    # номер элемента списка для данного файла (номер диапазона равен номеру фрагмента)
        for j in list_of_ranges[d]:
            if key_name >= j[0] and key_name <= j[1]:    # если ключ попадает в диапазон
                # открываем нужный файл
                chunk_iterator = dfs.get_chunk_data(list_of_servers[list_of_chunks[d][i]], list_of_chunks[d][i])
                for line in chunk_iterator:           # считываем строку из нужного файла с ключами и значениями
                    mas = line[:-1].split( )          # разделяем на лексемы
                    dv = 0
                    for m in mas:                     # считаем количество лексем
                        dv += 1
                    if dv == 2:                       # если их 2 (ключ-значение), то
                        if mas[0] == key_name:        # проверяем, совпадает ли ключ в строке с ключом, который ищем
                            value = int(mas[1])        # если совпадает, нашли значение ключа
                            break
                break

            i += 1
    return value

# Функция принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
   name_of_partitions = ''
   name_of_keys = ''
   server_of_keys = ''
   sum = 0
   for f in dfs.files():
        chunks_of_file = {}
        if 'partitions' in f.name:
            name_of_partitions = f.chunks[0] # запоминаем имя фрагмента файла partitions
        if f.name == keys_filename:
            name_of_keys = f.chunks[0]       # запоминаем имя фрагмента файла keys
        chunks_of_file[f.name] = f.chunks    # запоминаем для каждого файла список его фрагментов в словаре
   for c in dfs.chunk_locations():
        if (c.id == name_of_partitions):
            list_of_ranges = partitions_handling(c);    # обрабатываем файл partitions
        if (c.id == name_of_keys):
            server_of_keys = c.chunkserver    # запоминаем имя сервера, на котором лежит файл keys

   list_of_chunks =  files_handling(name_of_partitions)  # обрабатываем файл files
   list_of_servers = chunk_locations_handling()
   # Считываем файл keys и для каждого ключа находим значение
   chunk_iterator = dfs.get_chunk_data(server_of_keys, name_of_keys)
   for line in chunk_iterator:
        key = line[:-1]
        sum += key_value(key, list_of_ranges, list_of_chunks, list_of_servers)
   print(sum)
   return sum

calculate_sum("/keys")
