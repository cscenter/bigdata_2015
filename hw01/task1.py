#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
#import test_dfs as dfs

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

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
    chunks = []
    for f in dfs.files():
        if f.name == filename:
            chunks = f.chunks
            break
    for chunk in chunks:
        cs = ""
        for c in dfs.chunk_locations():
            if (c.id == chunk):
                cs = c.chunkserver
                break
        for line in dfs.get_chunk_data(cs, chunk):
            yield line

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    sum = 0
    for line in get_file_content(keys_filename):
        key = line[:-1]     #убираем перевод строки
        if not key:         #если пустая строка, читаем следующую
            continue
        shard = ""
        for s in get_file_content("/partitions"):
            interval = s.split()
            if interval:        #если не пустая строка
                if (key > interval[0]) and (key < interval[1]):     #если key лежит в интервале
                    shard = interval[2]         #нашли в каком файле искать key
                    break
        chunks = []
            #нужно удалить след. строчку, если в files добавят к именам шардов слеш
            #иначе файлы не будут находится и ответ будет всегда 0
        shard = shard[1:]          #убираем слеш из начала имени файла
        for f in dfs.files():
            if f.name == shard:
                chunks = f.chunks           #нашли все фрагменты этого файла
                break
        is_find = False         #ключ не найден
        for chunk in chunks:
            if is_find:     #если ключ найден
                break       #переходим к следующему ключу
            cs = ""
            for c in dfs.chunk_locations():
                if (c.id == chunk):
                    cs = c.chunkserver      #нашли сервер текущего фрагмента
                    break
            for str in dfs.get_chunk_data(cs, chunk):
                information = str.split()
                if information:                 #если текущая строка фрагмента не пуста
                    if information[0] > key:        #если она лексографически больше, то
                        break                           #в этом фрагменте уже не будет ключа
                    if information[0] == key:
                        is_find = True                  #нашли ключ
                        sum += int(information[1])      #суммируем
                        break                   #переходим к следующему фрагменту
    return sum

#demo()
#print(calculate_sum("/keys"))
