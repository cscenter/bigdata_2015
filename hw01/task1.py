#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
# import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
import http_dfs as dfs

import time
import interval_tree as intr

# Контстанты
KEYS = "/keys"
PARTITIONS = "/partitions"

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
# использованы исключительно для демонстрации)
def demo():
    for f in dfs.files():
        print("File {0} consists of fragments {1}".format(f.name, f.chunks))

    for c in dfs.chunk_locations():
        print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

    # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
    # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
    # на использованных тут файл-серверах

    # При использовании test_dfs читаем из каталога cs0
    # chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

    # При использовании http_dfs читаем с данного сервера
    chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
    print("\nThe contents of chunk partitions:")
    for line in chunk_iterator:
        # удаляем символ перевода строки
        print(line[:-1])


def test():
    # print("\nTest: ")
    # for line in get_file_content(PARTITIONS):
    # print(line[:-1])

    print("\nTest: keys...")
    start_time = time.time()
    result = calculate_sum(KEYS)
    print("--- %s seconds ---" % str(time.time() - start_time))
    print("result:" + str(result))


# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/

def get_file_content(filename):
    # Находим из каких фрагментов состоит файл
    chunk_ids = find_chunks_for_file(filename)
    if chunk_ids is None:
        raise Exception("ERROR: Content for file: %s not found" % (filename))
    else:
        # Находим располождение фрагментов
        chunks_and_locations = get_unique_chunks_locations(chunk_ids)
        # Делаем построчный итератор контента
        for chunk_id, location_id in chunks_and_locations:
            for line in dfs.get_chunk_data(location_id, chunk_id):
                yield line
        raise StopIteration


# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
    partitions_content = get_file_content(PARTITIONS)
    key_intervals = parse_key_intervals(partitions_content)
    key_content = get_file_content(keys_filename)
    query_schedule_map_of_sets, sum_result_map = schedule_calculation(key_content, key_intervals)
    process_calculation(query_schedule_map_of_sets, sum_result_map)
    return sum(sum_result_map.itervalues())


def find_chunks_for_file(filename):
    """
    Выдает сисок чанков из которых состоит указанный файл.

    :param filename: (str) Имя файла
    :return:
        (list of str): Если описание для файла удалось найти
        None: Если файл не найден
    """
    for file_descriptor in dfs.files():
        if file_descriptor.name == filename:
            return file_descriptor.chunks
    return None


def get_unique_chunks_locations(chunk_ids):
    """
    Выдает адреса серверов где распологаются чанки. Следит за уникальностью адреса.
    В слуучае когда один и тот же чанк имеет несколько адресов бросает исключение.

    :param chunk_ids: (list of str) Список идентификаторов
    :return: (list of (str,str)) Список пар идентификатор,адрес [(id,location),...]
    :raise Exception: В случае если обнаружен чанк с несколькими адресами
    """
    chunk_map = dict.fromkeys(chunk_ids)
    for chunk_location in dfs.chunk_locations():
        chunk_id = chunk_location.id
        if chunk_id in chunk_map:
            location_id = chunk_location.chunkserver
            if chunk_map.get(chunk_id) is not None:
                raise Exception("ERROR: Duplicate chunk location. chunk_id:%s, chunk_locations:[%s,%s]  " % (
                    chunk_id, chunk_map.get(chunk_id), location_id))
            else:
                chunk_map[chunk_id] = location_id
    chunk_locations = map(lambda key: chunk_map[key], chunk_ids)
    return zip(chunk_ids, chunk_locations)


def parse_key_intervals(partitions_content):
    """
    Извлекает интервалы ключей для щардов и скалдывает в список

    :param partitions_content: итератор строк вида "str str str"
    :return: (list of Interval)
    :raise Exception: В случе если строка имеет неверный формт
    """
    intervals = []
    for partition in partitions_content:
        partition = partition.strip()
        if partition:
            try:
                star_key, finish_key, location = filter((lambda x: x), partition.split(" "))
            except:
                raise Exception(
                    "Malformed partition string. Expected format:\"{str} {str} {str}\", but found: %s" % partition)
            intervals.append(intr.Interval(location, star_key, finish_key))
    return intervals


def schedule_calculation(keys, intervals):
    """
    Планирует будущие вычисления. Строит таблицу рапсределяя ключи по шардам где их следует искать.
    Так как нет никакой информации о глобальном порядке ключей, приходится распределять ключи по шардам
    диапазоны которых соответсвуют ключу. Для этого используется структура данных - дерево интервалов.

    :param keys: Итератор исследуемых ключей
    :param intervals: Список интервалов
    :return: Возвращяет пару объектов:
        query_schedule_map_of_sets: (map of sets) Таблица ключем являются имена шардов а значением,
    множество исследуемых ключей.
        sum_result_map: (map) Таблица для сбора будущих результатов вычисдений, ключами являются
    множество исследуемых ключей, а все значения таблицы равны None
    """
    partition_interval_tree = intr.IntervalTree(intervals)
    query_schedule_map_of_sets = {}
    sum_result_map = {}
    for key in keys:
        key = key.strip()
        if key:
            if key not in sum_result_map:
                sum_result_map[key] = None
                partition_nodes = partition_interval_tree.search(key)
                for partition_node in partition_nodes:
                    location = partition_node.name
                    if location in query_schedule_map_of_sets:
                        query_schedule_map_of_sets[location].add(key)
                    else:
                        query_schedule_map_of_sets[location] = set([key])
    return query_schedule_map_of_sets, sum_result_map


def process_calculation(query_schedule_map_of_sets, sum_result_map):
    """
    Выполняет вычисления. Читает полседовательно данные из всех запланированных шардов,
    и сохраняет значения найденых ключей в таблице результатов.

    :param query_schedule_map_of_sets: (map of sets) Таблица ключем являются имена шардов а значением,
    множество исследуемых ключей.
    :param sum_result_map: (map) Таблица для сбора будущих результатов вычисдений, ключами являются
    множество исследуемых ключей, а все значения таблицы по умолчанию должны быть равны None
    :raise Exception: В случае если будут найдены дубликаты ключей на шардах.
    Ожидается, что все множество ключей разбито на непересекающиеся диапазоны.
    """
    for location, keys_set in query_schedule_map_of_sets.iteritems():
        # Хак на формат именования шардов от files приходит без слеша а от partitions со слешем в начале
        location = location[1:]
        file_content = get_file_content(location)
        for local_key, count in iterate_shard_content(file_content):
            if local_key in keys_set:
                if sum_result_map[local_key] is not None:
                    raise Exception("Expected unique key but found duplicate. Key:%s" % (local_key))
                else:
                    sum_result_map[local_key] = count


def iterate_shard_content(file_content):
    """
    Обертка для обхода содержимого файла в фомрме (ключ,количество)

    :param file_content: Итератор по строкам файла
    :return(iterator of (str,int) ) Возвращяет итератор по парам (ключ,количество)
    :raise Exception: В случае если формат файла будет неверный
    """
    for line in file_content:
        line = line.strip()
        if line:
            try:
                local_key, count_str = filter((lambda x: x), line.split(" "))
                count = int(count_str)
                yield local_key, count
            except:
                raise Exception("Malformed shard string. Expected format:\"{str} {int}\", but found: %s" % line)
    raise StopIteration


test()
