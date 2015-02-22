#!/usr/bin/python
# encoding: utf8

import http_dfs as dfs


files_to_chunk_servers_map = {}


def get_file_content(filename):
    for line in dfs.get_chunk_data(get_chunk_server(filename), filename):
        if line.strip():
            yield line.strip()


def get_chunk_server(filename):
    if filename in files_to_chunk_servers_map:
        return files_to_chunk_servers_map[filename]
    for c in dfs.chunk_locations():
        if c.id == filename:
            files_to_chunk_servers_map[filename] = c.chunkserver
            return c.chunkserver
    raise FileNotFoundError('File {} was not found'.format(filename))


def get_shard_by_key(key):
    for range_shard_pair in get_file_content('partitions'):
        lower_bound, upper_bound, shard = range_shard_pair.split()
        if lower_bound <= key <= upper_bound:
            for f in dfs.files():
                # Slicing had been made due to shard filename incompatibility
                if f.name == shard[1:]:
                    return f
    raise Exception('There\'s no such key as {} in the dfs'.format(key))


def get_first_line_only(filename):
    generator = get_file_content(filename)
    first_line = next(generator)
    generator.close()
    return first_line


def find_value_in_chunk(filename, key):
    for key_value_pair in get_file_content(filename):
        that_key, that_value = key_value_pair.split()
        if that_key == key:
            return int(that_value)


def get_value(key):
    shard = get_shard_by_key(key)
    # In case the shard chunks are sorted in dfs.chunk_locations, we can skip chunks
    # while first_key of the chunk is less than key. Performance is increased by 2.5 times
    for chunk in shard.chunks:
        first_key, _ = get_first_line_only(chunk).split()
        if first_key <= key:
            previous_chunk = chunk  # skip the chunk
        else:
            return find_value_in_chunk(previous_chunk, key)  # the previous chunk  contains the key
    return find_value_in_chunk(chunk, key)  # key is in the last chunk
    # If not, the code above (from the 'first_key, _ = ...' line) should be replaced with this simple block:
    #     value = find_value_in_file(chunk, key)
    #     if value:
    #         return value


def calculate_sum(keys_filename):
    files_to_chunk_servers_map.clear()  # Because file locations are mutable (I guess)
    sum_of_values = 0
    for key in get_file_content(keys_filename):
        sum_of_values += get_value(key)
        print(1)
    return sum_of_values
