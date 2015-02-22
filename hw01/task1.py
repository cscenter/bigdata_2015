#!/usr/bin/python
# encoding: utf8

import http_dfs as dfs


def get_file_content(filename):
    for line in dfs.get_chunk_data(get_chunkserver(filename), filename):
        if line.strip():
            yield line.strip()


def get_chunkserver(filename):
    for c in dfs.chunk_locations():
        if c.id == filename:
            return c.chunkserver
    raise FileNotFoundError('File {} was not found'.format(filename))


def find_shard_containing_key(key):
    for range_shard_pair in get_file_content('partitions'):
        lower_bound, upper_bound, shard = range_shard_pair.split()
        if lower_bound <= key <= upper_bound:
            for f in dfs.files():
                # Slicing has been made due to shard filename incompatibility
                if f.name == shard[1:]:
                    return f
    raise Exception("There's no such key as {0} in the dfs".format(key))


def get_first_line_only(filename):
    generator = get_file_content(filename)
    first_line = next(generator)
    generator.close()
    return first_line


def find_value_in_file(filename, key):
    for key_value_pair in get_file_content(filename):
        that_key, that_value = key_value_pair.split()
        if key_value_pair != '\n' and that_key == key:
            return int(that_value)


def get_value(key):
    shard = find_shard_containing_key(key)
    # In case the shard chunks are sorted in dfs.chunk_locations, we can skip chunks
    #  while first_key of the chunk is less than key. Performance is increased by 2.5 times
    for chunk in shard.chunks:
        first_key, _ = get_first_line_only(chunk).split()
        if first_key <= key:
            previous_chunk = chunk  # skip the chunk
        else:
            return find_value_in_file(previous_chunk, key)  # the previous chunk  contains the key
    return find_value_in_file(chunk, key)  # key is in the last chunk
    #  If not, the code above should be replaced with this simple block:
    #     value = find_value_in_file(chunk, key)
    #     if value:
    #         return value


def calculate_sum(keys_filename):
    sum = 0
    for key in get_file_content(keys_filename):
        sum += get_value(key)
    return sum

# Example:
print(calculate_sum('keys'))