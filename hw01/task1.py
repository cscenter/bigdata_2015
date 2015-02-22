#!/usr/bin/python
# encoding: utf8

import os.path
import re
import http_dfs as dfs
# import test_dfs as dfs

files = dfs.files()
chunk_locations = dfs.chunk_locations()

def get_file_chunks_ids(filename):
    for record in files:
        if record.name == filename:
            return record.chunks


def get_chunks_info(filename):
    file_chunks_ids = get_file_chunks_ids(filename)
    info = []
    for chunk_id in file_chunks_ids:
        for location in chunk_locations:
            if location.id == chunk_id:
                info.append(location)
    return info
        

def get_file_content(filename):
    for chunk_info in get_chunks_info(filename):
        chunk_data = dfs.get_chunk_data(chunk_info.chunkserver, chunk_info.id)
        for line in chunk_data:
            yield line


def get_filenames_for_key(key):
    filenames = []
    for line in get_file_content('/partitions'):
        try:
            (min_key, max_key, filename) = line.split()
            if min_key <= key and max_key >= key:
                filenames.append(filename)
        except:
            pass
    return filenames


def calculate_sum(keys_filename):
    res = 0
    for key in get_file_content('/keys'):
        key = key[:-1]
        print('Counting for key %s' % (key))
        for filename in get_filenames_for_key(key):
            print('Looking in file %s' % (filename))
            for line in get_file_content(filename):
                pair = line.split()
                if len(pair) == 2 and pair[0] == key:
                    res += int(pair[1])
    return res



def init():
    print('Total sum is: %s' % (calculate_sum('data/keys')))

init()
