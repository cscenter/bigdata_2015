#!/usr/bin/python
# encoding: utf8

# for local testing
import test_dfs as dfs

# for work with real environment
# import http_dfs as dfs


def get_file_content(filename):
    chunk_ids = [f.chunks for f in dfs.files() if f.name == filename]
    if not chunk_ids:
        raise Exception("file %s were not found" % filename)
    locations = dfs.chunk_locations()
    for chunkId in chunk_ids[0]:
        chunk_server = [l.chunkserver for l in locations if l.id == chunkId][0]
        for line in dfs.get_chunk_data(chunk_server, chunkId):
            if not line.isspace():
                yield line[:-1]


def get_filename(page_key):
    for line in get_file_content("/partitions"):
        lo, hi, filename = line.split(' ')
        if lo <= page_key <= hi:
            return filename
    raise Exception("page %s were not found" % page_key)


def get_visit_count(page_key):
    file = get_filename(page_key)
    for line in get_file_content(file):
        curr_page_key, visit_count = line.split(' ')
        if page_key == curr_page_key:
            return int(visit_count)
    raise Exception("page %s were not found" % page_key)


def calculate_sum(keys_filename):
    """
    :param keys_filename:
    :return: visit count sum by page keys listed in keys_filename
    """
    total = 0
    for line in get_file_content(keys_filename):
        total += get_visit_count(line)
    return total


print(calculate_sum("/keys"))