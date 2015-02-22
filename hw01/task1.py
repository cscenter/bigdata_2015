#!/usr/bin/python
# encoding: utf8

# for local testing
# import test_dfs as dfs

# for work with real environment
import http_dfs as dfs

from collections import defaultdict


def get_chunk_keys_intervals(filename):
    start_to_end = defaultdict()
    for line in partitions_cache:
        lo, hi, curr_filename = line.split(' ')
        if filename.strip('/') == curr_filename.strip('/'):
            start_to_end[lo] = hi

    return [{'start': key, 'end': start_to_end[key]} for key in sorted(start_to_end)]


def get_file_chunks(filename):
    """
    :param filename:
    :return: chunks with metadata (location, id, start key, end key) for
    specified file
    """
    chunk_ids = [f.chunks for f in files_cache if f.name == filename]
    if not chunk_ids:
        raise Exception("file %s were not found" % filename)

    chunk_keys_intervals = get_chunk_keys_intervals(filename)
    ret = list()
    for chunk_id, chunk_key_interval in zip(sorted(chunk_ids[0]), chunk_keys_intervals):
        chunk_server = [l.chunkserver for l in locations_cache if l.id == chunk_id][0]
        ret.append({
            'location': chunk_server,
            'id': chunk_id,
            'start': chunk_key_interval['start'],
            'end': chunk_key_interval['end']})

    return ret


def get_chunk_content(chunk_location, chunk_id):
    for line in dfs.get_chunk_data(chunk_location, chunk_id):
        if not line.isspace():
            yield line[:-1]


def get_file_content(filename):
    chunk_ids = [f.chunks for f in files_cache if f.name == filename]
    if not chunk_ids:
        raise Exception("file %s were not found" % filename)
    for chunk_id in chunk_ids[0]:
        chunk_server = [l.chunkserver for l in locations_cache if l.id == chunk_id][0]
        for line in get_chunk_content(chunk_server, chunk_id):
            yield line


def get_filename(page_key):
    for line in partitions_cache:
        lo, hi, filename = line.split(' ')
        if lo <= page_key <= hi:
            return filename
    raise Exception("page %s were not found" % page_key)

def find_chunk(page_key, all_chunks):
    for chunk in all_chunks:
        if chunk['start'] <= page_key <= chunk['end']:
            return chunk
    raise Exception("chunk for %s not found" % page_key)


def get_visit_count(file, page_keys):
    file_visits = 0
    all_chunks = get_file_chunks(file)
    while len(page_keys) > 0:
        chunk = find_chunk(list(page_keys)[0], all_chunks)
        for line in get_chunk_content(chunk['location'], chunk['id']):
            curr_page_key, visit_count = line.split(' ')
            if curr_page_key in page_keys:
                page_keys.remove(curr_page_key)
                file_visits += int(visit_count)

    return file_visits


def calculate_sum(keys_filename):
    task_file_to_keys = defaultdict(list)
    for page_key in get_file_content(keys_filename):
        file = get_filename(page_key)
        task_file_to_keys[file].append(page_key)

    total = 0
    # todo: parallelize
    for file in task_file_to_keys.keys():
        total += get_visit_count(file, set(task_file_to_keys[file]))

    return total


def demo():
    print(calculate_sum("/keys"))


files_cache = list(dfs.files())
locations_cache = list(dfs.chunk_locations())
partitions_cache = list(get_file_content("/partitions"))
demo()