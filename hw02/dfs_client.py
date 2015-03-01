import test_dfs as dfs


def get_file_content(filename):
    chunks = []
    for f in dfs.files():
        if f.name == filename:
            chunks = f.chunks
    if len(chunks) == 0:
        return
    clocs = {}
    for c in dfs.chunk_locations():
        clocs[c.id] = c.chunkserver

    for chunk in chunks:
        try:
            loc = clocs[chunk]
            if loc == "":
                raise "ERROR: location of chunk %s is unknown" % chunk
            for l in dfs.get_chunk_data(loc, chunk):
                yield l[:-1]
        except StopIteration:
            pass
