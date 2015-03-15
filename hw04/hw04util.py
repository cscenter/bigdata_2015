# encoding: utf-8

def read_vertices(vertices, filename, vertexConstructor):
  with open(filename) as f:
    for l in f:
      l = l.strip()
      if len(l) == 0:
        continue
      components = l.split(" ")
      if len(components) < 2:
        raise Exception("ERROR: too few components in the vertex record (expected at least 2): %s" % l)
      docid = int(components[0])
      vertex = vertexConstructor(docid)
      vertices[docid] = vertex
      if len(components) > 2:
        vertex.value = components[2]

def read_edges(vertices, filename):
  with open(filename) as f:
    for l in f:
      l = l.strip()
      if len(l) == 0:
        continue
      components = l.split(" ")
      outlinks = components[1]
      if outlinks != "==":
        docid = int(components[0])
        if not docid in vertices:
          raise Exception("ERROR when creating links from %d to %s: source not found" % (docid, outlinks))
        src = vertices[docid]
        for dst_id in [int(v) for v in outlinks.split(',')]:
          if not dst_id in vertices:
            raise Exception("ERROR when creating a link from %d to %d: destination not found" % (docid, dst_id))
          dst = vertices[dst_id]
          src.out_vertices.append(dst)

def read_graph(filename, vertexConstructor):
  vertices = {}
  read_vertices(vertices, filename, vertexConstructor)
  read_edges(vertices, filename)
  return vertices