Summary
=======

The main bottleneck here is network. I resolved it by reducing the number of http calls.

As for http_dfs module, it makes:
- 17 get_chunk_data calls
- 1 chunk_locations call
- 1 files call

P.S. By the way, it can be easily parallelized, but I decided to save readability of the solution.

P.P.S. Python version is 3.4 ([Dockerfile: python:3.4] (https://registry.hub.docker.com/_/python/))
