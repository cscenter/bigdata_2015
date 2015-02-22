Summary
=======

The main bottleneck here is network. I resolved it by reducing number of http
calls. It can be easily paralellized, but I decided to save readability of the
solution.

As for http_dfs module, it makes:
- 17 get_chunk_data calls
- 1 chunk_locations call
- 1 files call
