Summary
=======

The main bottleneck here is network. I resolved it by reducing the number of http calls.

As for http_dfs module, it makes:
- 17 get_chunk_data calls
- 1 chunk_locations call
- 1 files call

P.S. By the way, it can be easily paralellized, but I decided to save readability of the solution.
