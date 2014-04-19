#!/bin/bash

hadoop jar page-rank.jar com.github.ygf.pagerank.PageRank \
    -D pagerank.block_size=10000 \
    -D pagerank.damping_factor=0.85 \
    -D pagerank.max_iterations=2 \
    -D pagerank.top_results=100 \
    "$@"

