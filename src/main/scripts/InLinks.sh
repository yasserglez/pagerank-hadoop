#!/bin/bash

hadoop jar page-rank.jar com.github.ygf.pagerank.InLinks \
    -D inlinks.top_results=100 \
    "$@"

