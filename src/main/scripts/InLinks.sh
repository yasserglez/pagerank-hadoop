#!/bin/bash

hadoop jar page-rank.jar com.yassergonzalez.pagerank.InLinks \
    -D inlinks.top_results=100 \
    "$@"

