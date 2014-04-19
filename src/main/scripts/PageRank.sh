#!/bin/bash

hadoop jar page-rank.jar com.github.ygf.pagerank.PageRank \
    -D pagerank.block_size=10000 \
    -D pagerank.damping_factor=0.85 \
    -D pagerank.max_iterations=2 \
    -D pagerank.top_results=100 \
    -D yarn.am.liveness-monitor.expiry-interval-ms=3600000 \
    -D yarn.nm.liveness-monitor.expiry-interval-ms=3600000 \
    -D yarn.resourcemanager.container.liveness-monitor.interval-ms=3600000 \
    -D mapreduce.jobtracker.expire.trackers.interval=3600000 \
    -D mapreduce.task.timeout=3600000 \
    "$@"

