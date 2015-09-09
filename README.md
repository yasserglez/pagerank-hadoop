## PageRank implementation in Hadoop

### Introduction

These Hadoop programs were written to analyze the Wikipedia page-to-page
link database available at http://haselgrove.id.au/wikipedia.htm.

The dataset contains the links between the pages and their titles in
two separate plain-text files `links-simple-sorted.txt` and
`titles-sorted.txt`. The file `links-simple-sorted.txt` has the
following format:

```
from1: to11 to12 to13 ...
from2: to21 to22 to23 ...
...
```

where `from1` is an integer labelling a page that has links from it,
and `to11 to12 to13 ...` are integers labelling all the pages that the
page links to. The file `titles-sorted.txt` contains all the page
titles line-by-line ordered by the integer labels.

The project contains 3 Hadoop jobs that can be executed using the
scripts `TitleIndex.sh`, `InLinks.sh` and `PageRank.sh`. Each part is
described in more detail in the following sections.

### Title Index

A job consisting only of a map task is used to build an index of the
page titles. This job needs to be run only once, before running the
jobs to compute PageRank or the number of in-links.

The job receives the lines of `titles-sorted.txt` and produces a
`MapFile` as its output. The `MapFile` serves as a database that can
be queried to obtain the title of a page given its number (using the
`getEntry` method of `MapFileOutputFormat`). A custom input format
`UnsplittableTextInputFormat` prevents Hadoop from splitting the input
file, so that lines in `titles-sorted.txt` are read in sequential
order to keep track of the page numbers.

### In-Links

The algorithm to compute the number of in-links is very similar to the
classic word-count Hadoop example. The map task receives a line `v: w1
w2 ... wn` from the input file `links-simple-sorted.txt` and emits one
`(w,1)` key-value pair for every out-link `w` in `w1 w2 ... wn`. The
reduce task sums all the entries corresponding to every key `w`
obtaining the number of in-links for the page `w`. The same function
is set as the combiner.

An additional job collects the titles of the top `N` pages. It
receives the number of in-links of each page with the page numbers as
their keys. The map task keeps a bounded priority queue with `N`
entries sorted by the number of in-links. Entries are added to the
queue when they are processed by the map function. The queue is
initialized in the `setup` method of the mapper and the `N` results
are emitted in the `cleanup` method. The reducer receives the top `N`
pages collected by each map task and repeats the same procedure using
the priority queue to obtain the final top `N` results. The page
titles are obtained from the title index before emitting the final
results. The number of reduce tasks in this job is set to 1.

### PageRank

The PageRank computation algorithm follows the ideas described in Section 5.2 of
[Mining Massive Datasets](http://infolab.stanford.edu/~ullman/mmds.html).
It consists of three map-reduce jobs: one pre-processing job, one job
for the power iteration method and a final job to compute the top-N
results.

The pre-processing job reads `links-simple-sorted.txt` and generates
the block representation of the transition matrix `M`. The map task
receives a line from the file and produces pairs with the keys `(i,j)`
corresponding to the indexes of the block of `M` in which each link
should be stored. The reduce task receives all the entries associated
with each key `(i,j)` and stores the block in a jagged array. The
indexes `(i,j)` of the blocks are encoded as an array of
`ShortWritable` and the block itself as a bidimensional array of
`ShortWritable`. The intermediate results of the map tasks and the
final results of the reduce tasks are compressed using Hadoop's
default zlib method.

The second map-reduce job implements the power iteration method. The
map task receives each block `(i,j)`, loads the corresponding stripe
`j` of the vector of PageRank scores `v` from the previous iteration,
and produces a partial result of the stripe `i` of the vector `v'`
that will become the result of the current iteration. The matrix
multiplication of the block and the vector stripe is computed
in-memory and the result is identified with the key `i`. The reduce
task first sums all the partial results for each stripe `i` of `v'`
and then adds the damping/teleportation factors (a combiner that only
performs the sum is also used). The stripes of `v'` are stored in a
`MapFile` with the index of the stripe as the key. The `getEntry`
method of `MapFileOutputFormat` is used within the map task to load
the stripe `j` of `v`. After an iteration is completed, the output
directory of the previous iteration is deleted.

The job to compute the top-N results by PageRank is the same as in the
computation of the top-N pages by number of in-links.

### Compiling and Running

The project can be built using the Maven command `mvn package`.
Then, you can use the scripts `TitleIndex.sh`, `InLinks.sh` and
`PageRank.sh` in `target/pagerank-1.0-bin.zip` to set the options
and run each job.

### License

Copyright 2014 [Yasser Gonzalez](mailto:contact@yassergonzalez.com).

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.
