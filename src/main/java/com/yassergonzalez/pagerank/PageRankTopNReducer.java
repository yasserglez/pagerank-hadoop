/*
 * Copyright 2014 Yasser Gonzalez <contact@yassergonzalez.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yassergonzalez.pagerank;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class PageRankTopNReducer extends
		Reducer<FloatWritable, IntWritable, FloatWritable, Text> {

	// TODO: Create base classes TopN{Mapper,Reducer} to avoid duplicate
	// code in {PageRank,InLinks}TopN{Mapper,Reducer}.

	private PriorityQueue<Map.Entry<Float, Integer>> topN;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("pagerank.top_results"));

		// This queue keeps the top N elements by PageRank.
		topN = new PriorityQueue<Map.Entry<Float, Integer>>(topResults,
				new MapEntryKeyComparator<Float, Integer>());
	}

	@Override
	protected void reduce(FloatWritable inKey,
			Iterable<IntWritable> inValues, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("pagerank.top_results"));

		for (IntWritable inValue : inValues) {
			int page = inValue.get();
			float pageRank = inKey.get();

			// The elements in the queue are sorted (in non-decreasing order) by
			// PageRank. The queue is filled up until it contains topResults
			// elements. Then, a new element will be added only if its PageRank
			// is greater than the lowest PageRank in the queue. If the queue is
			// full and a new element is added, the one with the lowest PageRank
			// is removed from the queue.
			if (topN.size() < topResults || pageRank >= topN.peek().getKey()) {
				topN.add(new AbstractMap.SimpleEntry<Float, Integer>(pageRank, page));
				if (topN.size() > topResults) {
					topN.poll();
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		Path titlesDir = new Path(conf.get("pagerank.titles_dir"));

		MapFile.Reader[] readers = MapFileOutputFormat.getReaders(titlesDir, conf);
		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
		IntWritable page = new IntWritable();
		Text title = new Text();

		float[] pageRanks = new float[topN.size()];
		String[] titles = new String[topN.size()];

		// The order of the entries is reversed. The priority queue is in
		// non-decreasing order and we want the highest PageRank first.
		for (int i = pageRanks.length - 1; i >= 0; i--) {
			Map.Entry<Float, Integer> entry = topN.poll();
			// Get the title of the page from the title index.
			page.set(entry.getValue());
			MapFileOutputFormat.getEntry(readers, partitioner, page, title);
			pageRanks[i] = entry.getKey();
			titles[i] = title.toString();
		}

		for (MapFile.Reader reader : readers) {
			reader.close();
		}

		for (int i = 0; i < pageRanks.length; i++) {
			context.write(new FloatWritable(pageRanks[i]), new Text(titles[i]));
		}
	}
}
