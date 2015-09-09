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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InLinksTopNReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, Text> {

	// TODO: Create base classes TopN{Mapper,Reducer} to avoid duplicate
	// code in {PageRank,InLinks}TopN{Mapper,Reducer}.

	private PriorityQueue<Map.Entry<Integer, Integer>> topN;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("inlinks.top_results"));

		topN = new PriorityQueue<Map.Entry<Integer, Integer>>(topResults,
				new MapEntryKeyComparator<Integer, Integer>());
	}

	@Override
	protected void reduce(IntWritable inKey,
			Iterable<IntWritable> inValues, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("inlinks.top_results"));

		for (IntWritable inValue : inValues) {
			int page = inValue.get(), pageInLinks = inKey.get();

			if (topN.size() < topResults || pageInLinks >= topN.peek().getKey()) {
				topN.add(new AbstractMap.SimpleEntry<Integer, Integer>(pageInLinks, page));
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
		Path titlesDir = new Path(conf.get("inlinks.titles_dir"));

		MapFile.Reader[] readers = MapFileOutputFormat.getReaders(titlesDir, conf);
		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
		IntWritable page = new IntWritable();
		Text title = new Text();

		int[] inLinks = new int[topN.size()];
		String[] titles = new String[topN.size()];

		for (int i = inLinks.length - 1; i >= 0; i--) {
			Map.Entry<Integer, Integer> entry = topN.poll();
			page.set(entry.getValue());
			MapFileOutputFormat.getEntry(readers, partitioner, page, title);
			inLinks[i] = entry.getKey();
			titles[i] = title.toString();
		}

		for (MapFile.Reader reader : readers) {
			reader.close();
		}

		for (int i = 0; i < inLinks.length; i++) {
			context.write(new IntWritable(inLinks[i]), new Text(titles[i]));
		}
	}
}
