package com.github.ygf.pagerank;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankTopNMapper extends
		Mapper<ShortWritable, FloatArrayWritable, FloatWritable, IntWritable> {

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
	public void map(ShortWritable inKey, FloatArrayWritable inValue,
			Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		short blockSize = Short.parseShort(conf.get("pagerank.block_size"));
		int topResults = Integer.parseInt(conf.get("pagerank.top_results"));

		Writable[] vStripe = inValue.get();
		for (int i = 0; i < vStripe.length; i++) {
			int page = 1 + (inKey.get() - 1) * blockSize + i;
			float pageRank = ((FloatWritable) vStripe[i]).get();

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

		// The mapper outputs the top N pages by PageRank in the partition.
		for (Map.Entry<Float, Integer> entry : topN) {
			context.write(new FloatWritable(entry.getKey()), 
					new IntWritable(entry.getValue()));
		}
	}
}
