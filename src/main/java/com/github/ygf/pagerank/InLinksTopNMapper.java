package com.github.ygf.pagerank;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class InLinksTopNMapper extends
		Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

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
	public void map(IntWritable inKey, IntWritable inValue,
			Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int topResults = Integer.parseInt(conf.get("inlinks.top_results"));

		int page = inKey.get(), pageInLinks = inValue.get();

		if (topN.size() < topResults || pageInLinks >= topN.peek().getKey()) {
			topN.add(new AbstractMap.SimpleEntry<Integer, Integer>(pageInLinks, page));
			if (topN.size() > topResults) {
				topN.poll();
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		for (Map.Entry<Integer, Integer> entry : topN) {
			context.write(new IntWritable(entry.getKey()), 
					new IntWritable(entry.getValue()));
		}
	}
}