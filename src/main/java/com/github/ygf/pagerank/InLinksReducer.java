package com.github.ygf.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class InLinksReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable inKey, Iterable<IntWritable> inValues,
			Context context) throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable inValue : inValues) {
			sum += inValue.get();
		}

		context.write(inKey, new IntWritable(sum));
	}
}
