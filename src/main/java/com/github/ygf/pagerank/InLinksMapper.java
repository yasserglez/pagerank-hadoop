package com.github.ygf.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InLinksMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable inKey, Text inValue, Context context)
			throws IOException, InterruptedException {

		String[] lineParts = inValue.toString().split(":\\s+");
		String[] vOutlinks = lineParts[1].split("\\s+");

		for (int k = 0; k < vOutlinks.length; k++) {
			int w = Integer.parseInt(vOutlinks[k]);
			context.write(new IntWritable(w), one);
		}
	}
}
