package com.github.ygf.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InLinks extends Configured implements Tool {

	private void computeInLinks(Configuration conf, Path linksFile, Path outputDir)
			throws Exception {

		// This job computes the number of in-links for every page. The
		// implementation is very similar to the classic word count example.

		Job job = Job.getInstance(conf, "InLinks:Computation");

		job.setJarByClass(InLinks.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(InLinksMapper.class);
		job.setCombinerClass(InLinksReducer.class);
		job.setReducerClass(InLinksReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, linksFile);
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "inlinks"));

		job.waitForCompletion(true);
	}

	private void summarizeResults(Configuration conf, Path outputDir)
			throws Exception {

		int topResults = Integer.parseInt(conf.get("inlinks.top_results"));

		Job job = Job.getInstance(conf, "InLinks:TopN");

		job.setJarByClass(InLinks.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(InLinksTopNMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(InLinksTopNReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(outputDir, "inlinks"));
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "inlinks-top" + topResults));

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: InLinks <links-simple-sorted.txt> <titles-dir> <output-dir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}

		Path linksFile = new Path(args[0]);
		Path titlesDir = new Path(args[1]);
		Path outputDir = new Path(args[2]);

		Configuration conf = getConf();

		// Do not create _SUCCESS files. MapFileOutputFormat.getReaders calls
		// try to read the _SUCCESS as another MapFile dir.
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		// Default values of the parameters of the algorithm.
		conf.setInt("inlinks.top_results", conf.getInt("inlinks.top_results", 100));

		conf.set("inlinks.titles_dir", titlesDir.toString());

		computeInLinks(conf, linksFile, outputDir);
		summarizeResults(conf, outputDir);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new InLinks(), args);
		System.exit(result);
	}
}
