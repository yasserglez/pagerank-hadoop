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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

	private int getNumPages(Configuration conf, Path titlesDir)
			throws Exception {

		int numPages = 0;

		IntWritable pageNumber = new IntWritable();
		MapFile.Reader[] readers = MapFileOutputFormat.getReaders(titlesDir, conf);
		for (int i = 0; i < readers.length; i++) {
			readers[i].finalKey(pageNumber);
			if (pageNumber.get() > numPages) {
				numPages = pageNumber.get();
			}
		}
		for (MapFile.Reader reader : readers) {
			reader.close();
		}

		return numPages;
	}

	private void createTransitionMatrix(Configuration conf, Path linksFile,
			Path outputDir) throws Exception {

		// This job reads the links-simple-sorted.txt input file and generates
		// the corresponding transition matrix. The matrix is divided into
		// square blocks and each block is represented by the nonzero entries.
		// See Section 5.2 (and 5.2.3 in particular) of Mining of Massive Datasets
		// (http://infolab.stanford.edu/~ullman/mmds.html) for details.
		// The output is written to the "M" subdir in the output dir.

		Job job = Job.getInstance(conf, "PageRank:Matrix");

		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PageRankMatrixMapper.class);
		job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
		job.getConfiguration().setClass("mapreduce.map.output.compress.codec",
				DefaultCodec.class, CompressionCodec.class);
		job.setMapOutputKeyClass(ShortArrayWritable.class);
		job.setMapOutputValueClass(ShortArrayWritable.class);
		job.setReducerClass(PageRankMatrixReducer.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ShortArrayWritable.class);
		job.setOutputValueClass(MatrixBlockWritable.class);
		FileInputFormat.addInputPath(job, linksFile);
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "M"));

		job.waitForCompletion(true);
	}

	private void pageRankIteration(int iter, Configuration conf, Path outputDir)
			throws Exception {

		// This job performs an iteration of the power iteration method to
		// compute PageRank. The map task processes each block M_{i,j}, loads
		// the corresponding stripe j of the vector v_{k-1} and produces the
		// partial result of the stripe i of the vector v_k. The reduce task
		// sums all the partial results of v_k and adds the teleportation factor
		// (the combiner only sums all the partial results). See Section 5.2
		// (and 5.2.3 in particular) of Mining of Massive Datasets
		// (http://infolab.stanford.edu/~ullman/mmds.html) for details. The
		// output is written in a "vk" subdir of the output dir, where k is the
		// iteration number. MapFileOutputFormat is used to keep an array of the
		// stripes of v.

		Job job = Job.getInstance(conf, "PageRank:Iteration");

		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(PageRankIterationMapper.class);
		job.setMapOutputKeyClass(ShortWritable.class);
		job.setMapOutputValueClass(FloatArrayWritable.class);
		job.setCombinerClass(PageRankIterationCombiner.class);
		job.setReducerClass(PageRankIterationReducer.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		job.setOutputKeyClass(ShortWritable.class);
		job.setOutputValueClass(FloatArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(outputDir, "M"));
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "v" + iter));

		job.waitForCompletion(true);
	}

	private void cleanPreviousIteration(int iter, Configuration conf, Path outputDir)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path prevIterDir = new Path(outputDir, "v" + (iter - 1));
		fs.delete(prevIterDir, true);
		fs.close();
	}

	private void summarizeResults(int iter, Configuration conf, Path outputDir)
			throws Exception {

		// This job creates a plain text file with the top N PageRanks and the
		// titles of the pages. Each map task emits the top N PageRanks it
		// receives, and the reduce task merges the partial results into the
		// global top N PageRanks. A single reducer is used in the job in order
		// to have access to all the individual top N PageRanks from the
		// mappers. The reducer looks up the titles in the index built by
		// TitleIndex. This job was designed considering that N is small.

		int topResults = Integer.parseInt(conf.get("pagerank.top_results"));

		Job job = Job.getInstance(conf, "PageRank:TopN");

		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(PageRankTopNMapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(PageRankTopNReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(outputDir, "v" + iter));
		FileOutputFormat.setOutputPath(job, new Path(outputDir, "v" + iter + "-top" + topResults));

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: PageRank <links-simple-sorted.txt> <titles-dir> <output-dir>");
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
		conf.setInt("pagerank.block_size", conf.getInt("pagerank.block_size", 10000));
		conf.setInt("pagerank.max_iterations", conf.getInt("pagerank.max_iterations", 2));
		conf.setFloat("pagerank.damping_factor", conf.getFloat("pagerank.damping_factor", 0.85f));
		conf.setInt("pagerank.top_results", conf.getInt("pagerank.top_results", 100));

		conf.set("pagerank.titles_dir", titlesDir.toString());
		int numPages = getNumPages(conf, titlesDir);
		conf.setLong("pagerank.num_pages", numPages);

		createTransitionMatrix(conf, linksFile, outputDir);

		int maxIters = Integer.parseInt(conf.get("pagerank.max_iterations"));
		for (int iter = 1; iter <= maxIters; iter++) {
			conf.setInt("pagerank.iteration", iter);
			pageRankIteration(iter, conf, outputDir);
			cleanPreviousIteration(iter, conf, outputDir);
		}

		summarizeResults(maxIters, conf, outputDir);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(result);
	}
}
