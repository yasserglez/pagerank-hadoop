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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TitleIndex extends Configured implements Tool {

	public static class TitleIndexMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		int lineNum = 0;

		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(++lineNum), inValue);
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: TitleIndex <titles-sorted.txt> <output-dir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}

		Path titlesFile = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Configuration conf = getConf();

		// Do not create _SUCCESS files. MapFileOutputFormat.getReaders calls
		// try to read the _SUCCESS as another MapFile dir.
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		// This job creates a MapFile of the titles indexed by the page id.
		// UnsplittableTextInputFormat is used to ensure that the same map task
		// gets all the lines in the titlesFile and it can count the line
		// numbers. The number of reduce tasks is set to 0.

		Job job = Job.getInstance(conf, "TitleIndex");

		job.setJarByClass(InLinks.class);
		job.setInputFormatClass(UnsplittableTextInputFormat.class);
		job.setMapperClass(TitleIndexMapper.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, titlesFile);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setNumReduceTasks(0);
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new TitleIndex(), args);
		System.exit(result);
	}
}
