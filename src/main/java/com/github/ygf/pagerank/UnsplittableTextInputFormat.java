package com.github.ygf.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class UnsplittableTextInputFormat extends TextInputFormat  {

	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
		// This prevents Hadoop from splitting the file, i.e., all the lines of
		// the file will be passed to a single map task.
		  return false;
	  }
}
