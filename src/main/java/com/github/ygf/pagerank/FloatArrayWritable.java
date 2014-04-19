package com.github.ygf.pagerank;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class FloatArrayWritable extends ArrayWritable {

	public FloatArrayWritable() {
		super(FloatWritable.class);
	}

	public FloatArrayWritable(Writable[] values) {
		super(FloatWritable.class, values);
	}

}
