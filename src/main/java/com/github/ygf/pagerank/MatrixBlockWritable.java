package com.github.ygf.pagerank;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

public class MatrixBlockWritable extends TwoDArrayWritable {

	public MatrixBlockWritable() {
		super(ShortWritable.class);
	}

	public MatrixBlockWritable(Writable[][] values) {
		super(ShortWritable.class, values);
	}
}
