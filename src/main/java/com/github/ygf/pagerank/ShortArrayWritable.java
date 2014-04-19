package com.github.ygf.pagerank;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ShortArrayWritable extends ArrayWritable implements
		WritableComparable<ShortArrayWritable> {

	public ShortArrayWritable() {
		super(ShortWritable.class);
	}

	public ShortArrayWritable(Writable[] values) {
		super(ShortWritable.class, values);
	}

	public int compareTo(ShortArrayWritable that) {
		Writable[] self = this.get();
		Writable[] other = that.get();

		if (self.length != other.length) {
			// Length decides first.
			return Integer.valueOf(self.length).compareTo(Integer.valueOf(other.length));
		} else {
			// Then, compare every pair of elements.
			for (int i = 0; i < self.length; i++) {
				short s = ((ShortWritable) self[i]).get();
				short o = ((ShortWritable) other[i]).get();
				if (s != o) return Integer.valueOf(s).compareTo(Integer.valueOf(o)); 
			}
			// Same length, same elements => same array. 
			return 0;
		}
	}
}
