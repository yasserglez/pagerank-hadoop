/*
 * Copyright 2014 Yasser Gonzalez Fernandez <ygonzalezfernandez@gmail.com>.
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
