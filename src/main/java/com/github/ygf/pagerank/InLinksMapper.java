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
