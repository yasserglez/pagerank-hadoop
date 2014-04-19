/*
 * Copyright 2014 Yasser Gonzalez Fernandez
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankIterationReducer
		extends Reducer<ShortWritable, FloatArrayWritable, 
		                ShortWritable, FloatArrayWritable> {

	@Override
	protected void reduce(ShortWritable inKey,
			Iterable<FloatArrayWritable> inValues, Context context)
			throws IOException, InterruptedException {

		// This task sums all the partial results for one stripe of the vector
		// v_k and adds the teleportation factor.

		Configuration conf = context.getConfiguration();
		int numPages = Integer.parseInt(conf.get("pagerank.num_pages"));
		float beta = Float.parseFloat(conf.get("pagerank.damping_factor"));

		FloatWritable[] vi = null;

		for (FloatArrayWritable inValue : inValues) {
			Writable[] partialVi = inValue.get();

			if (vi == null) {
				// vi is initialized here in order to know the correct size of
				// the stripe (the last stripe can be incomplete).
				vi = new FloatWritable[partialVi.length];
				for (int k = 0; k < vi.length; k++) {
					vi[k] = new FloatWritable(0);
				}
			}

			// Sum the partial results.
			for (int k = 0; k < vi.length; k++) {
				vi[k].set(vi[k].get() + ((FloatWritable) partialVi[k]).get());
			}
		}

		// Add the teleportation factor.
		for (int k = 0; k < vi.length; k++) {
			 vi[k].set(beta * vi[k].get() + (1 - beta) / numPages);
		}

		context.write(inKey, new FloatArrayWritable(vi));
	}
}
