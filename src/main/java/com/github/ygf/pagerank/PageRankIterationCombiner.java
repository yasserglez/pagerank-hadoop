package com.github.ygf.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankIterationCombiner
		extends Reducer<ShortWritable, FloatArrayWritable, 
		                ShortWritable, FloatArrayWritable> {

	@Override
	protected void reduce(ShortWritable inKey,
			Iterable<FloatArrayWritable> inValues, Context context)
			throws IOException, InterruptedException {

		// This task sums all the partial results for one stripe of the vector
		// v_k. It is a separate class since PageRankIterationReducer also adds
		// the teleportation factor.

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

		context.write(inKey, new FloatArrayWritable(vi));
	}
}
