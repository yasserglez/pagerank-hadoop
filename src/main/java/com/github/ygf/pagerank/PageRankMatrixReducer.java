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

package com.github.ygf.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankMatrixReducer extends
		Reducer<ShortArrayWritable, ShortArrayWritable,
		        ShortArrayWritable, MatrixBlockWritable> {

	@Override
	public void reduce(ShortArrayWritable inKey,
			Iterable<ShortArrayWritable> inValues, Context context)
			throws IOException, InterruptedException {

		// This task receives all the entries in M_{i,j} and builds the compact
		// representation of the block. See Section 5.2.4 of Mining of Massive
		// Datasets (http://infolab.stanford.edu/~ullman/mmds.html) for details.
		// Only blocks with at least one nonzero entry are generated.

		Configuration conf = context.getConfiguration();
		short blockSize = Short.parseShort(conf.get("pagerank.block_size"));

		short vIndexInBlock, wIndexInBlock, vDegree;
		List<List<Short>> blockColumns = new ArrayList<List<Short>>(blockSize);
		for (int k = 0; k < blockSize; k++) {
			blockColumns.add(new ArrayList<Short>());
		}

		for (ShortArrayWritable inValue : inValues) {
			Writable[] blockEntry = inValue.get();
			vIndexInBlock = ((ShortWritable) blockEntry[0]).get();
			wIndexInBlock = ((ShortWritable) blockEntry[1]).get();
			vDegree = ((ShortWritable) blockEntry[2]).get();

			if (blockColumns.get(vIndexInBlock).isEmpty()) {
				blockColumns.get(vIndexInBlock).add(vDegree);
			}
			blockColumns.get(vIndexInBlock).add(wIndexInBlock);
		}

		ShortWritable[][] blockColumnWritables = new ShortWritable[blockColumns.size()][];
		for (int k = 0; k < blockColumns.size(); k++) {
			List<Short> column = blockColumns.get(k);
			blockColumnWritables[k] = new ShortWritable[column.size()];
			for (int l = 0; l < column.size(); l++) {
				blockColumnWritables[k][l] = new ShortWritable();
				blockColumnWritables[k][l].set(column.get(l).shortValue());
			}
		}

		context.write(inKey, new MatrixBlockWritable(blockColumnWritables));
	}
}
