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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMatrixMapper extends
		Mapper<LongWritable, Text, ShortArrayWritable, ShortArrayWritable> {

	@Override
	public void map(LongWritable inKey, Text inValue, Context context)
			throws IOException, InterruptedException {

		// This task gets a line from links-simple-sorted.txt that contains the
		// out links of a page v. It produces results with keys (i, j)
		// corresponding to the indexes of the block M_{i,j} in which each
		// link v -> w should be stored. The value is (v, w, degree(v)).

		Configuration conf = context.getConfiguration();
		short blockSize = Short.parseShort(conf.get("pagerank.block_size"));

		String[] lineParts = inValue.toString().split(":\\s+");
		String[] vOutlinks = lineParts[1].split("\\s+");

		ShortWritable[] blockIndexes = new ShortWritable[2];
		blockIndexes[0] = new ShortWritable();
		blockIndexes[1] = new ShortWritable();

		ShortWritable[] blockEntry = new ShortWritable[3];
		blockEntry[0] = new ShortWritable();
		blockEntry[1] = new ShortWritable();
		blockEntry[2] = new ShortWritable();

		int v, w;
		short i, j;

		v = Integer.parseInt(lineParts[0]);
		j = (short) ((v - 1) / blockSize + 1);

		for (int k = 0; k < vOutlinks.length; k++) {
			w = Integer.parseInt(vOutlinks[k]);
			i = (short) ((w - 1) / blockSize + 1);

			// Indexes of the block M_{i,j}.
			blockIndexes[0].set(i);
			blockIndexes[1].set(j);
			// One entry of the block M_{i,j} corresponding to the v -> w link.
			// The sparse block representation also needs information about
			// the degree of the vector v.
			blockEntry[0].set((short) ((v - 1) % blockSize));
			blockEntry[1].set((short) ((w - 1) % blockSize));
			blockEntry[2].set((short) vOutlinks.length);

			context.write(new ShortArrayWritable(blockIndexes),
					new ShortArrayWritable(blockEntry));
		}
	}
}
