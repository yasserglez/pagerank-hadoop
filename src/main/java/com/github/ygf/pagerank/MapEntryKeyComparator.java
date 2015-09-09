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

import java.util.Comparator;
import java.lang.Comparable;
import java.util.Map;
import java.util.Map.Entry;

public class MapEntryKeyComparator<K extends Comparable<K>, V>
		implements Comparator<Map.Entry<K, V>> {

	public int compare(Entry<K, V> o1, Entry<K, V> o2) {
		return o1.getKey().compareTo(o2.getKey());
	}
}
