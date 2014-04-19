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
