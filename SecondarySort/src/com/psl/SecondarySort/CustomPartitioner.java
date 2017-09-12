package com.psl.SecondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CompositeKey, Text> {

	@Override
	public int getPartition(CompositeKey key, Text arg1, int numberOfPartition) {
		
		return Math.abs(key.state.hashCode() & Integer.MAX_VALUE) % numberOfPartition;
		
	}

}
