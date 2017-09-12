package com.psl.Employee;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class customPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numberOfPartitions) {
		
		String[] tokens = value.toString().split(" ");
		int age = Integer.parseInt(tokens[0]);
		if(age<=20)
			return 0;
		else if(age>20 && age<=40)	
			return 1;
		else
			return 2;
	}

}
