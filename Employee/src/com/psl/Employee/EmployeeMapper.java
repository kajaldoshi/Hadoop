package com.psl.Employee;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String word;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		word = tokens[2]+" "+tokens[4];
		context.write(new Text(tokens[3]), new Text(word));
	}

}
