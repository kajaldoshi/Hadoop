package com.psl.Employee;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmployeeReducer extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		int max=0;
		for(Text val : value){
			String[] values = val.toString().split(" ");
			int salary = Integer.parseInt(values[1]);
			if(salary > max)
					max=salary;
			
		}
		context.write(key, new IntWritable(max));
		
		
	}

}
