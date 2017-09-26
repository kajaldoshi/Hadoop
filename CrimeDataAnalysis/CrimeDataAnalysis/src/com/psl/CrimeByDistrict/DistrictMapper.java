package com.psl.CrimeByDistrict;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistrictMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private String district;
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		district = tokens[11];
		context.write(new Text(district), new IntWritable(1));
		
	}
}
