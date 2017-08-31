package com.psl.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private String word;
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		 StringTokenizer itr = new StringTokenizer(value.toString());
		 while(itr.hasMoreElements()){
			 word = itr.nextToken();
			 context.write(new Text(word), one);
		 }
	}

}
