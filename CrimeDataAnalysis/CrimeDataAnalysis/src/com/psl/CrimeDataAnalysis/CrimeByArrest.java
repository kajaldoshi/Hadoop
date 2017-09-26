package com.psl.CrimeDataAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeByArrest {
	
	public static class ArrestMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private String arrest;
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split(",");
			arrest = tokens[8];
			if(arrest.equalsIgnoreCase("true")||arrest.equalsIgnoreCase("false"))
				context.write(new Text(arrest), new IntWritable(1));
			
		}
	}
	
	public static class ArrestReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		protected void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
			int sum =0;
			for(IntWritable val : value){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}
	
	public static class CustomPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int noOfPartitions) {
			
			if(key.toString().equalsIgnoreCase("false")){
				return 1;
			}
			else 
				return 0;
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(CrimeByArrest.class);
		
		job.setMapperClass(ArrestMapper.class);
		job.setReducerClass(ArrestReducer.class);
		
		job.setPartitionerClass(CustomPartitioner.class);
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
