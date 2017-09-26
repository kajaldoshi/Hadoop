package com.psl.CrimeByLocation_Type;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeByLocationType {

	public static class CrimeMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable>{
		
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
		String[] tokens= value.toString().split(",");
		CompositeKey c = new CompositeKey(tokens[7],tokens[5]);
		context.write(c, new IntWritable(1));
				
		}
	}
	public static class CrimeReducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable>{
		
		protected void reduce(CompositeKey key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val : values){
				sum+=val.get();
			}
			context.write(new Text(key.locationDescription + " & "+key.primaryType), new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(CrimeByLocationType.class);
		job.setMapperClass(CrimeMapper.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setSortComparatorClass(FullKeyComparator.class);
		job.setReducerClass(CrimeReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
