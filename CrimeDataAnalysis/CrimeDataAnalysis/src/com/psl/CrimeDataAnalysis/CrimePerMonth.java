package com.psl.CrimeDataAnalysis;

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


public class CrimePerMonth {
	
	
	
	public static class MonthMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
			
		//8/29/2016  3:31:00 PM
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] str = {"January","February","March", "April","May","June","July","August","September",    
					   "October","November","December"};
			String[] tokens = value.toString().split(",");
			String datetime = tokens[2];
			
			String[] values =  datetime.split(" ");
			String date = values[0];
			String[] dateSplit = date.split("/");
			String month = dateSplit[0];
			String monthInName = str[Integer.parseInt(month)-1];
			
			context.write(new Text(monthInName), new IntWritable(1));	
		}
		
	}

	public static class MonthReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			int sum =0;
			for(IntWritable val : value){
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(CrimePerMonth.class);
		job.setMapperClass(MonthMapper.class);
		job.setReducerClass(MonthReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
