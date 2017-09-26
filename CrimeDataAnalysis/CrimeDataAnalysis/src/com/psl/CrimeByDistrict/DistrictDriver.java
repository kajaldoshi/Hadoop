package com.psl.CrimeByDistrict;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistrictDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(DistrictDriver.class);
		job.setMapperClass(DistrictMapper.class);
		job.setReducerClass(DistrictReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
