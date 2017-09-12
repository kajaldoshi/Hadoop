package com.psl.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



class SecondaryMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\\t");
		// Ignore entries with empty values for better readability of results
		if(tokens[4].isEmpty() || tokens[5].isEmpty())
						return;
		context.write(new CompositeKey(tokens[5],tokens[4],Float.parseFloat(tokens[11])),new Text(value));
	
	}

}

class SecondaryReducer extends Reducer<CompositeKey, Text, Text, Text>{

	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text value :values){
			String[] tokens = value.toString().split("\\t");
			context.write(new Text(tokens[0]), new Text(tokens[5]+" "+tokens[4]+" "+Float.parseFloat(tokens[11])));
		}
		
	}
}


public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		
		job.setJarByClass(Driver.class);
		
		//mapper
		job.setMapperClass(SecondaryMapper.class);
		job.setMapOutputKeyClass(CompositeKey.class);

		//Set Sort Custom Comparator class
		job.setSortComparatorClass(FullKeyComparator.class);
		job.setPartitionerClass(CustomPartitioner.class);
		
		job.setNumReduceTasks(1);
		//reducer
		job.setReducerClass(SecondaryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

	
	
	
	
}
