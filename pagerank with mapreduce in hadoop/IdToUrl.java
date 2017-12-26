package com.lendap.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IdToUrl {
	
	public static class Map1 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
		
			String line = value.toString();
			// (M, i, j, Mij);
			String[] indicesAndValue = line.split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();
			outputKey.set(indicesAndValue[1]);
			outputValue.set(indicesAndValue[3]);
			// outputValue.set(M,j,Mij);
			context.write(outputKey, outputValue);
				
		} 
	}


	public static class Map2 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{

			String line = value.toString();
			// (M, i, j, Mij);
			String[] indicesAndValue = line.split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();
			outputKey.set(indicesAndValue[0]);
			outputValue.set(indicesAndValue[1]);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] value;
			ArrayList<Text> dest = new ArrayList<>();
			for (Text val : values) 
			{
				dest.add(val);
			}
			context.write(null,new Text(dest.get(0)+","+dest.get(1)));
	
			
	}
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
	int iterations=5;
        // M is an m-by-n matrix; N is an n-by-p matrix.
        conf.set("m", "4");
        conf.set("n", "4");
        conf.set("p", "1");
        @SuppressWarnings("deprecation")
	Path mat= new Path(args[0]);
	Path vec= new Path(args[1]);

		Job job = new Job(conf, "IdToUrl");
		job.setJarByClass(IdToUrl.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	   	job.setReducerClass(Reduce.class);
	 	job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);



			MultipleInputs.addInputPath(job,mat,TextInputFormat.class,Map1.class);
			MultipleInputs.addInputPath(job,vec,TextInputFormat.class,Map2.class);
			FileOutputFormat.setOutputPath(job,new Path("/output/temp"+Integer.toString(1)));
			//after the first time the output is stored in temp1

		job.waitForCompletion(true);


		
     
      	

 
      
    }


}
