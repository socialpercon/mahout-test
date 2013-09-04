package com.mobigen.mahout.wikitest;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.mahout.math.*;

public class WikipediaPrefsDriver {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WikipediaPrefsDriver.class);
		conf.setJobName("WikipediaPrefs");
		
		conf.setMapOutputKeyClass(VarLongWritable.class);
		conf.setMapOutputValueClass(VarLongWritable.class);
		conf.setOutputKeyClass(VarLongWritable.class);
		conf.setOutputValueClass(VectorWritable.class);
		
		conf.setMapperClass(WikipediaToItemPrefsMapper.class);
		conf.setReducerClass(WikipediaToItemPrefsReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
