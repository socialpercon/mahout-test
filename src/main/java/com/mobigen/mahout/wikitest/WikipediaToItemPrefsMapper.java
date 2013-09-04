package com.mobigen.mahout.wikitest;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.mahout.math.VarLongWritable;

public class WikipediaToItemPrefsMapper extends MapReduceBase implements Mapper<LongWritable, Text, VarLongWritable, VarLongWritable> {
	private static final Pattern NUMBERS = Pattern.compile("(\\d+)");

	public void configure(JobConf arg0) {
		// do nothing for now
		return;
	}

	public void close() throws IOException {
		// do nothing for now
		return;
	}

	public void map(LongWritable key, Text value, OutputCollector<VarLongWritable, VarLongWritable> collector, Reporter reporter) throws IOException {
		String line = value.toString();
		Matcher m = NUMBERS.matcher(line);
		m.find();
		VarLongWritable userID = new VarLongWritable(Long.parseLong(m.group()));
		VarLongWritable itemID = new VarLongWritable();
		
		while(m.find()) {
			itemID.set(Long.parseLong(m.group()));
			collector.collect(userID, itemID);
		}
	}
}
