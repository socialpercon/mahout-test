package com.mobigen.mahout.wikitest;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.*;
import org.apache.mahout.math.*;

public class WikipediaToItemPrefsReducer extends MapReduceBase implements Reducer<VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable> {
	public void configure(JobConf arg0) {
		// do nothing
		return;
	}

	public void close() throws IOException {
		// do nothing
		return;
	}

	public void reduce(VarLongWritable userID, Iterator<VarLongWritable> itemPrefs, OutputCollector<VarLongWritable, VectorWritable> collector, Reporter reporter) throws IOException {
		Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
		while(itemPrefs.hasNext()) {
			VarLongWritable itemPref = itemPrefs.next();
			userVector.set((int)itemPref.get(), 1.0f);
		}
		collector.collect(userID, new VectorWritable(userVector));
	}
}
