package org.kaggle.fb3.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocCountsForWordReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
	
	private IntWritable sumCount = new IntWritable();
	
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
		int count = 0;
		for(IntWritable c:values){
			count += c.get();
		}
		sumCount.set(count);
		context.write(key, sumCount);		
	}
}
