package org.kaggle.fb3.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author root
 * <Mapper-input> [ tag-word - sum(ones) - totalWordsInTag- noOfTicksts - NoOfTicketsContainsTag]
 * <Mapper-output> [ word - 1]
 */
public class DocCountsForWordMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	protected void map(Object kay,Text value,Context context)throws IOException,InterruptedException{
		
		String[] lineWords = value.toString().split("\t");		
		String wrd = lineWords[1].trim();
		word.set(wrd);
		context.write(word, one);
	}
}
