package org.kaggle.fb3.cv;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CVGeneratorMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
	
	//private Text outoutKey = new Text();
	
	private final String training = "training";
	private final String cv = "cv";
	
	@Override
	protected void map(LongWritable key,Text value, Context context)throws IOException,InterruptedException{
		
		if(Math.random() <= 0.01) //TODO: this value should be captured from command line
		//outoutKey.set(cv);		
		context.write(NullWritable.get(), value);
	}
}
