package org.kaggle.fb3.utilMR;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqeTagsMapper extends Mapper<Object, Text, Text, NullWritable>{

	String splitString = "\",\"";
	Text outputKey = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String entry = value.toString();
		
		String[] columns = entry.split(splitString);
		int splitSize = columns.length;
		
		String tags = columns[splitSize-1]; //ideally it should be 3 but few data are not clean so getting it from the last of the string
		tags = tags.replaceAll("\"?$","");
		
		
		
		for(String tag:tags.split("\\s")){
			outputKey.set(tag.trim());			
			context.write(outputKey, NullWritable.get());
		}		
	}
}
