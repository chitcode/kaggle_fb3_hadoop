package org.kaggle.fb3.featureExtract;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFFeatExtractReducer extends Reducer<Text, Text, Text, Text>{	
	
	Text outputValue = new Text();
	
	@Override
	protected void reduce(Text key,Iterable<Text> values, Context context)throws IOException,InterruptedException{
		StringBuffer titles = new StringBuffer();
		for(Text value : values){			
			titles.append(value.toString());			
			titles.append(" ");
		}
		String valueStr = titles.toString();
		valueStr = valueStr.replace("\\s?$", "");
		
		outputValue.set(valueStr);
		context.write(key,outputValue);		
	}
}
