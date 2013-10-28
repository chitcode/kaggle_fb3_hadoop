package org.kaggle.fb3.featureExtract;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FeatureExtractionMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	String splitString = "\",\"";
	
	private LongWritable outputkey = new LongWritable();
	private Text outputValue = new Text();
	
	@Override
	protected void map(LongWritable key,Text value, Context context)throws IOException,InterruptedException{
		String entry = value.toString();
		
		String[] columns = entry.split(splitString);
		int splitSize = columns.length;
		
		String lineId = columns[0];
		lineId = lineId.replaceAll("^NEW_LINE_CHHAR\"","");
		try{
			long l = Long.valueOf(lineId.trim());
			if(splitSize >= 4){
				
				String title = columns[1];
				
				String tags = columns[3];
				tags = tags.replaceAll("\"?$","");
				
				StringBuffer filteredData = new StringBuffer();				
				
				filteredData.append(title);
				filteredData.append('@');
				filteredData.append(tags);
				
				
				
				outputkey.set(l);
				outputValue.set(filteredData.toString());
				context.write(outputkey, outputValue);
				
			}else{
				//TODO: put some counters to count the number of records having problem
			}
		}catch(Exception e){
			//TODO: handle it properly
		}
	}
}
