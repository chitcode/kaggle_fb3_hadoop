package org.kaggle.fb3.featureExtract;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kaggle.fb3.util.CleanData;
import org.kaggle.fb3.util.Ngram;


/***
 * 
 * @author Chit
 * 
 * Extracts features from the text file
 * 
 * <input> [id,title,body,tags]
 * <map-out> [tag - title]    # title is cleaned and bi-grammed
 *
 */
public class TFIDFFeatExtractMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	String splitString = "\",\"";
	
	private Text outputkey = new Text();
	private Text outputValue = new Text();
	
	@Override
	protected void map(LongWritable key,Text value, Context context)throws IOException,InterruptedException{
		String entry = value.toString();
		
		String[] columns = entry.split(splitString);
		int splitSize = columns.length;
		
		String lineId = columns[0];
		lineId = lineId.replaceAll("^NEW_LINE_CHHAR\"","");
		try{
			//long l = Long.valueOf(lineId.trim());
			
			if(splitSize >= 4){
				
				String title = columns[1];				
				
				title = CleanData.clean(title);
				
				StringBuffer titleStrBuff = new StringBuffer(title);
				titleStrBuff.append(" ");
				titleStrBuff.append(Ngram.getBiGram(title));
				
				String tags = columns[splitSize-1]; //should be 3 ideally but some of the data not good
				tags = tags.replaceAll("\"?$","");
				
				
				
				for(String tag:tags.split("\\s")){
					outputkey.set(tag.trim());
					outputValue.set(titleStrBuff.toString());
					context.write(outputkey, outputValue); //TODO: instead of passing the entire string, only word frequencies could be passed
				}			
				
			}else{
				//TODO: put some counters to count the number of records having problem
			}
		}catch(Exception e){
			//TODO: handle it properly
		}
	}	
}
