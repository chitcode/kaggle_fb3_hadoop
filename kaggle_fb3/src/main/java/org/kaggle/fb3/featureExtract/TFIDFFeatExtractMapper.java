package org.kaggle.fb3.featureExtract;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
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
 * <map-out> [tag - title+body,noOfTicket = 1, keyContains = 0/1 ]    # title is cleaned and bi-grammed
 *
 */
public class TFIDFFeatExtractMapper extends Mapper<LongWritable, Text, Text, TFIDFWritable>{
	
	String splitString = "\",\"";
	
	private Text outputkey = new Text();
	private TFIDFWritable outputValue = new TFIDFWritable();
	final static VIntWritable zero = new VIntWritable(0);
	final static VIntWritable one = new VIntWritable(1);
	private Text contentValue = new Text();
	
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
				
				String body = null;
				if(columns[2].length() >= 150)
					body = columns[2].substring(0, 150);
				else
					body = columns[2];
				
				title = CleanData.clean(title);	
				body = CleanData.clean(body);
				
				
				StringBuffer contentStringBuff = new StringBuffer(title);
				contentStringBuff.append(" ");
				contentStringBuff = contentStringBuff.append(body);
				contentStringBuff.append(" ");
				contentStringBuff.append(Ngram.getBiGram(title));
				contentStringBuff.append(" ");
				contentStringBuff.append(Ngram.getBiGram(body));
				
				String tags = columns[splitSize-1]; //ideally it should be 3 but few data are not clean so getting it from the last of the string
				tags = tags.replaceAll("\"?$","");
				
				
				
				for(String tag:tags.split("\\s")){
					outputkey.set(tag.trim());
					contentValue.set(contentStringBuff.toString().trim());
					if(contentStringBuff.toString().contains(tag))
						outputValue.set(contentValue,one,one);
					else
						outputValue.set(contentValue,one,zero);
					//System.out.println(outputValue.toString());
					context.write(outputkey, outputValue); //TODO: instead of passing the entire string, only word frequencies could be passed
				}			
				
			}else{
				//TODO: put some counters to count the number of records having problem
			}
		}catch(Exception e){
			//System.out.println(e);
		}
	}	
}
