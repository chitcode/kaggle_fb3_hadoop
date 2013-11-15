package org.kaggle.fb3.featureExtract;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Chit
 * <Reducer-in> [tag - tag+body - noOfTicket = 1 -  tagContains = 0/1]    # content is cleaned and bi-grammed
 * <Reducer-out> [tag - tag+body - noOfTicket -  tagContains] # all the content,noOfTicket,keyContains  combined
 *
 */
public class TFIDFFeatExtractReducer extends Reducer<Text, TFIDFWritable, Text, TFIDFWritable>{	
	
	TFIDFWritable outputValue = new TFIDFWritable();
	
	@Override
	protected void reduce(Text key,Iterable<TFIDFWritable> values, Context context)throws IOException,InterruptedException{
		StringBuffer contentBuff = new StringBuffer();
		
		int noOfTicket = 0;
		int keyContains = 0;
		String content = null;
		for(TFIDFWritable value : values){
			content = value.getContent().toString();
			contentBuff.append(content);		
			contentBuff.append(" ");
			noOfTicket = noOfTicket + value.getNoOfTicket().get();
			keyContains = keyContains + value.getKeyContains().get();			
		}
		String valueStr = contentBuff.toString();
		valueStr = valueStr.replace("\\s?$", "");
		
		outputValue.set(valueStr.trim(),noOfTicket,keyContains);
		context.write(key,outputValue);	
	}
}
