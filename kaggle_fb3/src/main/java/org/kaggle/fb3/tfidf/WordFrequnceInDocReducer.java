package org.kaggle.fb3.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * @author Chit
 * <Mapper-Input> [ tag-word , 1 - totalWordsInTag- noOfTicksts - NoOfTicketsContainsTag]
 * <Reducer-Input> [ tag-word , sum(ones) - totalWordsInTag- noOfTicksts - NoOfTicketsContainsTag] # wordCount accumulated
 *
 */
public class WordFrequnceInDocReducer extends Reducer<WordTagWritable, WdCntWdsInDocCntWritable, WordTagWritable, WdCntWdsInDocCntWritable>{
	
	WdCntWdsInDocCntWritable wdCntWdsInDocCntWritable = new WdCntWdsInDocCntWritable();
	
	@Override
	protected void reduce(WordTagWritable key,Iterable<WdCntWdsInDocCntWritable> values, Context context)throws IOException, InterruptedException{
		int count = 0;
		int totalWordsInTag = 0;
		int noOfTicket = 0;
		int keyContains = 0;	
		
		for(WdCntWdsInDocCntWritable c:values){
			count += c.getWordsIntag().get();
			totalWordsInTag = c.getTotalWordsIntag().get();
			noOfTicket = c.getNoOfTicket().get();
			keyContains = c.getKeyContains().get();
		}
		wdCntWdsInDocCntWritable.set(count,totalWordsInTag,noOfTicket,keyContains);
		context.write(key, wdCntWdsInDocCntWritable);	
	}
}
