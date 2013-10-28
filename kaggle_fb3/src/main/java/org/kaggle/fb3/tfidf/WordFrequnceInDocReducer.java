package org.kaggle.fb3.tfidf;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class WordFrequnceInDocReducer extends Reducer<WordTagWritable, WdCntWdsInDocCntWritable, WordTagWritable, WdCntWdsInDocCntWritable>{
	
	WdCntWdsInDocCntWritable wdCntWdsInDocCntWritable = new WdCntWdsInDocCntWritable();
	
	@Override
	protected void reduce(WordTagWritable key,Iterable<WdCntWdsInDocCntWritable> values, Context context)throws IOException, InterruptedException{
		int count = 0;
		int totalWordsInTag = 0;
		for(WdCntWdsInDocCntWritable c:values){
			count += c.getWordsIntag().get();
			totalWordsInTag = c.getTotalWordsIntag().get();
		}
		wdCntWdsInDocCntWritable.set(count,totalWordsInTag);
		context.write(key, wdCntWdsInDocCntWritable);	
	}

}
