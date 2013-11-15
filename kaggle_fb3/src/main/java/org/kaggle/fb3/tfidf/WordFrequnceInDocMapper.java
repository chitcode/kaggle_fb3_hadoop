package org.kaggle.fb3.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Chit
 * <Mapper-input> [tag - tag+body - noOfTicket -  tagContains]
 * <Mapper-output> [ word-tag, 1 - totalWordsInTag- noOfTicksts - NoOfTicketsContainsTag]
 *
 */
public class WordFrequnceInDocMapper extends Mapper<Object,Text,WordTagWritable,WdCntWdsInDocCntWritable>{	
	
	
	WordTagWritable wordTagKey = new WordTagWritable();
	static final VIntWritable one = new VIntWritable(1);
	VIntWritable totalWords = new VIntWritable(0);
	private VIntWritable noOfTicket = new VIntWritable(0);
	private VIntWritable keyContains = new VIntWritable(0);
	
	WdCntWdsInDocCntWritable wdCntWdsInDocCntWritable = new WdCntWdsInDocCntWritable();
	@Override 
	protected void map(Object key, Text value, Context context) throws IOException,InterruptedException{
		
		String[] tagWords = value.toString().split("\t");
		String tag = tagWords[0].trim();
		noOfTicket.set(Integer.valueOf(tagWords[2].trim()));
		keyContains.set(Integer.valueOf(tagWords[3].trim()));
		
		if(tag != null && !"".equals(tag)){
			String[] words = tagWords[1].split("\\s");
			totalWords.set(words.length);
			
			for(String word:words){ // first arg word, 2nd arg is key
				if(word == null | "".equals(word)){
					continue;
				}
				word = word.replaceAll("\"", "").trim();
				wordTagKey.set(word,tag);
				wdCntWdsInDocCntWritable.set(one,totalWords,noOfTicket,keyContains);
				context.write(wordTagKey, wdCntWdsInDocCntWritable);		
			}			
		}
				
	}
}
