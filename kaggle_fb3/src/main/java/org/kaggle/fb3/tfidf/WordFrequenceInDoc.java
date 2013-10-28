package org.kaggle.fb3.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordFrequenceInDoc extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		Job wordFrequncyCount = new Job(conf); //TODO: check what is new way of getting the job object 
		
		//wordFrequncyCount.setJobName();
		wordFrequncyCount.setJarByClass(getClass());
		wordFrequncyCount.setNumReduceTasks(10);
		
		//setting mapper, combiner, reducer
		wordFrequncyCount.setMapperClass(WordFrequnceInDocMapper.class);
		wordFrequncyCount.setCombinerClass(WordFrequnceInDocReducer.class);
		wordFrequncyCount.setReducerClass(WordFrequnceInDocReducer.class);
		
		wordFrequncyCount.setMapOutputKeyClass(WordTagWritable.class);
		wordFrequncyCount.setMapOutputValueClass(WdCntWdsInDocCntWritable.class);
		wordFrequncyCount.setOutputKeyClass(WordTagWritable.class);
		wordFrequncyCount.setOutputValueClass(WdCntWdsInDocCntWritable.class);
				
		//wordFrequncyCount.setInputFormatClass(KeyValueTextInputFormat.class);
		wordFrequncyCount.setInputFormatClass(TextInputFormat.class);
		wordFrequncyCount.setOutputValueClass(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(wordFrequncyCount, inputPath);
		FileOutputFormat.setOutputPath(wordFrequncyCount, outputPath);
		
		int code = wordFrequncyCount.waitForCompletion(true)?0:1;
		
		if(code == 0){
			//TODO: fixing the class error , this is due to hadoop version
			//Counters counters = wordFrequncyCount.getCounters();
			//LongWritable counter = (LongWritable)counters.findCounter("org.apache.hadoop.mapred.Tesk$Counter","MAP_INPUT_RECORDS");
			//System.out.println("TOTAL MAPPER INPUT LINES \t" + counter.get());
		
			return code;
		}
		return 1;
	}
	
	public static void main(String[] args) throws Exception{
		int returnCode = ToolRunner.run(new WordFrequenceInDoc(), args);
		System.exit(returnCode);
	}


}
