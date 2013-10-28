package org.kaggle.fb3.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DocCountsForWord extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		Job idfCount = new Job(conf); 
		
		idfCount.setJobName("Count document frequencies for words");
		idfCount.setJarByClass(getClass());
		idfCount.setNumReduceTasks(3);
		
		//setting mapper, combiner, reducer
		idfCount.setMapperClass(DocCountsForWordMapper.class);
		idfCount.setCombinerClass(DocCountsForWordReducer.class);
		idfCount.setReducerClass(DocCountsForWordReducer.class);
		
		idfCount.setMapOutputKeyClass(Text.class);
		idfCount.setMapOutputValueClass(IntWritable.class);
		idfCount.setOutputKeyClass(Text.class);
		idfCount.setOutputValueClass(IntWritable.class);
				
		//idfCount.setInputFormatClass(KeyValueTextInputFormat.class);
		idfCount.setInputFormatClass(TextInputFormat.class);
		idfCount.setOutputValueClass(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(idfCount, inputPath);
		FileOutputFormat.setOutputPath(idfCount, outputPath);
		
		int code = idfCount.waitForCompletion(true)?0:1;
		
		if(code == 0){
			//TODO: fixing the class error , this is due to hadoop version
			//Counters counters = idfCount.getCounters();
			//LongWritable counter = (LongWritable)counters.findCounter("org.apache.hadoop.mapred.Tesk$Counter","MAP_INPUT_RECORDS");
			//System.out.println("TOTAL MAPPER INPUT LINES \t" + counter.get());
		
			return code;
		}
		return 1;
	}
	
	public static void main(String[] args) throws Exception{
		int returnCode = ToolRunner.run(new DocCountsForWord(), args);
		System.exit(returnCode);
	}


}
