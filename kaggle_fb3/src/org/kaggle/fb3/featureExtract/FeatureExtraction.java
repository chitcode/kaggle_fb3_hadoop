package org.kaggle.fb3.featureExtract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FeatureExtraction extends Configured implements Tool{
	
	

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		Job textFeatureExtraction = new Job(conf);
		
		textFeatureExtraction.setJobName("Extraction of features for DFIDF");
		textFeatureExtraction.setJarByClass(getClass());
		textFeatureExtraction.setNumReduceTasks(6);
		
		//setting mapper, combiner, reducer
		textFeatureExtraction.setMapperClass(TFIDFFeatExtractMapper.class);
		textFeatureExtraction.setCombinerClass(TFIDFFeatExtractReducer.class);
		textFeatureExtraction.setReducerClass(TFIDFFeatExtractReducer.class);
		
		textFeatureExtraction.setMapOutputKeyClass(Text.class);
		textFeatureExtraction.setMapOutputValueClass(Text.class);
		textFeatureExtraction.setOutputKeyClass(Text.class);
		textFeatureExtraction.setOutputValueClass(Text.class);
		
		textFeatureExtraction.setInputFormatClass(TextInputFormat.class);
		textFeatureExtraction.setOutputValueClass(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(textFeatureExtraction, inputPath);
		FileOutputFormat.setOutputPath(textFeatureExtraction, outputPath);
		
		if(textFeatureExtraction.waitForCompletion(true)){
			return 0;
		}
		return 1;
	}
	
	public static void main(String[] args) throws Exception{
		int returnCode = ToolRunner.run(new FeatureExtraction(), args);
		System.exit(returnCode);
	}

}
