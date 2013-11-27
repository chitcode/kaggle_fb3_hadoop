package org.kaggle.fb3.predict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PredictTag extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = super.getConf();
		Job tagPredict = new Job(conf); 
		
		long milliseconds = 1000 * 60 * 60 * 2; //setting 2 hours
		conf.setLong("mapred.healthChecker.script.timeout", milliseconds);
		conf.setLong("mapred.task.timeout", milliseconds);	
		
		//conf.setLong("mapreduce.healthChecker.script.timeout", milliseconds);
		//conf.setLong("mapreduce.task.timeout", milliseconds);
		
		
		tagPredict.setJobName("Prediction for tags");
		tagPredict.setJarByClass(getClass());
		tagPredict.setNumReduceTasks(1);
		
		//setting mapper, combiner, reducer
		tagPredict.setMapperClass(PredictTagMapper.class);
		//tagPredict.setCombinerClass(PredictTagReducer.class);
		tagPredict.setReducerClass(PredictTagReducer.class);
		
		tagPredict.setMapOutputKeyClass(LongWritable.class);
		tagPredict.setMapOutputValueClass(Text.class);
		tagPredict.setOutputKeyClass(NullWritable.class);
		tagPredict.setOutputValueClass(Text.class);
				
		//idfCount.setInputFormatClass(KeyValueTextInputFormat.class);
		tagPredict.setInputFormatClass(TextInputFormat.class);
		tagPredict.setOutputValueClass(TextInputFormat.class);
		
		FileInputFormat.setInputPaths(tagPredict, inputPath);
		FileOutputFormat.setOutputPath(tagPredict, outputPath);
		
		int code = tagPredict.waitForCompletion(true)?0:1;
		
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
		Configuration conf = new Configuration();
		int returnCode = ToolRunner.run(conf,new PredictTag(), args);
		System.exit(returnCode);
	}


}
