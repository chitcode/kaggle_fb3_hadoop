package org.kaggle.fb3.cv;

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

public class CVGenerator extends Configured implements Tool{
	
	@Override
		public int run(String[] args) throws Exception {
			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);
			
			Configuration conf = super.getConf();
			Job tagPredict = new Job(conf); 
			
			long milliseconds = 1000 * 60 * 60 ; //setting 1 hours
			conf.setLong("mapred.healthChecker.script.timeout", milliseconds);
			conf.setLong("mapred.task.timeout", milliseconds);	
			
			tagPredict.setJobName("Prediction for tags");
			tagPredict.setJarByClass(getClass());
			tagPredict.setNumReduceTasks(0);
			
			//setting mapper, combiner, reducer
			tagPredict.setMapperClass(CVGeneratorMapper.class);
			
			tagPredict.setMapOutputKeyClass(Text.class);
			tagPredict.setMapOutputValueClass(Text.class);
			
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
			int returnCode = ToolRunner.run(conf,new CVGenerator(), args);
			System.exit(returnCode);
		}


}
