package org.kaggle.fb3.predict;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PredictTagTest {
	
	private MapDriver<Object,Text,Text,MapWritable> mapDriver;
	private ReduceDriver<Text,MapWritable,Text,Text> reduceDriver;
	

	@Before
	public void setUp() throws Exception {
		
		PredictTagMapper mapper = new PredictTagMapper();
		PredictTagReducer reducer = new PredictTagReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		
		//setting configuration
		Configuration conf = new Configuration();
		conf.setStrings("files","kaggle_facebook3_train2/part-r-00000","kaggle_facebook3_train2/part-r-00001","kaggle_facebook3_train2/part-r-00002","kaggle_fb_train3.txt");
		
		
	}

	@Test
	public void testMapper() throws IOException{
		
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_facebook3_train2/part-r-00000");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_facebook3_train3.txt");
		String line = "Mocking the context object in MR unit";
		mapDriver.withInput(new LongWritable(0),new Text(line));
		
		Text outkey = new Text("0");
		
		
		MapWritable outputValue = new MapWritable();
		outputValue.put(new Text("hadoop"), new DoubleWritable(0.003));
		mapDriver.withOutput(outkey,outputValue);
		mapDriver.runTest();
	}

}
