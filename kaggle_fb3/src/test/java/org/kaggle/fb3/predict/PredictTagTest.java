package org.kaggle.fb3.predict;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class PredictTagTest {
	
	private MapDriver<Object,Text,Text,Text> mapDriver;
	//private ReduceDriver<Text,MapWritable,Text,Text> reduceDriver;
	

	@Before
	public void setUp() throws Exception {
		
		PredictTagMapper mapper = new PredictTagMapper();
		//PredictTagReducer reducer = new PredictTagReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		//reduceDriver = ReduceDriver.newReduceDriver(reducer);
		
		
		
	}

	@Test
	public void testMapper() throws IOException{
		
	
		/*mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_trainwords_bloom_filter");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00000-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00001-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00002-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00003-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00004-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00005-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00006-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00007-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00008-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2/part-r-00009-sample");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train3-sample.txt");*/
		
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_trainwords_bloom_filter");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00000");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00001");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00002");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00003");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00004");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00005");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00006");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00007");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00008");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train2_sample/part-r-00009");
		mapDriver.addCacheFile("/root/hadoop-launcher/hdfs-launcher/kaggle_fb3_train3_sample.txt");
		
		String line = "NEW_LINE_CHHAR\"6034196\",\"Nodes inside Cisco aspxauth doc app SSH outbound SSH application apple\",\"<p>How do I disable site-specific hotkeys\"";
		mapDriver.withInput(new LongWritable(0),new Text(line));
		
		Text outkey = new Text("6034196");
		
		
		Text outputValue = new Text();
		outputValue.set("6034196,\".app .doc .ds-store\"");
		//outputValue.put(new Text(".a"), new DoubleWritable(0.003));
		mapDriver.withOutput(outkey,outputValue);
		mapDriver.runTest();
	}
	
	

}
