package org.kaggle.fb3.predict;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.kaggle.fb3.dao.DBUtil;

public class PredictTagReducer extends Reducer<Text,Text,NullWritable,Text>{	
	
	private Text outputkey = new Text();
	private Text outputValue = new Text();
	
	
	@Override
	protected void reduce(Text key,Iterable<Text> values, Context context)throws IOException,InterruptedException{/*
		
		Map<Double,String> scoreTagMap = new TreeMap<Double,String>(Collections.reverseOrder());
		
		Map<String,Double> tagMapScore = new HashMap<String,Double>();
		for(MapWritable tagTFIDF:values){
			String tag = null;
			Double tfIdf = null;
			for(Entry<Writable,Writable> entry:tagTFIDF.entrySet()){
				tag = ((Text)entry.getKey()).toString();
				tfIdf = ((DoubleWritable)entry.getValue()).get();
				if(tagMapScore.containsKey(tag)){
					tfIdf = tfIdf + tagMapScore.get(tag);
				}
				tagMapScore.put(tag, tfIdf);
			}			
		}
		
		String tag = null;
		Double score = null;
		for(Entry<String,Double> entry:tagMapScore.entrySet()){
			tag = entry.getKey();
			score = entry.getValue();
			if(scoreTagMap.containsKey(score)){
				score = score + 0.0001;
			}
			scoreTagMap.put(score, tag);
		}
		
		StringBuffer top4Tags = new StringBuffer();
		StringBuffer top4TagsLog = new StringBuffer();
		int counter = 0;
		for(Entry<Double,String> entry : scoreTagMap.entrySet()){			
			top4Tags.append(entry.getValue());
			
			//logging
			top4TagsLog.append(entry.getValue());
			top4TagsLog.append(" ");
			top4TagsLog.append(entry.getKey());
			
			if(counter == 3){
				break;
			}else{
				top4Tags.append(" ");
				top4TagsLog.append(" ");
				counter++;
			}
		}
		
		outputkey.set(key);
		outputValue.set(top4Tags.toString());
		context.write(outputkey, outputValue);
		
		//updating the database
		try {
			DBUtil.logInDB(Integer.parseInt(key.toString()),top4TagsLog.toString());
		} catch (Exception e) {
			//if not able to update databse then suppressing the error
			//throw new IOException(e);
			//System.out.println("Error updating databse");
		}
		
	*/
		for(Text tagsPredict : values){
			context.write(NullWritable.get(), tagsPredict);
		}
		
	
	}
	
	
	
}
