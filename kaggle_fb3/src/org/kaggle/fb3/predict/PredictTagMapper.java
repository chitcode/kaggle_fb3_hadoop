package org.kaggle.fb3.predict;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kaggle.fb3.util.CleanData;
import org.kaggle.fb3.util.Ngram;

public class PredictTagMapper extends Mapper<Object,Text,Text,MapWritable>{
	
	final String SPLIT_STRING = "\",\"";
	
	private Text outputkey = new Text();
	//private Text outputValue = new Text();
	private MapWritable outputValue = new MapWritable();
	
	private Map<String,Map<String,Double>> wordFrequencyInTag = new HashMap<String,Map<String,Double>>();
	private Map<String,Integer> wordInTagsMap = new HashMap<String,Integer>();
	
	private int progressCoounter = 0;
	
	public static final String BUFFER_READER_GROUP = "BUFFER_READER_GROUP";
	public static final String BUFFER_FILE_COUNTER = "BUFFER_FILE_COUNTER";
	public static final String BUFFER_READER_COUNTER = "BUFFER_READER_COUNTER";
	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
		System.out.println("Reading the files into cache");
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(files[0],conf);
		
		InputStream in = null;
		try{		
			
			System.out.println("Reading all the lines from the file parts 0-9");
			
			for(int i = 0; i < 10; i++){				
				System.out.println("Reading all the lines from the file part -----  " + i);
				System.out.println("file path --" + files[i]);				
				
				context.getCounter(BUFFER_READER_GROUP,BUFFER_FILE_COUNTER).increment(1);
				
				in = fs.open(new Path(files[i]));
				List<String> lines = IOUtils.readLines(in);
				String[] ws = null;
				
				Map<String,Double> tagMap = null;
				
				for(String line: lines){
					
					context.getCounter(BUFFER_READER_GROUP,BUFFER_READER_COUNTER).increment(1);
					
					ws = line.split("\\t");
					
					tagMap = wordFrequencyInTag.get(ws[0]);
					if(null == tagMap){
						tagMap = new HashMap<String,Double>();		
					}	
					tagMap.put(ws[1], Double.valueOf(ws[2])/Double.valueOf(ws[3]));				
					
					wordFrequencyInTag.put(ws[1], tagMap);
					
					//reporting the progress
					progressCoounter++;
					
					if(progressCoounter % 1000 == 0){
						context.progress();
					}
				}
				
				
			}
			
			
			in = fs.open(new Path(files[10]));
			List<String>  lines = IOUtils.readLines(in);
			String[] w = null;
			System.out.println("Reading all the lines from the file2");		
			progressCoounter = 0;
			for(String line: lines){
				context.getCounter(BUFFER_READER_GROUP,BUFFER_READER_COUNTER).increment(1);
				 w = line.split("\\t");
				wordInTagsMap.put(w[0],Integer.valueOf(w[1]));
				
				//reporting the progress
				progressCoounter++;
				
				if(progressCoounter % 1000 == 0){
					context.progress();
				}
			}
		}catch(Exception e){
			throw new IOException(e);
		}
	}
	
	
	@Override
	protected void map(Object key,Text value, Context context)throws IOException,InterruptedException{
		
		String testLine = value.toString();
		
		String[] columns = testLine.split(SPLIT_STRING);
		int splitSize = columns.length;
		
		String lineId = columns[0];
		lineId = lineId.replaceAll("^NEW_LINE_CHHAR\"","").trim();
			
			if(splitSize >= 3){
				
				String title = columns[1];
				title = title.toLowerCase();
				
				title = CleanData.clean(title);
				
				StringBuffer titleStrBuff = new StringBuffer(title);
				titleStrBuff.append(" ");
				titleStrBuff.append(Ngram.getBiGram(title));
				
				title = titleStrBuff.toString().trim();
				outputkey.set(lineId);
				//outputValue.set(title);			
				
				Map<String,Double> tagFrequency = null;
				int tagsForWord = 0;
				for(String word:title.split("\\s")){
					tagFrequency = wordFrequencyInTag.get(word);
					tagsForWord = wordInTagsMap.get(word);
					
					double tfIdf = 0.0;
					if(null != tagFrequency){
						String tag = null;
						double wordFreqInTag = 0.0;
						double localWt = 1.0;
						
						for(Map.Entry<String,Double> entry:tagFrequency.entrySet()){
							tag = entry.getKey();
							wordFreqInTag = entry.getValue();
							if(tag.equalsIgnoreCase(word)){
								localWt = 3.0;
							}
							//calculate for tf-idf
							tfIdf = wordFreqInTag * Math.log(10000000/(tagsForWord + 1)) * localWt;
							outputValue.put(new Text(tag), new DoubleWritable(tfIdf));
							context.write(outputkey, outputValue);
						}
					}
				}
				
				
				/** reading from database
				try {
					outputValue.set(new Text(DBUtil.getPredictedTags(Integer.parseInt(lineId),title)));
				} catch (Exception e) {
					
					throw new IOException(e);
				}
				**/
								
			}		
	}

}
