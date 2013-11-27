package org.kaggle.fb3.predict;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.kaggle.fb3.util.CleanData;
import org.kaggle.fb3.util.CollectionsExtn;
import org.kaggle.fb3.util.Ngram;

/**
 * 
 * @author root
 *
 */
public class PredictTagMapper extends Mapper<Object, Text, LongWritable, Text> {

	final String SPLIT_STRING = "\",\"";

	private LongWritable outputkey = new LongWritable();
	//private MapWritable outputValue = new MapWritable();
	private Text outputValue = new Text();
	
	
	//wordFrequencyInTag format <word,<tag,tf>>
	private Map<String, Map<String, Double>> wordFrequencyInTag = new HashMap<String, Map<String, Double>>();
	
	//wordInTagsMap format <word,idf>
	private Map<String, Integer> wordInTagsMap = new HashMap<String, Integer>();
	//private Set<String> tags = new HashSet<String>();
	
	//tagWordProb format <tag, probability>
	private Map<String,Double> tagWordProb = new HashMap<String,Double>();

	private int progressCoounter = 0;

	public static final String BUFFER_READER_GROUP = "BUFFER_READER_GROUP";
	public static final String BUFFER_FILE_COUNTER = "BUFFER_FILE_COUNTER";
	public static final String BUFFER_READER_COUNTER = "BUFFER_READER_COUNTER";
	
	private BloomFilter filter = new BloomFilter();
	//private int tagsSize = 0;
	
	//private boolean loggingEnable = true;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
		System.out.println("Reading the files into cache");
		
		
		System.out.println("The files from cache are :");
		for(URI file:files){
			System.out.println(file.getPath());
		}
		

		Configuration conf = context.getConfiguration();
		
		FileSystem fs = null;

		InputStream in = null;
		InputStreamReader reader = null;
		
		System.out.println("Reading Bloom filter from :"+files[0].getPath());
		
		//open local file for read
		try{
			
			fs = FileSystem.get(files[0], conf);
			in = fs.open(new Path(files[0]));					
			
			DataInputStream strm = new DataInputStream(in);
			filter.readFields(strm);
			strm.close();
		}catch (Exception e) {
			System.out.println(e.getStackTrace());
			
			throw new IOException(e);
		}finally{
			if(in != null){
				in.close();
			}
			
			if(fs != null){						
				//fs.close();
			}
		}
		
		try {

			System.out.println("Reading all the lines from the file parts 0-9");

			for (int i = 1; i <= 10; i++) {
				System.out.println("Reading all the lines from the file part ---  "	+ i);
				System.out.println("file path --" + files[i]);

				//incrementing the file counter
				//context.getCounter(BUFFER_READER_GROUP, BUFFER_FILE_COUNTER).increment(1);				
				 
				String[] ws = null;
				Map<String, Double> tagMap = null;
				BufferedReader buffReader = null;
				try {
					fs = FileSystem.get(files[i], conf);
					in = fs.open(new Path(files[i]));					
					reader = new InputStreamReader(in);
					buffReader = new BufferedReader(reader);
					String line = null;
					System.out.println("reading the cache file");
					while ((line = buffReader.readLine()) != null) {

						//incrementing the line counter
						//context.getCounter(BUFFER_READER_GROUP, BUFFER_READER_COUNTER).increment(1);

						ws = line.split("\\t");
						if(ws[1].getBytes() != null && ws[1].getBytes().length > 0 && filter.membershipTest(new Key(ws[1].getBytes()))){							
							
							if(ws[1].contains("0-")|(ws[1].contains("-") && Integer.valueOf(ws[2]) < 4) | Integer.valueOf(ws[2]) < 7){
									continue;
								}
								tagMap = wordFrequencyInTag.get(ws[1]);
								if (null == tagMap) {
									tagMap = new HashMap<String, Double>(); //holds map - tf
								}
								//tags.add(ws[0]);
								tagMap.put(ws[0],Double.valueOf(ws[2]) / Double.valueOf(ws[3]));
								wordFrequencyInTag.put(ws[1], tagMap);													
						}	
						
						if(!tagWordProb.containsKey(ws[0])){
							tagWordProb.put(ws[0], Double.valueOf(ws[5])/Double.valueOf(ws[4]));
						}

						// reporting the progress
						progressCoounter++;

						if (progressCoounter % 1000000 == 0) {
							context.progress();
							System.out.println("Progressed Recoeds :" + progressCoounter);
						}
					}
				} catch (Exception e) {
					System.out.println(e.getStackTrace());
					
					throw new IOException(e);
				}finally{
					if(in != null){
						in.close();
					}
					if(null != reader){						
						reader.close();
					}
					if(null != buffReader){						
						buffReader.close();
					}
					if(fs != null){						
						//fs.close();
					}
				}
				
				
			}
				
			BufferedReader buffReader = null;
				try {
					fs = FileSystem.get(files[11], conf);
					in = fs.open(new Path(files[11]));					
					reader = new InputStreamReader(in);
					buffReader = new BufferedReader(reader);
					

				String[] w = null;				
				System.out.println("Reading all the lines from the file2 from "+ files[11]);
				progressCoounter = 0;
				String line = null;
				while ((line = buffReader.readLine()) != null) {
					//context.getCounter(BUFFER_READER_GROUP,BUFFER_READER_COUNTER).increment(1);
					
					w = line.split("\\t");
					
					if(filter.membershipTest(new Key(w[0].trim().getBytes())) && wordFrequencyInTag.containsKey(w[0])){
						//System.out.print(w[0]+"  ----   "+w[1]);
						try{
							wordInTagsMap.put(w[0], Integer.valueOf((w[1].trim())));
						}catch(Exception e){
							System.out.println("Error encounter for key with value in try:" + w[0] +"  -  "+ w[1]);							
						}
					}
							
					progressCoounter++;

					if (progressCoounter % 100000 == 0) {
						context.progress(); // reporting the progress
						System.out.println("Progressing ....");
					}
				}
				}catch(Exception e){
					System.out.println(e.getStackTrace());
					throw new IOException(e);
					
				}finally{
					if(in != null){
						in.close();
					}
					if(reader  != null){
						reader.close();
					}
					if(null != buffReader){
						buffReader.close();
					}
					if(fs != null){
						//fs.close();
					}
					System.out.println("PROCESSED ALL THE CHACHED FILES");
				}
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		//tagsSize = tags.size();
		//tags = null;
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String testLine = value.toString();

		String[] columns = testLine.split(SPLIT_STRING);
		int splitSize = columns.length;		
		
		String lineId = columns[0];
		lineId = lineId.replaceAll("^NEW_LINE_CHHAR\"", "").trim();

		if (splitSize >= 3) {

			String title = columns[1];
			
			title = CleanData.clean(title);
			
			String body = null;
			if(columns[2].length() >= 150)
				body = columns[2].substring(0, 150);
			else
				body = columns[2];
			
			body = CleanData.clean(body);
			String content = null;

			StringBuffer contentBuff = new StringBuffer(title);
			contentBuff.append(" ");
			contentBuff.append(body);
			contentBuff.append(" ");
			contentBuff.append(Ngram.getBiGram(title));
			contentBuff.append(" ");
			contentBuff.append(Ngram.getBiGram(body));

			content = contentBuff.toString().trim();
			//outputkey.set(lineId);
			// outputValue.set(title);

			Map<String, Double> tagFrequency = null;
			int tagsForWord = 0;
			String[] words = content.split("\\s");
			
			//create a local map to hold all the tags and scores and radiate the top 3 scores from this map
			Map<String,Double> tagScoreMap = new HashMap<String,Double>();
			
			for (String word : words) {
				tagFrequency = wordFrequencyInTag.get(word);
				//System.out.println("wordInTagsMap size "+ wordInTagsMap.size());
				Integer tagsCount = wordInTagsMap.get(word);
				if(tagsCount != null)
					tagsForWord = tagsCount;

				double tfIdf = 0;
				if (null != tagFrequency) {
					String tag = null;
					double wordFreqInTag = 0;
					

					String tagTrimed = null;
					for (Map.Entry<String, Double> entry : tagFrequency.entrySet()) {
						tag = entry.getKey();
						wordFreqInTag = entry.getValue();
						tagTrimed = tag.replaceAll("[^a-z-]","");
						
						double localWt = 1;
						if (tagTrimed.equals(word) && tagWordProb.containsKey(tag)) {
							localWt = Math.pow(400,new Double(tagWordProb.get(tag)));
						}
						
						// calculate for tf-idf					
						int max_doc_count = Math.max(5000, tagsForWord);
						tfIdf = wordFreqInTag
								* Math.log(new Double((max_doc_count+1) / (tagsForWord + 1)))
								* localWt;
						
						if(tagScoreMap.containsKey(tag)){
							tagScoreMap.put(tag, tagScoreMap.get(tag)+tfIdf);
						}else{
							tagScoreMap.put(tag,tfIdf);
						}
						
						//outputValue.put(new Text(tag),new FloatWritable(tfIdf));						
						
					}
				}
			}
			
			outputValue.set(lineId+","+CollectionsExtn.getSortedTags(tagScoreMap, 3));
			outputkey.set(Long.parseLong(lineId));
			context.write(outputkey, outputValue);

			/**
			 * reading from database 
			 * try { outputValue.set(new
			 * Text(DBUtil.getPredictedTags(Integer.parseInt(lineId),title))); }
			 * catch (Exception e) {
			 * 
			 * throw new IOException(e); }
			 **/

		}	
	}

}
