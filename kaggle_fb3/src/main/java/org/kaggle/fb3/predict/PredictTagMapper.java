package org.kaggle.fb3.predict;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kaggle.fb3.util.CleanData;
import org.kaggle.fb3.util.Ngram;

public class PredictTagMapper extends Mapper<Object, Text, Text, MapWritable> {

	final String SPLIT_STRING = "\",\"";

	private Text outputkey = new Text();
	private MapWritable outputValue = new MapWritable();

	private Map<String, Map<String, Float>> wordFrequencyInTag = new HashMap<String, Map<String, Float>>();
	private Map<String, Integer> wordInTagsMap = new HashMap<String, Integer>();

	private int progressCoounter = 0;

	public static final String BUFFER_READER_GROUP = "BUFFER_READER_GROUP";
	public static final String BUFFER_FILE_COUNTER = "BUFFER_FILE_COUNTER";
	public static final String BUFFER_READER_COUNTER = "BUFFER_READER_COUNTER";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		URI[] files = DistributedCache
				.getCacheFiles(context.getConfiguration());
		System.out.println("Reading the files into cache");

		Configuration conf = context.getConfiguration();
		
		FileSystem fs = null;

		InputStream in = null;
		InputStreamReader reader = null;
		try {

			System.out.println("Reading all the lines from the file parts 0-9");

			for (int i = 0; i < 3; i++) {
				System.out
						.println("Reading all the lines from the file part -----  "
								+ i);
				System.out.println("file path --" + files[i]);

				//incrementing the file counter
				context.getCounter(BUFFER_READER_GROUP, BUFFER_FILE_COUNTER).increment(1);
				
				 
				String[] ws = null;
				Map<String, Float> tagMap = null;
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
						context.getCounter(BUFFER_READER_GROUP, BUFFER_READER_COUNTER).increment(1);

						ws = line.split("\\t");

						tagMap = wordFrequencyInTag.get(ws[0]);
						if (null == tagMap) {
							tagMap = new HashMap<String, Float>();
						}
						tagMap.put(ws[1],
								Float.valueOf(ws[2]) / Float.valueOf(ws[3]));

						wordFrequencyInTag.put(ws[0], tagMap);

						// reporting the progress
						progressCoounter++;

						if (progressCoounter % 100000 == 0) {
							context.progress();
							System.out.println("Progressing ....");
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
						fs.close();
					}
				}
			}
				
			BufferedReader buffReader = null;
				try {
					fs = FileSystem.get(files[files.length-1], conf);
					in = fs.open(new Path(files[3]));					
					reader = new InputStreamReader(in);
					buffReader = new BufferedReader(reader);
					

				String[] w = null;
				System.out.println("Reading all the lines from the file2");
				progressCoounter = 0;
				String line = null;
				while ((line = buffReader.readLine()) != null) {
					context.getCounter(BUFFER_READER_GROUP,BUFFER_READER_COUNTER).increment(1);
					
					w = line.split("\\t");
					wordInTagsMap.put(w[0], Integer.valueOf(w[1]));					
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
						fs.close();
					}
					System.out.println("PROCESSED ALL THE CHACHED FILES");
				}
		} catch (Exception e) {
			throw new IOException(e);
		}
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
			title = title.toLowerCase();

			title = CleanData.clean(title);

			StringBuffer titleStrBuff = new StringBuffer(title);
			titleStrBuff.append(" ");
			titleStrBuff.append(Ngram.getBiGram(title));

			title = titleStrBuff.toString().trim();
			outputkey.set(lineId);
			// outputValue.set(title);

			Map<String, Float> tagFrequency = null;
			int tagsForWord = 0;
			for (String word : title.split("\\s")) {
				tagFrequency = wordFrequencyInTag.get(word);
				tagsForWord = wordInTagsMap.get(word);

				Float tfIdf = 0.0f;
				if (null != tagFrequency) {
					String tag = null;
					Float wordFreqInTag = 0.0f;
					Float localWt = 1.0f;

					for (Map.Entry<String, Float> entry : tagFrequency
							.entrySet()) {
						tag = entry.getKey();
						wordFreqInTag = entry.getValue();
						if (tag.equalsIgnoreCase(word)) {
							localWt = 3.0f;
						}
						// calculate for tf-idf
						tfIdf = wordFreqInTag
								* (float)Math.log(new Double(10000000 / (tagsForWord + 1)))
								* localWt;
						outputValue.put(new Text(tag),
								new FloatWritable(tfIdf));
						context.write(outputkey, outputValue);
					}
				}
			}

			/**
			 * reading from database try { outputValue.set(new
			 * Text(DBUtil.getPredictedTags(Integer.parseInt(lineId),title))); }
			 * catch (Exception e) {
			 * 
			 * throw new IOException(e); }
			 **/

		}
	}

}
