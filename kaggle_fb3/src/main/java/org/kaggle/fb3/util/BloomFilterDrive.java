package org.kaggle.fb3.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterDrive {
	
	final static String SPLIT_STRING = "\",\"";
	
	public static void main(String[] args) throws Exception{
		
		if(args.length < 4){
			throw new Exception(" Missing required paramers inptFile / numMembers / falsePositiveRate / outputPath");			
		}
		
		Path inputFile = new Path(args[0]);
		int numMembers = Integer.parseInt(args[1]);
		float falsePositiveRate = Float.parseFloat(args[2]);
		
		Path bfFile = new Path(args[3]);
		
		//calculate the vector size and optimal k value based on approximattions
		int vectorSize = getOptimalBloomFilterSize(numMembers,falsePositiveRate);
		int nbHash = getOptimalK(numMembers,vectorSize);
		
		BloomFilter filter = new BloomFilter(vectorSize,nbHash,Hash.MURMUR_HASH);
		
		System.out.println("training Bloom filter of size "+vectorSize+" with "+nbHash+" hash functions, "
						+numMembers+ " approximate number of records, and " + falsePositiveRate + " false positive rate");
		
		//open file to read
		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());
		
		for(FileStatus status:fs.listStatus(inputFile)){
			BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
			
			System.out.println("reading " + status.getPath());
			while((line = rdr.readLine()) != null){
				
				//splitting all the columns
				String[] columns = line.split(SPLIT_STRING);
				String title = columns[1];
				title = title.toLowerCase();

				title = CleanData.clean(title);

				StringBuffer titleStrBuff = new StringBuffer(title);
				titleStrBuff.append(" ");
				titleStrBuff.append(Ngram.getBiGram(title));

				title = titleStrBuff.toString().trim();
				String[] words = title.split("\\s+");
				for(String word:words){
					filter.add(new Key(word.getBytes()));
					++numElements;
				}				
			}
			rdr.close();
		}
		
		System.out.println("Trained bloom filter with "+numElements + " entries");
		System.out.println("Serializing the bloom filter to HDFS at "+ bfFile);
		
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();
		
		System.exit(0);
	}

	private static int getOptimalK(int numMembers, int vectorSize) {
		return (int)Math.round((numMembers/vectorSize)/Math.log(2));
	}

	private static int getOptimalBloomFilterSize(int numMembers, float falsePositiveRate) {
		int size = (int)(- numMembers * (float)Math.log(falsePositiveRate)/Math.pow(Math.log(2),2));
		return size;
	}
}
