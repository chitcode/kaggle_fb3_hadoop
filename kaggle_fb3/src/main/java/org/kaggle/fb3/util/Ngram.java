package org.kaggle.fb3.util;

public class Ngram {
	
	public static String getBiGram(String line){
		
		StringBuffer bigramLine = new StringBuffer();
		String[] words = line.split("\\s+");
		
		
		for(int i = 0; i < words.length-1; i++){
			bigramLine.append(words[i]);
			bigramLine.append("-");
			bigramLine.append(words[i+1]);	
			bigramLine.append(" ");
		}	
		
		return bigramLine.toString().replace("\\s?$", "");
	}

}
