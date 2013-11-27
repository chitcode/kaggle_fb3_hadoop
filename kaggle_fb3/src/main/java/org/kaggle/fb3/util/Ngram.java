package org.kaggle.fb3.util;

public class Ngram {
	
	public static String getBiGram(String line){
		
		StringBuffer bigramLine = new StringBuffer();
		String[] words = line.trim().split("\\s+");
		
		
		for(int i = 0; i < words.length-1; i++){
			bigramLine.append(words[i]);
			bigramLine.append("-");
			bigramLine.append(words[i+1]);	
			bigramLine.append(" ");
		}	
		
		return bigramLine.toString().replace("\\s?$", "");
	}
	
	public static String getFullTitle(String title){
		title = title.toLowerCase();
		StringBuffer fullTitle = new StringBuffer();
		String[] words = title.trim().split("\\s+");
				
		for(String word:words){
			fullTitle.append("-");
			fullTitle.append(word);			
		}
		fullTitle.replace(0, 1, "");
		return fullTitle.toString();
	}
	
	public static void main(String[] args){
		System.out.println(Ngram.getFullTitle("lt;= -\\w)(?:(?=\""));
	}

}
