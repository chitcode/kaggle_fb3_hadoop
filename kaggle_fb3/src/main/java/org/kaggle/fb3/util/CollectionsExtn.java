package org.kaggle.fb3.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Chit
 * 
 * 
 *
 */
public class CollectionsExtn {

	/**
	 * 
	 * @param map
	 * @param trimSize
	 * @return String of top tags of length trimSize
	 * 
	 * Source : webcache.googleusercontent.com/search?q=cache:bXwIBh97VG4J:stackoverflow.com/questions/13852725/java-map-sort-by-value+&cd=1&hl=en&ct=clnk&gl=in&client=ubuntu
	 */
	public static String getSortedTags(Map<String,Double> map,int trimSize){
	List<Map.Entry> a = new ArrayList<Map.Entry>(map.entrySet());
	Collections.sort(a, new Comparator() {
	             public int compare(Object o1, Object o2) {
	                 Map.Entry e1 = (Map.Entry) o1;
	                 Map.Entry e2 = (Map.Entry) o2;
	                 return (((Comparable) e1.getValue()).compareTo(e2.getValue()) * -1);
	             }
	         });
	
	StringBuffer tags = new StringBuffer("\"");
	
	int i = 0;
	
	
	for (Map.Entry e : a) {
	    if(i == trimSize)
	    	break;
	    
	    i++;
	    tags.append(e.getKey());
	    tags.append(" ");
	   
	}
	
	//taag_scores
	
	StringBuffer tagScoress = new StringBuffer("\"");
	i = 0;
	for (Map.Entry e : a) {
	    if(i == 10)
	    	break;
	    
	    i++;
	    tagScoress.append(e.getKey());
	    tagScoress.append("--");
	    tagScoress.append(e.getValue());
	    tagScoress.append(" ");
	   
	}
	
	return tags.toString().trim() + "\""+"   "+tagScoress.toString().trim();
	//return tags.toString().trim() + "\"";
}
}
