package org.kaggle.fb3.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CollectionsExtn {

	
	public static String getSortedTags(Map<String,Float> map,int trimSize){
	List<Map.Entry> a = new ArrayList<Map.Entry>(map.entrySet());
	Collections.sort(a, new Comparator() {
	             public int compare(Object o1, Object o2) {
	                 Map.Entry e1 = (Map.Entry) o1;
	                 Map.Entry e2 = (Map.Entry) o2;
	                 return ((Comparable) e1.getValue()).compareTo(e2.getValue());
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
	return tags.toString().trim() + "\"";
}
}
