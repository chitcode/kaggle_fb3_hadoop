package org.kaggle.fb3.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class CollectionsExtnTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		Map<String,Double> tagMap = new HashMap<String,Double>();
		tagMap.put("a", new Double(5.0));
		tagMap.put("c", new Double(4.0));
		tagMap.put("b", new Double(6.0));
		tagMap.put("e", new Double(3.2));
		tagMap.put("d", new Double(5.1));
		
		System.out.println(CollectionsExtn.getSortedTags(tagMap, 3));
	}

}
