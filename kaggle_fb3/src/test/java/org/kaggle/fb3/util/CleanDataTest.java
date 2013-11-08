package org.kaggle.fb3.util;

import static org.junit.Assert.*;

import org.apache.log4j.lf5.PassingLogRecordFilter;
import org.junit.Before;
import org.junit.Test;

public class CleanDataTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		
		String s = "this is cann't be done ! and ----I am testing this! And\"String########\" {a+\\\\sqrt{a+\\\\sqrt{a+\\\\sqrt{a...}}}}$ version 34.2.3 3 .4";
		System.out.println(CleanData.clean(s));
	}

}
