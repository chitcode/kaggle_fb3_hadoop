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
		
		String s = "~%b1z%2b%26k%c9%bf%b1%3fh%3fd%3f%9f%2fa%90%3ft%c5%8b%a0%3f-01	2~~~~~~~~~~~~~~~~~~~~~~~~~|-directfb	1 this is cann't be done ! and ----I am testing this! And\"String########\" {a+\\\\sqrt{a+\\\\sqrt{a+\\\\sqrt{a...}}}}$ version 34.2.3 3.4";
		System.out.println(CleanData.clean(s));
	}

}
