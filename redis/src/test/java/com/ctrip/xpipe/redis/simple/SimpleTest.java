package com.ctrip.xpipe.redis.simple;

import java.util.Calendar;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:44:03 PM
 */
public class SimpleTest extends AbstractTest{
	
	@Test
	public void testFormat(){
		
		System.out.println(String.format("%,d", 111111111));
		System.out.println(String.format("%1$s", "a b c"));
		System.out.println(String.format("%s", "a b c"));
//		System.out.println(String.format("%2$tm %2$te,%2$tY", Calendar.getInstance()));
		
		Calendar calendar = Calendar.getInstance();
		System.out.println(calendar);
//		logger.printf(Level.INFO, "%2$tm %2$te,%2$tY", calendar);
		
	}

}
