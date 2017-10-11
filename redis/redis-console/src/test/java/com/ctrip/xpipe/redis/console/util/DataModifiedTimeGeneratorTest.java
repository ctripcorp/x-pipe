package com.ctrip.xpipe.redis.console.util;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * @author shyin
 *
 * Aug 30, 2016
 */
public class DataModifiedTimeGeneratorTest {
	@Test
	public void testDataModifiedTimeGenerator() {
		Date date = new Date();
		assertEquals(DataModifiedTimeGenerator.generateModifiedTime(date), new SimpleDateFormat("yyyyMMddHHmmssSSS").format(date));
	}
}
