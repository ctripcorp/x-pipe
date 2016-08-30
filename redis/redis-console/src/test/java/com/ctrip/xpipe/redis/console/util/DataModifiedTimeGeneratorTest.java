package com.ctrip.xpipe.redis.console.util;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

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
