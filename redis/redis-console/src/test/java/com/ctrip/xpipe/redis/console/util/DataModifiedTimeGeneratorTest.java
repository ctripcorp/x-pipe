package com.ctrip.xpipe.redis.console.util;

import com.ctrip.xpipe.utils.DateTimeUtils;
import com.mysql.cj.util.TimeUtil;
import org.junit.Assert;
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

	@Test
	public void testGenerateModifiedTime() {
		Date date1 = new Date();
		Date date2 = DateTimeUtils.getSecondsLaterThan(date1, 10);
		Assert.assertEquals(DataModifiedTimeGenerator.generateModifiedTime(date1), DataModifiedTimeGenerator.generateModifiedTime(date1));
		Assert.assertTrue(DataModifiedTimeGenerator.generateModifiedTime(date1).compareTo(DataModifiedTimeGenerator.generateModifiedTime(date2)) < 0);
		Assert.assertTrue(DataModifiedTimeGenerator.generateModifiedTime(date2).compareTo(DataModifiedTimeGenerator.generateModifiedTime(date1)) > 0);
	}

}
