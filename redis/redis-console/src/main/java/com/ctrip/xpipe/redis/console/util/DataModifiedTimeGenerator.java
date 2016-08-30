package com.ctrip.xpipe.redis.console.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author shyin
 *
 * Aug 15, 2016
 */
public class DataModifiedTimeGenerator {
	public static String generateModifiedTime() {
		return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
	}

	protected static String generateModifiedTime(Date date) {
		return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(date);
	}
}
