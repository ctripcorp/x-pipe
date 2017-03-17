package com.ctrip.xpipe.utils;

import java.util.Date;

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * @author wenchao.meng
 *
 *         Mar 16, 2017
 */
public class DateTimeUtils {

	public static String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";

	public static String currentTimeAsString() {
		return FastDateFormat.getInstance(format).format(new Date());
	}

	public static String currentTimeAsString(Date date) {
		return FastDateFormat.getInstance(format).format(date);
	}

}
