package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *
 *         Mar 16, 2017
 */
public class LogUtils {
	
	public static String info(String message) {
		return format("info", message);
	}

	public static String warn(String message) {
		return format("warn", message);
	}

	public static String error(String message) {
		return format("error", message);
	}

	private static String format(String tag, String message) {

		return String.format("[%s][%s]%s\n", tag, DateTimeUtils.currentTimeAsString(), message);
	}

}
