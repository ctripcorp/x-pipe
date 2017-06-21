package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *
 *         Mar 21, 2017
 */
public class UrlUtils {

	public static String format(String url) {

		int index = url.indexOf("://");
		if (index == -1) {
			return url.replaceAll("/+", "/");
		}

		String prefix = url.substring(0, index + 3);
		String following = url.substring(index + 3);

		return prefix + following.replaceAll("/+", "/");

	}
}
