package com.ctrip.xpipe.utils;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Calendar;
import java.util.Date;

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

	public static String currentTimeAsString(String currentFormat) {
		return FastDateFormat.getInstance(currentFormat).format(new Date());
	}

	public static String timeAsString(Date date) {
		return FastDateFormat.getInstance(format).format(date);
	}

	public static String timeAsString(Date date, String currentFormat) {
		return FastDateFormat.getInstance(currentFormat).format(date);
	}

	public static String timeAsString(long timeMilli) {
		if(timeMilli < 0){
			return String.format("wrong time: %d", timeMilli);
		}
		return FastDateFormat.getInstance(format).format(new Date(timeMilli));
	}

	public synchronized static Date getHoursLaterDate(int hours) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.HOUR_OF_DAY, hours);
		return cal.getTime();
	}

	public synchronized static Date getMinutesLaterThan(Date date, int minute) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.MINUTE, minute);
		return cal.getTime();
	}

	public synchronized static Date getSecondsLaterThan(Date date, int second) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.SECOND, second);
		return cal.getTime();
	}

	public static Date getHoursBeforeDate(Date date, int hours) {
		int minusHours = -Math.abs(hours);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, minusHours);
		return cal.getTime();
	}

	public static Date getSecondBeforeDate(Date date, int second) {
		int minusSecond = -Math.abs(second);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.SECOND, minusSecond);
		return cal.getTime();
	}

	public static Date getNearestHour() {
		return DateUtils.ceiling(new Date(), Calendar.HOUR);
	}
}
