package com.ctrip.xpipe.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public class DateTimeUtilsTest {

    @Test
    public void testGetHoursLaterDate1() throws Exception {
        Date now = new Date();
        Date date = DateTimeUtils.getHoursLaterDate(1);
        Assert.assertTrue(now.before(date));
        long duration = date.getTime() - now.getTime();
        TimeUnit.HOURS.toMillis(duration);
        Assert.assertTrue(Math.abs(TimeUnit.HOURS.toMillis(1) - duration) < TimeUnit.SECONDS.toMillis(1));
    }

    @Test
    public void testGetHoursLaterDate2() throws Exception {
        Date now = new Date();
        Date before = DateTimeUtils.getHoursLaterDate(-1);
        Assert.assertTrue(now.after(before));
        long duration = now.getTime() - before.getTime();
        Assert.assertTrue(Math.abs(TimeUnit.HOURS.toMillis(1) - duration) < TimeUnit.SECONDS.toMillis(1));
    }

}