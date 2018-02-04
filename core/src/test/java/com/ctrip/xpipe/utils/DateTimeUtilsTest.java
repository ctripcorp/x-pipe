package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 *         <p>
 *         Nov 27, 2017
 */
public class DateTimeUtilsTest extends AbstractTest {

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

    @Test
    public void testGetMinutesLasterDate() throws Exception {
        Date now = new Date();
        Date after = DateTimeUtils.getMinutesLaterThan(new Date(), 1);
        long duration = after.getTime() - now.getTime();
        Assert.assertTrue((Math.abs(TimeUnit.MINUTES.toMillis(1L) - duration) < TimeUnit.SECONDS.toMillis(1)));

        after = DateTimeUtils.getMinutesLaterThan(new Date(), 15);
        duration = after.getTime() - now.getTime();
        Assert.assertTrue((Math.abs(TimeUnit.MINUTES.toMillis(15L) - duration) < TimeUnit.SECONDS.toMillis(1)));
    }


    @Test
    public void testFormat() {

        logger.info(DateTimeUtils.timeAsString(0));
        logger.info(DateTimeUtils.timeAsString(1));
        logger.info(DateTimeUtils.timeAsString(-1));
    }

    @Test
    public void test() {

        long begin = System.currentTimeMillis();

        long time = begin;
        int count = 1 << 20;

        String result = null;
        for (int i = 0; i < count; i++) {
            result = DateTimeUtils.timeAsString(time++);
        }

        long end = System.currentTimeMillis();

        logger.info("{} ns", (end - begin) * 1000000 / count);


    }

}