package com.ctrip.xpipe.redis.console.simple;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 05, 2017
 */
public class SimpleTest {


    @Test
    public void testTime(){

        System.out.println(TimeUnit.NANOSECONDS.toMillis(1000000));

    }
}
