package com.ctrip.xpipe.redis.integratedtest.simple;

import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2016
 */
public class SimpleTest extends AbstractSimpleTest {

    @Test
    public void test1() {
        System.out.println(new Date(1515148244949L));

    }


    @Test
    public void test() {
        System.out.println("nihaoma");
    }

    @Test
    public void testAlloc() throws InterruptedException {

        while (true) {

            TimeUnit.MILLISECONDS.sleep(1);
            @SuppressWarnings("unused")
            byte[] data = new byte[1 << 10];
        }

    }


}
