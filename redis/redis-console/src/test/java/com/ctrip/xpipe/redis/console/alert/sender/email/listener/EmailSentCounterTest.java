package com.ctrip.xpipe.redis.console.alert.sender.email.listener;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class EmailSentCounterTest {

    private EmailSentCounter instance = new EmailSentCounter();

    @Test
    public void testSuccess() throws Exception {
        instance.success();
        Assert.assertEquals(1, instance.getTotal());
        Assert.assertEquals(1, instance.getSuccess());
        Assert.assertEquals(0, instance.getFailed());
    }

    @Test
    public void testFail() throws Exception {
        instance.fail(new Exception("test"));
        Assert.assertEquals(1, instance.getTotal());
        Assert.assertEquals(0, instance.getSuccess());
        Assert.assertEquals(1, instance.getFailed());
    }

    @Test
    public void manualTestScheduled() throws InterruptedException {
        int count = 0;
        while(count++ < 66) {
            if ((count & 1) == 0) {
                instance.success();
            } else {
                instance.fail(new Exception());
            }
            Thread.sleep(1000);
        }

    }

}