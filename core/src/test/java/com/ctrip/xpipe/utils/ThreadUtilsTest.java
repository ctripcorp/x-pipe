package com.ctrip.xpipe.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 15, 2020
 */
public class ThreadUtilsTest {

    @Test
    public void testBestEffortThreadNums() {
        Assert.assertTrue(ThreadUtils.bestEffortThreadNums() < 8);
    }
}