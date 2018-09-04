package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class CatHealthEventProcessorTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private List<DelayHealthEventProcessor> delays;

    @Autowired
    private List<PingHealthEventProcessor> pings;

    @Autowired
    private CatHealthEventProcessor processor;

    @Test
    public void testCatHealthEventProcessorTest() {
        Assert.assertTrue(delays.contains(processor));
        Assert.assertTrue(pings.contains(processor));
    }
}