package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultPingHealthEventProcessorTest extends AbstractRedisTest {

    private DefaultPingHealthEventProcessor processor = new DefaultPingHealthEventProcessor();

    @Test
    public void testMarkDown() {
        processor.markDown(new DefaultRedisHealthCheckInstance());
    }

    @Test
    public void testMarkUp() {
    }
}