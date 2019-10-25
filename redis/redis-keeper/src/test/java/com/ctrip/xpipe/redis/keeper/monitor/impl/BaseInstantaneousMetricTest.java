package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.monitor.BaseInstantaneousMetric;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class BaseInstantaneousMetricTest extends AbstractTest {

    private BaseInstantaneousMetric metric = new BaseInstantaneousMetric();

    private static final int FIXED_INPUT = 1000;

    @Test
    public void testGetInstantaneousMetric() {
        metric.trackInstantaneousMetric(FIXED_INPUT);
        Assert.assertEquals(FIXED_INPUT, metric.getInstantaneousMetric());
    }

    @Test
    public void testTrackInstantaneousMetric() {
        float kilo = 1024;
        Assert.assertEquals(0, metric.getInstantaneousMetric());
        for(int i = 0; i < 16; i++) {
            metric.trackInstantaneousMetric(FIXED_INPUT * (i + 1));
            sleep(1000);
        }
        logger.info("{}", metric.getInstantaneousMetric());
        logger.info("{}", strAndNum("key", ((float)metric.getInstantaneousMetric() / kilo)));
    }

    protected String strAndNum(String key, float val) {
        return String.format("%s:%f%s", key, val, RedisProtocol.CRLF);
    }
}