package com.ctrip.xpipe.redis.proxy.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class DefaultProxyConfigTest {

    ProxyConfig config = new DefaultProxyConfig();

    @Test
    public void getTrafficReportIntervalMillis() {
        Assert.assertEquals(30000, config.getTrafficReportIntervalMillis());
    }

    @Test
    public void endpointHealthCheckIntervalSec() {
        Assert.assertEquals(60, config.endpointHealthCheckIntervalSec());
    }

    @Test
    public void socketStatsCheckInterval() {
        Assert.assertEquals(1000, config.socketStatsCheckInterval());
    }

}