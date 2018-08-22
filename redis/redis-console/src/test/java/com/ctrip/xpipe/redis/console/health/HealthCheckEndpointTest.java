package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Aug 21, 2018
 */
public class HealthCheckEndpointTest {

    private HealthCheckEndpoint endpoint;

    @Test
    public void testIsProxyEnabled() {
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379);
        endpoint = new DefaultHealthCheckEndpoint(redisMeta);
        Assert.assertFalse(endpoint.isProxyEnabled());

        endpoint = new DefaultProxyEnabledHealthCheckEndpoint(redisMeta, null);
        Assert.assertTrue(endpoint.isProxyEnabled());
    }

    @Test
    public void testTimeoutMilli() {
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379);
        endpoint = new DefaultHealthCheckEndpoint(redisMeta);
        Assert.assertEquals(1500, endpoint.getDelayCheckTimeoutMilli());

        endpoint = new DefaultProxyEnabledHealthCheckEndpoint(redisMeta, null);
        Assert.assertEquals(30 * 1000, endpoint.getDelayCheckTimeoutMilli());
    }
}