package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class HealthCheckEndpointFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private HealthCheckEndpointFactory factory;

    @Test
    public void testHealthCheckEndpointFactoryTest() {
        Assert.assertNotNull(factory);
        Assert.assertTrue(factory instanceof DefaultHealthCheckEndpointFactory);
    }

}