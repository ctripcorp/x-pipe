package com.ctrip.xpipe.redis.checker.healthcheck.factory;

import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class HealthCheckEndpointFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private HealthCheckEndpointFactory factory;

    @Test
    public void testHealthCheckEndpointFactoryTest() {
        Assert.assertNotNull(factory);
        Assert.assertTrue(factory instanceof DefaultHealthCheckEndpointFactory);
    }

}