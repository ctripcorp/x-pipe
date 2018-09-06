package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.healthcheck.HealthStatusManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultHealthCheckContextFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultHealthCheckRedisInstanceFactoryTest;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultHealthStatusManagerTest extends DefaultHealthCheckRedisInstanceFactoryTest {

    private RedisHealthCheckInstance instance;

    private DefaultHealthStatusManager healthStatusManager;

    @Before
    public void beforeDefaultHealthStatusManagerTest() {
        instance = factory.create(normalRedisMeta());
        healthStatusManager = (DefaultHealthStatusManager) instance.getHealthStatusManager();
    }

    @Test
    public void testDelayMarkDown() {
        healthStatusManager.markDown(instance, HealthStatusManager.MarkDownReason.LAG);
    }

    @Test
    public void testPingMarkDown() {
        healthStatusManager.markDown(instance, HealthStatusManager.MarkDownReason.PING_FAIL);
    }

    @Test
    public void testDelayMarkUp() {
    }
}