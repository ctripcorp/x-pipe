package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 17, 2018
 */
public class DefaultRedisHealthCheckInstanceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RedisHealthCheckInstanceFactory factory;

    @Test
    public void testDoStop() throws Exception {
        RedisHealthCheckInstance instance = factory.create(newRandomFakeRedisMeta());
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            logger.info("[action] {}", action);
            for(Object listener : ((AbstractHealthCheckAction) action).getListeners()) {
                logger.info("   [listener] {}", listener);
            }
        }
        instance.stop();
    }
}