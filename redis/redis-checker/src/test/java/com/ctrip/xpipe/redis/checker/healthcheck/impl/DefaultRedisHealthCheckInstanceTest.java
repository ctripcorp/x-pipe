package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 17, 2018
 */
public class DefaultRedisHealthCheckInstanceTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private HealthCheckInstanceFactory factory;

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