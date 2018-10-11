package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class RedisMasterCheckActionFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RedisMasterCheckActionFactory factory;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Before
    public void beforeRedisMasterCheckActionFactoryTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreate() {
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(newRandomFakeRedisMeta());
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        logger.info("[isMaster] {}", instance.getRedisInstanceInfo().isMaster());
        logger.info("[activeDc] {}", instance.getRedisInstanceInfo().getActiveDc());
        action.doTask();
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(RedisMasterCheckAction.class, factory.support());
    }
}