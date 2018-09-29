package com.ctrip.xpipe.redis.console.healthcheck.redismaster;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

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

    @Mock
    private MetaCache metaCache;

    @Before
    public void beforeRedisMasterCheckActionFactoryTest() {
        MockitoAnnotations.initMocks(this);
        factory.setMetaCache(metaCache);
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(false);
    }

    @Test
    public void testCreate() {
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(newRandomFakeRedisMeta());
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        logger.info("[isMaster] {}", instance.getRedisInstanceInfo().isMaster());
        action.doScheduledTask();
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(RedisMasterCheckAction.class, factory.support());
    }
}