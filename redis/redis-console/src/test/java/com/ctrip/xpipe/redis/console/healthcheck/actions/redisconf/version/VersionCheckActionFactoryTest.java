package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.version;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.crossdc.CrossDcLeaderAwareHealthCheckAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public class VersionCheckActionFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private VersionCheckActionFactory factory;

    private RedisHealthCheckInstance instance;

    @Before
    public void testVersionCheckActionFactoryTest() throws Exception {
        instance = newRandomRedisHealthCheckInstance(randomPort());
        instance.register(factory.create(instance));
    }

    @Test
    public void testCreate() {
        Assert.assertTrue(instance.getHealthCheckActions().size() > 0);
        VersionCheckAction action = (VersionCheckAction) instance.getHealthCheckActions().get(0);
        Assert.assertTrue(action.getLifecycleState().isEmpty());
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(VersionCheckAction.class, factory.support());
    }

    @Test
    public void testDestroy() {
        HealthCheckAction target = null;
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            if(action.getClass().isAssignableFrom(factory.support())) {
                target = action;
                factory.destroy((CrossDcLeaderAwareHealthCheckAction) target);
            }
        }


        logger.info("{}", instance.getHealthCheckActions());
        instance.unregister(target);
        instance.unregister(target);
        Assert.assertTrue(instance.getHealthCheckActions().isEmpty());
    }
}