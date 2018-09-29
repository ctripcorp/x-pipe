package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
public class DefaultCrossDcLeaderAwareHealthCheckManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultCrossDcLeaderAwareHealthCheckManager manager;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    private RedisHealthCheckInstance instance;

    @Before
    public void beforeDefaultCrossDcLeaderAwareHealthCheckManagerTest() {
        instance = instanceManager.getOrCreate(newRandomFakeRedisMeta());
        instance.getHealthCheckActions().clear();
    }

    @Test
    public void testRegisterTo() {
        manager.registerTo(instance);
        sleep(10);
        instance.getHealthCheckActions().keySet().forEach((clazz)-> {
            logger.info("[listener] {}", clazz.getSimpleName());
        });
        Assert.assertFalse(instance.getHealthCheckActions().isEmpty());
    }

    @Test
    public void testRemoveFrom() {
        manager.registerTo(instance);
        sleep(10);
        Assert.assertFalse(instance.getHealthCheckActions().isEmpty());
        manager.removeFrom(instance);
        sleep(10);
        Assert.assertTrue(instance.getHealthCheckActions().isEmpty());
    }

    @Test
    public void testIsCrossDcLeader() {
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(newRandomFakeRedisMeta());
        manager.isCrossDcLeader();
        sleep(10);
        instance.getHealthCheckActions().keySet().forEach((clazz)-> {
            logger.info("[listener] {}", clazz.getSimpleName());
        });
        Assert.assertFalse(instance.getHealthCheckActions().isEmpty());
        // default delay action & ping action
        Assert.assertTrue(instance.getHealthCheckActions().size() > 2);
    }

    @Test
    public void testNotCrossDcLeader() {
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(newRandomFakeRedisMeta());
        manager.isCrossDcLeader();
        sleep(10);
        // default delay action & ping action
        Assert.assertTrue(instance.getHealthCheckActions().size() > 2);
        int beforeSize = instance.getHealthCheckActions().size();

        manager.notCrossDcLeader();
        sleep(10); // wait for executors execute
        Assert.assertTrue(instance.getHealthCheckActions().size() < beforeSize);
    }

    @Test
    public void testSafeLoop() {
        int N = 10;
        for(int i = 0; i < N; i++) {
            instanceManager.getOrCreate(newRandomFakeRedisMeta().setPort(randomPort()));
        }
        manager.new SafeLoop<RedisHealthCheckInstance>(instanceManager.getAllRedisInstance()) {
            @Override
            void doRun0(RedisHealthCheckInstance instance) {
                throw new XpipeRuntimeException("expected exception");
            }
        }.run();
    }
}