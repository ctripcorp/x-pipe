package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class CheckerClusterHealthManagerTest extends AbstractCheckerTest {

    private CheckerClusterHealthManager manager;

    private RedisHealthCheckInstance instance;

    @Before
    public void setupCheckerClusterHealthManagerTest() throws Exception {
        manager = new CheckerClusterHealthManager(executors);
        instance = newRandomRedisHealthCheckInstance(6379);
    }

    @Test
    public void testMakeRedisDown() {
        manager.healthCheckMasterDown(instance);
        RedisInstanceInfo info = instance.getCheckInfo();
        Assert.assertEquals(Collections.singletonMap(info.getClusterId(), Collections.singleton(info.getShardId())),
                manager.getAllClusterWarningShards());
    }

    @Test
    public void testMakeRedisUp() {
        manager.healthCheckMasterUp(instance);
        RedisInstanceInfo info = instance.getCheckInfo();
        Assert.assertEquals(Collections.singletonMap(info.getClusterId(), Collections.emptySet()),
                manager.getAllClusterWarningShards());
    }

    @Test
    public void testRedisRecover() {
        RedisInstanceInfo info = instance.getCheckInfo();
        manager.healthCheckMasterDown(instance);
        manager.healthCheckMasterUp(instance);
        Assert.assertEquals(Collections.singletonMap(info.getClusterId(), Collections.emptySet()),
                manager.getAllClusterWarningShards());
    }

}
