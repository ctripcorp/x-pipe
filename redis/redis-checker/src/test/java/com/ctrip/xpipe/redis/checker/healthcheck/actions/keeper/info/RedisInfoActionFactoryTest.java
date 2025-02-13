package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by yu
 * 2023/8/29
 */
public class RedisInfoActionFactoryTest extends AbstractCheckerIntegrationTest {
//    @Autowired
//    RedisInfoActionFactory factory;

//    @Test
//    public void testCreate() throws Exception {
//        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, 6379);
//        RedisInfoAction action =  factory.create(instance);
//        action.doStart();
//
//        Assert.assertTrue(factory.support().isInstance(action));
//        Assert.assertEquals(1, action.getListeners().size());
//        Assert.assertTrue(action.getListeners().stream().allMatch(listener -> listener instanceof KeeperSupport));
//    }
}
