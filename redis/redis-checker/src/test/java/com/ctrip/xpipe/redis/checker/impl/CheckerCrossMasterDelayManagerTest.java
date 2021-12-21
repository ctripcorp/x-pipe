package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class CheckerCrossMasterDelayManagerTest extends AbstractCheckerTest {

    private CheckerCrossMasterDelayManager manager;

    private RedisHealthCheckInstance instance;

    @Before
    public void setupCheckerCrossMasterDelayManager() throws Exception {
        manager = new CheckerCrossMasterDelayManager(FoundationService.DEFAULT.getDataCenter());
        instance = newRandomRedisHealthCheckInstance(6379);
    }

    @Test
    public void testGetAllCrossMasterDelays() {
        DelayActionContext context = new DelayActionContext(instance, 100L);
        RedisInstanceInfo info = instance.getCheckInfo();
        manager.onAction(context);
        Assert.assertEquals(Collections.singletonMap(
                new DcClusterShard(FoundationService.DEFAULT.getDataCenter(), info.getClusterId(), info.getShardId()),
                Collections.singletonMap(info.getDcId(), Pair.of(info.getHostPort(), 100L))),
                manager.getAllCrossMasterDelays());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUpdateCrossMasterDelays() {
        RedisInstanceInfo info = instance.getCheckInfo();
        manager.updateCrossMasterDelays(Collections.singletonMap(
                new DcClusterShard(FoundationService.DEFAULT.getDataCenter(), info.getClusterId(), info.getShardId()),
                Collections.singletonMap(info.getDcId(), Pair.of(info.getHostPort(), 100L))));
    }

}
