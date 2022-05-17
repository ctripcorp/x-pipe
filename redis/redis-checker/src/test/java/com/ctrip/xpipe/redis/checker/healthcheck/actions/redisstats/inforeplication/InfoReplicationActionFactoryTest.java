package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class InfoReplicationActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    public InfoReplicationActionFactory factory;

    @Test
    public void testCreate() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, 6379);
        InfoReplicationAction action = factory.create(instance);
        action.doStart();

        Assert.assertTrue(factory.support().isInstance(action));
        Assert.assertEquals(2, action.getListeners().size());
        Assert.assertTrue(action.getListeners().stream().allMatch(listener -> listener instanceof OneWaySupport));
    }
}
