package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class PingActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private PingActionFactory pingActionFactory;

    @Test
    public void testCreate() throws Exception {
        RedisHealthCheckInstance biInstance = newRandomRedisHealthCheckInstance("jq", ClusterType.BI_DIRECTION, 6379);
        PingAction biPingAction = pingActionFactory.create(biInstance);
        Assert.assertEquals(biPingAction.getControllers().size(), 1);
        Assert.assertEquals(biPingAction.getListeners().size(), 3);
        RedisHealthCheckInstance oneWayInstance = newRandomRedisHealthCheckInstance("jq", ClusterType.ONE_WAY, 6379);
        PingAction oneWayPingAction = pingActionFactory.create(oneWayInstance);
        Assert.assertEquals(oneWayPingAction.getControllers().size(), 1);
        Assert.assertEquals(oneWayPingAction.getListeners().size(), 0);

    }

}
