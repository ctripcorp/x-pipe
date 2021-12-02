package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class CrdtInfoStatsActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private CrdtInfoStatsActionFactory factory;

    @Test
    public void testCreate() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        CrdtInfoStatsAction action = factory.create(instance);
        action.doStart();

        Assert.assertTrue(factory.support().isInstance(action));
        Assert.assertEquals(1, action.getControllers().size());
        Assert.assertTrue(((HealthCheckActionController)action.getControllers().get(0)).shouldCheck(action.getActionInstance()));
        Assert.assertTrue(action.getListeners().stream().allMatch(listener -> listener instanceof BiDirectionSupport));
    }

}
