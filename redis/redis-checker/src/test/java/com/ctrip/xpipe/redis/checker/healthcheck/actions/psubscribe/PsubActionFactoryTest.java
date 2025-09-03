package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.*;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

public class PsubActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private PsubActionFactory psubActionFactory;

    @Test
    @SuppressWarnings("unchecked")
    public void testGetAndRemove() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("jq", ClusterType.ONE_WAY, 6379);
        instance.getCheckInfo().setActiveDc("oy");
        PsubAction psubAction = psubActionFactory.create(instance);
        Assert.assertEquals(1, psubAction.getControllers().size());
        Assert.assertEquals(1, psubAction.getListeners().size());
        List<PsubPingActionCollector> collectors = psubActionFactory.getPsubPingActionCollectorsByClusterType().get(instance.getCheckInfo().getClusterType());
        Assert.assertNotEquals(0, collectors.size());
        collectors.forEach(collector -> {
            Map<RedisHealthCheckInstance, HealthStatus> allStatus = collector.getAllInstancesHealthStatus();
            Assert.assertNotEquals(0, allStatus.size());
            Assert.assertTrue(allStatus.containsKey(instance));
            System.out.println("[testGetAndRemove]add success");
        });
        psubAction.getListeners().forEach(listener -> {
            listener.stopWatch(psubAction);
        });
        System.out.println("[testGetAndRemove]remove success");
        collectors.forEach(collector -> {
            if (collector instanceof DefaultPsubPingActionCollector) {
                Assert.assertNull(((DefaultPsubPingActionCollector) collector).getHealthStatus4Test(instance));
                System.out.println("[testGetAndRemove]get fail");
            }
        });
    }
}
