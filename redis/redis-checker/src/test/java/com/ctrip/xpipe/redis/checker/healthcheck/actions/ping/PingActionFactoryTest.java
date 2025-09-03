package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CRDTDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

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

    @Test
    @SuppressWarnings("unchecked")
    public void testGetAndRemove4OneWay() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("jq", ClusterType.ONE_WAY, 6379);
        instance.getCheckInfo().setActiveDc("jq");
        PingAction pingAction = pingActionFactory.create(instance);
        Assert.assertEquals(1, pingAction.getControllers().size());
        Assert.assertEquals(3, pingAction.getListeners().size());
        List<DelayPingActionCollector> collectors = pingActionFactory.getDelayPingCollectorsByClusterType().get(instance.getCheckInfo().getClusterType());
        Assert.assertNotEquals(0, collectors.size());
        collectors.forEach(collector -> {
            Map<RedisHealthCheckInstance, HealthStatus> allStatus = collector.getAllInstancesHealthStatus();
            Assert.assertNotEquals(0, allStatus.size());
            Assert.assertTrue(allStatus.containsKey(instance));
            System.out.println("[testGetAndRemove4OneWay]add success");
        });
        pingAction.getListeners().forEach(listener -> {
            listener.stopWatch(pingAction);
        });
        System.out.println("[testGetAndRemove4OneWay]remove success");
        collectors.forEach(collector -> {
            if (collector instanceof DefaultDelayPingActionCollector) {
                Assert.assertNull(((DefaultDelayPingActionCollector) collector).getHealthStatus4Test(instance));
                System.out.println("[testGetAndRemove4OneWay]get fail");
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetAndRemove4Bi() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("jq", ClusterType.BI_DIRECTION, 6379);
        PingAction pingAction = pingActionFactory.create(instance);
        Assert.assertEquals(1, pingAction.getControllers().size());
        Assert.assertEquals(3, pingAction.getListeners().size());
        List<DelayPingActionCollector> collectors = pingActionFactory.getDelayPingCollectorsByClusterType().get(instance.getCheckInfo().getClusterType());
        Assert.assertNotEquals(0, collectors.size());
        collectors.forEach(collector -> {
            Map<RedisHealthCheckInstance, HealthStatus> allStatus = collector.getAllInstancesHealthStatus();
            Assert.assertNotEquals(0, allStatus.size());
            Assert.assertTrue(allStatus.containsKey(instance));
            System.out.println("[testGetAndRemove4Bi]add success");
        });
        pingAction.getListeners().forEach(listener -> {
            listener.stopWatch(pingAction);
        });
        System.out.println("[testGetAndRemove4Bi]remove success");
        collectors.forEach(collector -> {
            if (collector instanceof CRDTDelayPingActionCollector) {
                Assert.assertNull(((CRDTDelayPingActionCollector) collector).getHealthStatus4Test(instance));
                System.out.println("[testGetAndRemove4Bi]get fail");
            }
        });
    }
}
