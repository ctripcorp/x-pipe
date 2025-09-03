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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

public class PingActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private PingActionFactory pingActionFactory;

    private static final Logger logger = LoggerFactory.getLogger(PingActionFactoryTest.class);

    @Test
    public void testCreate() throws Exception {
        RedisHealthCheckInstance biInstance = newRandomRedisHealthCheckInstance("jq", ClusterType.BI_DIRECTION, 6379);
        PingAction biPingAction = pingActionFactory.create(biInstance);
        Assert.assertEquals(1, biPingAction.getControllers().size());
        Assert.assertEquals(3, biPingAction.getListeners().size());
        RedisHealthCheckInstance oneWayInstance = newRandomRedisHealthCheckInstance("jq", ClusterType.ONE_WAY, 6379);
        PingAction oneWayPingAction = pingActionFactory.create(oneWayInstance);
        Assert.assertEquals(1, oneWayPingAction.getControllers().size());
        Assert.assertEquals(0, oneWayPingAction.getListeners().size());

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
            logger.info("[testGetAndRemove4OneWay] add success");
        });
        pingAction.getListeners().forEach(listener -> {
            listener.stopWatch(pingAction);
        });
        logger.info("[testGetAndRemove4OneWay] remove success");
        collectors.forEach(collector -> {
            if (collector instanceof DefaultDelayPingActionCollector) {
                Assert.assertNull(((DefaultDelayPingActionCollector) collector).getHealthStatus4Test(instance));
                logger.info("[testGetAndRemove4OneWay] get fail");
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
            logger.info("[testGetAndRemove4Bi] add success");
        });
        pingAction.getListeners().forEach(listener -> {
            listener.stopWatch(pingAction);
        });
        logger.info("[testGetAndRemove4Bi] remove success");
        collectors.forEach(collector -> {
            if (collector instanceof CRDTDelayPingActionCollector) {
                Assert.assertNull(((CRDTDelayPingActionCollector) collector).getHealthStatus4Test(instance));
                logger.info("[testGetAndRemove4Bi] get fail");
            }
        });
    }
}
