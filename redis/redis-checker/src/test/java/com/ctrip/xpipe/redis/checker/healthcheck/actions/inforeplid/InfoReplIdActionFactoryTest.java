package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultInfoReplIdPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

public class InfoReplIdActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private InfoReplIdActionFactory infoReplIdActionFactory;

    @Autowired
    private List<InfoReplIdPingActionCollector> collectors;

    public static final Logger logger = LoggerFactory.getLogger(InfoReplIdActionFactoryTest.class);

    @Test
    public void testGetAndRemove() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("jq", ClusterType.ONE_WAY, 6379);
        instance.getCheckInfo().setActiveDc("oy");
        InfoReplIdAction infoReplIdAction = infoReplIdActionFactory.create(instance);
        Assert.assertEquals(0, infoReplIdAction.getControllers().size());
        Assert.assertEquals(1, infoReplIdAction.getListeners().size());
        Assert.assertNotEquals(0, collectors.size());
        collectors.forEach(collector -> {
            Map<RedisHealthCheckInstance, HealthStatus> allStatus = collector.getAllInstancesHealthStatus();
            Assert.assertNotEquals(0, allStatus.size());
            Assert.assertTrue(allStatus.containsKey(instance));
            logger.info("[testGetAndRemove] add success");
        });
        infoReplIdAction.getListeners().forEach(listener -> {
            listener.stopWatch(infoReplIdAction);
        });
        logger.info("[testGetAndRemove] remove success");
        collectors.forEach(collector -> {
            if (collector instanceof DefaultInfoReplIdPingActionCollector) {
                Assert.assertNull(((DefaultInfoReplIdPingActionCollector) collector).getHealthStatus4Test(instance));
                logger.info("[testGetAndRemove]get fail");
            }
        });
    }
}
