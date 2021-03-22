package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Field;
import java.util.List;


/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class RedisMasterCheckActionFactoryTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private RedisMasterCheckActionFactory factory;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Before
    public void beforeRedisMasterCheckActionFactoryTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreate() throws Exception {
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(newRandomFakeRedisMeta());
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        logger.info("[isMaster] {}", instance.getCheckInfo().isMaster());
        logger.info("[activeDc] {}", instance.getCheckInfo().getActiveDc());
        checkActionController(action, ClusterType.ONE_WAY);
        action.doTask();
    }

    @Test
    public void testCreateForBiDirectionCluster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", ClusterType.BI_DIRECTION, 6379);
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        checkActionController(action, ClusterType.BI_DIRECTION);
        action.doTask();
    }

    private void checkActionController(RedisMasterCheckAction action, ClusterType clusterType) throws Exception {
        Field controllersField = AbstractHealthCheckAction.class.getDeclaredField("controllers");
        controllersField.setAccessible(true);
        List<HealthCheckActionController> controllers = (List<HealthCheckActionController>) controllersField.get(action);
        logger.info("[checkActionController] controllers: {}", controllers);
        Assert.assertTrue(controllers.size() > 0);
        if (clusterType.equals(ClusterType.ONE_WAY)) {
            Assert.assertTrue(controllers.stream().allMatch(controller -> controller instanceof OneWaySupport));
        } else {
            Assert.assertTrue(controllers.stream().allMatch(controller -> controller instanceof BiDirectionSupport));
        }
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(RedisMasterCheckAction.class, factory.support());
    }
}