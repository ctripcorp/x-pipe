package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
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
    
//    @Autowired
//    private DefaultHealthCheckEndpointFactory healthCheckEndpointFactory;
//    
//    @Autowired
//    private MetaCache metaCache;
    
    @Before
    public void beforeRedisMasterCheckActionFactoryTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreate() throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta();
//        healthCheckEndpointFactory.setMetaCache(metaCache);
        RedisHealthCheckInstance instance = instanceManager.getOrCreate(redisMeta);
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        logger.info("[isMaster] {}", instance.getCheckInfo().isMaster());
        logger.info("[activeDc] {}", instance.getCheckInfo().getActiveDc());
        checkActionController(action, ClusterType.ONE_WAY);
        action.doTask();
        instanceManager.remove(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
    }

    @Test
    public void testCreateForBiDirectionCluster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", ClusterType.BI_DIRECTION, 6379);
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        checkActionController(action, ClusterType.BI_DIRECTION);
        action.doTask();
    }

    @Test
    public void testCreateForSingleDcCluster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("singleDc", ClusterType.SINGLE_DC, 6379);
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        checkActionController(action, ClusterType.SINGLE_DC);
        action.doTask();
    }

    @Test
    public void testCreateForLocalDcCluster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("localDc", ClusterType.LOCAL_DC, 6379);
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        checkActionController(action, ClusterType.LOCAL_DC);
        action.doTask();
    }

    @Test
    public void testCreateForCrossDcCluster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("crossDc", ClusterType.CROSS_DC, 6379);
        RedisMasterCheckAction action = (RedisMasterCheckAction) factory.create(instance);
        checkActionController(action, ClusterType.CROSS_DC);
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
        } else if (clusterType.equals(ClusterType.BI_DIRECTION)) {
            Assert.assertTrue(controllers.stream().allMatch(controller -> controller instanceof BiDirectionSupport));
        } else if (clusterType.equals(ClusterType.SINGLE_DC)) {
            Assert.assertTrue(controllers.stream().allMatch(controller -> controller instanceof SingleDcSupport));
        } else if (clusterType.equals(ClusterType.LOCAL_DC)) {
            Assert.assertTrue(controllers.stream().allMatch(controller -> controller instanceof LocalDcSupport));
        } else if (clusterType.equals(ClusterType.CROSS_DC)) {
            Assert.assertTrue(controllers.stream().allMatch(controller -> controller instanceof CrossDcSupport));
        }
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(RedisMasterCheckAction.class, factory.support());
    }
}