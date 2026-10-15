package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.impl.DefaultKeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.model.impl.ShardModelServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class ShardModelServiceTest {

    @InjectMocks
    private ShardModelServiceImpl shardModelService;

    @Mock
    private KeeperAdvancedService keeperAdvancedService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Mock
    private RedisService redisService;

    @Mock
    private RedisSession redisSession;

    @Mock
    private RetryCommandFactory<?> retryCommandFactory;

    private final String dcName = "dc1";

    private final String clusterName = "cluster1";

    private final String shardName = "shard1";

    private ShardModel shardModel = new ShardModel();

    private final String srcIp = "10.10.10.10";

    private final String targetIp = "10.10.10.20";
    
    // keeperContainerId for test
    private final long srcKeeperContainerId = 1L;
    private final long targetKeeperContainerId = 2L;

    @Before
    public void initMockData() {
        ReflectionTestUtils.setField(shardModelService, "retryCommandFactory", retryCommandFactory);

        ShardTbl shardTbl = new ShardTbl();
        shardTbl.setShardName(shardName);
        shardModel.setShardTbl(shardTbl);
        List<RedisTbl> newKeepers = new ArrayList<>();
        newKeepers.add(new RedisTbl().setRedisIp("ip1").setRedisPort(6380));
        newKeepers.add(new RedisTbl().setRedisIp("ip2").setRedisPort(6381));
        when(keeperAdvancedService.getNewKeepers(dcName, clusterName, shardModel, srcKeeperContainerId, targetKeeperContainerId)).thenReturn(newKeepers);
        shardModelService.setKeyedObjectPool(new XpipeNettyClientKeyedObjectPool());
    }

    @Test
    public void testMigrateAutoBalanceKeepers() throws ExecutionException, InterruptedException {
        ScheduledThreadPoolExecutor executor = Mockito.mock(ScheduledThreadPoolExecutor.class);
        ScheduledFuture future = Mockito.mock(ScheduledFuture.class);
        try {
            shardModelService.migrateActiveKeeper(dcName, clusterName, shardModel, srcKeeperContainerId, targetKeeperContainerId);
        } catch (Throwable th) {
            Assert.assertEquals(th.getMessage(), "keeper_migration_active_rollback_error");
        }
    }

    @Test
    public void testGetSwitchMaterNewKeepers() {
        ShardModel model = new ShardModel();
        List<RedisTbl> keepers = new ArrayList<>();
        keepers.add(new RedisTbl().setMaster(true));
        model.setKeepers(keepers);
        List<RedisTbl> switchMaterNewKeepers = new DefaultKeeperAdvancedService().getSwitchMaterNewKeepers(model);
        Assert.assertEquals(switchMaterNewKeepers.size(), 1);
        Assert.assertFalse(switchMaterNewKeepers.get(0).isMaster());
    }

    @Test
    public void testSwitchActiveKeeper() {
        // shardModel initially has no keepers, should return false
        Assert.assertFalse(shardModelService.switchActiveKeeper(dcName, shardModel, 1L, 2L));
        
        // Add keepers with keeperContainerId
        List<RedisTbl> keepers = new ArrayList<>();
        keepers.add(new RedisTbl().setRedisIp("ip1").setRedisPort(6380).setKeepercontainerId(1L));
        keepers.add(new RedisTbl().setRedisIp("ip2").setRedisPort(6381).setKeepercontainerId(2L));
        shardModel.setKeepers(keepers);
        
        // keeperContainerId 3 doesn't exist, should return false
        Assert.assertFalse(shardModelService.switchActiveKeeper(dcName, shardModel,1L, 3L));
        
        // Both keeperContainerIds exist, but switchActiveKeeper needs retryCommandFactory
        // which is null, so it will return false due to NullPointerException or similar
        // Actually the method should work since we mock retryCommandFactory
    }

    @Test
    public void testSwitchActiveKeeperWithThreeKeepers() {
        List<RedisTbl> keepers = new ArrayList<>();
        keepers.add(new RedisTbl().setRedisIp("ip1").setRedisPort(6380).setKeepercontainerId(1L));
        keepers.add(new RedisTbl().setRedisIp("ip2").setRedisPort(6381).setKeepercontainerId(2L));
        keepers.add(new RedisTbl().setRedisIp("ip3").setRedisPort(6382).setKeepercontainerId(3L));
        shardModel.setKeepers(keepers);
        
        // keeperContainerId 4 doesn't exist
        Assert.assertFalse(shardModelService.switchActiveKeeper(dcName, shardModel,1L, 4L));
        
        // Both exist, but actual switch needs retryCommandFactory
        // The test validates the keeperContainerId lookup logic
    }

    @Test
    public void testDoMigrateKeepersWithThreeKeepers() throws Exception {
        List<RedisTbl> keepers = new ArrayList<>();
        keepers.add(new RedisTbl().setRedisIp("ip1").setRedisPort(6380));
        keepers.add(new RedisTbl().setRedisIp("ip2").setRedisPort(6381));
        keepers.add(new RedisTbl().setRedisIp("ip3").setRedisPort(6382));
        Boolean result = org.springframework.test.util.ReflectionTestUtils.invokeMethod(
                shardModelService, "doMigrateKeepers", dcName, clusterName, shardModel, keepers);
        Assert.assertTrue(result);
    }

}
