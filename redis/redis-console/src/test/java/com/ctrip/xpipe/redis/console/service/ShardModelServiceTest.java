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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class ShardModelServiceTest extends ShardModelServiceImpl{

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
    @Before
    public void initMockData() {
        ShardTbl shardTbl = new ShardTbl();
        shardTbl.setShardName(shardName);
        shardModel.setShardTbl(shardTbl);
        List<RedisTbl> newKeepers = new ArrayList<>();
        newKeepers.add(new RedisTbl().setRedisIp("ip1").setRedisPort(6380));
        newKeepers.add(new RedisTbl().setRedisIp("ip2").setRedisPort(6381));
        when(keeperAdvancedService.getNewKeepers(dcName, clusterName, shardModel, srcIp, targetIp)).thenReturn(newKeepers);
        shardModelService.setKeyedObjectPool(new XpipeNettyClientKeyedObjectPool());
    }

    @Test
    public void testMigrateAutoBalanceKeepers() throws ExecutionException, InterruptedException {
        ScheduledThreadPoolExecutor executor = Mockito.mock(ScheduledThreadPoolExecutor.class);
        ScheduledFuture future = Mockito.mock(ScheduledFuture.class);
        try {
            shardModelService.migrateActiveKeeper(dcName, clusterName, shardModel, srcIp, targetIp);
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
        shardModelService.switchActiveKeeper("ip1","ip2",shardModel);
        List<RedisTbl> keepers = new ArrayList<>();
        keepers.add(new RedisTbl().setRedisIp("ip1").setRedisPort(6380));
        keepers.add(new RedisTbl().setRedisIp("ip2").setRedisPort(6381));
        shardModel.setKeepers(keepers);
        Assert.assertFalse(shardModelService.switchActiveKeeper("ip1","ip3",shardModel));
        Assert.assertTrue(shardModelService.switchActiveKeeper("ip1","ip2",shardModel));
    }

}
