package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.impl.DefaultKeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.model.impl.ShardModelServiceImpl.*;
import com.ctrip.xpipe.redis.console.service.model.impl.ShardModelServiceImpl;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import org.apache.logging.log4j.core.config.CronScheduledFuture;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class ShardModelServiceTest extends ShardModelServiceImpl{

    @InjectMocks
    private ShardModelServiceImpl shardModelService;

    @Mock
    private KeeperAdvancedService keeperAdvancedService;

    @Mock
    private RedisService redisService;

    @Mock
    private RedisSession redisSession;

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
        when(keeperAdvancedService.getNewKeepers(dcName, clusterName, shardModel, srcIp, targetIp, true)).thenReturn(newKeepers);
        shardModelService.setKeyedObjectPool(new XpipeNettyClientKeyedObjectPool());
    }

    @Test
    public void testMigrateAutoBalanceKeepers() {
        shardModelService.migrateAutoBalanceKeepers(dcName, clusterName, shardModel, srcIp, targetIp);
    }

    @Test
    public void testFullSyncJudgeTask() {
        InfoCommand infoCommand1 = shardModelService.generteInfoCommand(new DefaultEndPoint("1", 6380));
        InfoCommand infoCommand2 = shardModelService.generteInfoCommand(new DefaultEndPoint("2", 6380));
        FullSyncJudgeTask task = new FullSyncJudgeTask("1", "2", infoCommand1, infoCommand2, 1000, 1000, dcName, clusterName, shardModel);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ScheduledFuture<?> scheduledFuture = executor.scheduleWithFixedDelay(task, 1000, 1000, TimeUnit.MILLISECONDS);
        task.setScheduledFuture(scheduledFuture);
        task.run();
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

}
