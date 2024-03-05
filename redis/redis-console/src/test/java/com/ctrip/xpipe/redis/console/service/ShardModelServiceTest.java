package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.model.impl.ShardModelServiceImpl.*;
import com.ctrip.xpipe.redis.console.service.model.impl.ShardModelServiceImpl;
import org.apache.logging.log4j.core.config.CronScheduledFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

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
    private RedisSessionManager redisSessionManager;

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
        RedisSession active = new RedisSession();
        RedisSession backUp = new RedisSession();
        when(redisSessionManager.findOrCreateSession(new HostPort("ip1", 6380))).thenReturn(active);
        when(redisSessionManager.findOrCreateSession(new HostPort("ip2", 6381))).thenReturn(backUp);
    }

    @Test
    public void testMigrateAutoBalanceKeepers() {
        shardModelService.migrateAutoBalanceKeepers(dcName, clusterName, shardModel, srcIp, targetIp);
    }

    @Test
    public void testFullSyncJudgeTask() {
//        when(redisSession.infoReplication(any())).thenReturn(new DefaultCommandFuture<>());
//        FullSyncJudgeTask task = new FullSyncJudgeTask(redisSession, redisSession, 1000, 1000, dcName, clusterName, shardModel);
//        task.setScheduledFuture(new CronScheduledFuture<Object>(null, null));
//        task.run();
    }

}
