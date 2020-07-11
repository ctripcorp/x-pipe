package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.class)
public class CurrentDcSentinelCheckControllerTest extends AbstractConsoleTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private RedisInstanceInfo info;

    @InjectMocks
    private CurrentDcSentinelCheckController controller;

    private String dcId = "jq", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupCurrentDcSentinelCheckControllerTest() {
        dcId = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta(2));
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(info);
        Mockito.when(info.getDcId()).thenReturn(dcId);
        Mockito.when(info.getClusterId()).thenReturn(clusterId);
        Mockito.when(info.getShardId()).thenReturn(shardId);
        Mockito.when(info.isMaster()).thenReturn(false);
    }

    @Test
    public void testCheckLocalDcSlave() {
        Assert.assertTrue(controller.shouldCheck(instance));

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta(1));
        Assert.assertTrue(controller.shouldCheck(instance));
    }

    @Test
    public void testCheckLocalDcMaster() {
        Mockito.when(info.isMaster()).thenReturn(true);
        Assert.assertFalse(controller.shouldCheck(instance));

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta(1));
        Assert.assertTrue(controller.shouldCheck(instance));
    }

    @Test
    public void testCheckOtherDcRedis() {
        Mockito.when(info.getDcId()).thenReturn("remote-dc");
        Assert.assertFalse(controller.shouldCheck(instance));
    }

    private XpipeMeta mockXpipeMeta(int redisCnt) {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta(dcId);
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        xpipeMeta.addDc(dcMeta.addCluster(clusterMeta.addShard(shardMeta)));
        IntStream.range(0, redisCnt).forEach(i -> shardMeta.addRedis(new RedisMeta()));
        return xpipeMeta;
    }

}
