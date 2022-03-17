package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.Silent.class)
public class CurrentDcSentinelCheckControllerTest extends AbstractCheckerTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private RedisInstanceInfo info;

    @InjectMocks
    private CurrentDcSentinelHelloCheckController controller = new CurrentDcSentinelHelloCheckController();

    private String dcId = "jq", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupCurrentDcSentinelCheckControllerTest() {
        dcId = FoundationService.DEFAULT.getDataCenter();
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta(2));
        Mockito.when(metaCache.getRedisOfDcClusterShard(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Arrays.asList(new RedisMeta(), new RedisMeta()));
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(info.getDcId()).thenReturn(dcId);
        Mockito.when(info.getClusterId()).thenReturn(clusterId);
        Mockito.when(info.getShardId()).thenReturn(shardId);
        Mockito.when(info.isMaster()).thenReturn(false);
    }

    @Test
    public void testCheckLocalDcSlave() {
        Assert.assertTrue(controller.shouldCheck(instance));

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta(1));
        Mockito.when(metaCache.getRedisOfDcClusterShard(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Collections.singletonList(new RedisMeta()));
        Assert.assertTrue(controller.shouldCheck(instance));
    }

    @Test
    public void testCheckLocalDcMaster() {
        Mockito.when(info.isMaster()).thenReturn(true);
        Assert.assertFalse(controller.shouldCheck(instance));

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta(1));
        Mockito.when(metaCache.getRedisOfDcClusterShard(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Collections.singletonList(new RedisMeta()));
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
