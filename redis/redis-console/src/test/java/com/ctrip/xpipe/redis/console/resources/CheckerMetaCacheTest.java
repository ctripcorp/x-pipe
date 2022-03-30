package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CheckerMetaCacheTest {

    @InjectMocks
    private CheckerMetaCache checkerMetaCache;

    @Mock
    private CheckerConfig config;

    @Mock
    private CheckerConsoleService checkerConsoleService;

    @Mock
    private ScheduledExecutorService scheduled;

    @Mock
    private DynamicDelayPeriodTask metaLoadTask;


    @Test
    public void getMaxMasterCountDcTest() throws Exception {
        when(config.getConsoleAddress()).thenReturn("");
        when(config.getClustersPartIndex()).thenReturn(1);
        XpipeMeta xpipeMeta = new XpipeMeta();

//          single dc trocks
        ClusterMeta oyCluster = new ClusterMeta().setId("cluster").addShard(
                new ShardMeta().setId("shard1").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));

        DcMeta oyDcMeta = new DcMeta("oy");
        oyDcMeta.addCluster(oyCluster);
        xpipeMeta.addDc(oyDcMeta);
        when(checkerConsoleService.getXpipeMeta(anyString(), anyInt())).thenReturn(xpipeMeta);
        checkerMetaCache.loadMeta();

        Assert.assertEquals("oy", checkerMetaCache.getMaxMasterCountDc("cluster", new HashSet<>()).getKey());

//        multi dc trocks，sinle shard，oy master
        ClusterMeta jqCluster = new ClusterMeta().setId("cluster").addShard(
                new ShardMeta().setId("shard1").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));

        DcMeta jqDcMeta = new DcMeta("jq");
        jqDcMeta.addCluster(jqCluster);
        xpipeMeta.addDc(jqDcMeta);
        checkerMetaCache.loadMeta();

        Assert.assertEquals("oy", checkerMetaCache.getMaxMasterCountDc("cluster", new HashSet<>()).getKey());

//        multi dc trocks，multi shards，oy master
        oyCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        checkerMetaCache.loadMeta();

        Assert.assertEquals("oy", checkerMetaCache.getMaxMasterCountDc("cluster", new HashSet<>()).getKey());

//        multi dc trocks，multi shards，one oy master，one jq master
        oyCluster.removeShard("shard2");
        jqCluster.removeShard("shard2");
        oyCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        checkerMetaCache.loadMeta();

        Assert.assertEquals("jq", checkerMetaCache.getMaxMasterCountDc("cluster", new HashSet<>()).getKey());

//        multidc trocks，multi shards，one oy master，two jq masters
        oyCluster.addShard(new ShardMeta().setId("shard3").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard3").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        checkerMetaCache.loadMeta();
        Assert.assertEquals("jq", checkerMetaCache.getMaxMasterCountDc("cluster", new HashSet<>()).getKey());

    }
}
