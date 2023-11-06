package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisUsedMemoryCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yu
 * <p>
 * 2023/10/18
 */
@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class KeeperContainerInfoReporterTest {

    @InjectMocks
    KeeperContainerInfoReporter keeperContainerInfoReporter;

    @Mock
    RedisUsedMemoryCollector redisUsedMemoryCollector;

    @Mock
    KeeperFlowCollector keeperFlowCollector;

    @Mock
    CheckerConsoleService checkerConsoleService;

    @Mock
    CheckerConfig config;

    @Captor
    ArgumentCaptor<List<KeeperContainerUsedInfoModel>> resultCaptor;

    @Before
    public void befor() {
        DcClusterShard dcClusterShard1 = new DcClusterShard("jq", "cluster1", "shard1");
        DcClusterShard dcClusterShard2 = new DcClusterShard("jq", "cluster1", "shard2");
        DcClusterShard dcClusterShard3 = new DcClusterShard("jq", "cluster2", "shard1");
        DcClusterShard dcClusterShard4 = new DcClusterShard("jq", "cluster2", "shard2");
        DcClusterShard dcClusterShard5 = new DcClusterShard("jq", "cluster3", "shard1");
        DcClusterShard dcClusterShard6 = new DcClusterShard("jq", "cluster3", "shard2");
        DcClusterShard dcClusterShard7 = new DcClusterShard("jq", "cluster3", "shard3");

        ConcurrentMap<String, Map<DcClusterShard, Long>> keeperFlowMap = Maps.newConcurrentMap();
        Map<DcClusterShard, Long> map1 = new HashMap<>();
        map1.put(dcClusterShard1, 2L);
        map1.put(dcClusterShard4, 2L);
        map1.put(dcClusterShard5, 2L);
        keeperFlowMap.put("127.0.0.1", map1);

        Map<DcClusterShard, Long> map2 = new HashMap<>();
        map2.put(dcClusterShard2, 2L);
        map2.put(dcClusterShard6, 2L);
        keeperFlowMap.put("127.0.0.2", map2);

        Map<DcClusterShard, Long> map3 = new HashMap<>();
        map3.put(dcClusterShard3, 2L);
        map3.put(dcClusterShard7, 2L);
        keeperFlowMap.put("127.0.0.3", map3);

        Mockito.when(keeperFlowCollector.getHostPort2InputFlow()).thenReturn(keeperFlowMap);

        ConcurrentMap<DcClusterShard, Long> redisUsedMemoryMap = Maps.newConcurrentMap();
        redisUsedMemoryMap.put(dcClusterShard1, 1L);
        redisUsedMemoryMap.put(dcClusterShard2, 2L);
        redisUsedMemoryMap.put(dcClusterShard3, 3L);
        redisUsedMemoryMap.put(dcClusterShard4, 4L);
        redisUsedMemoryMap.put(dcClusterShard6, 6L);
        redisUsedMemoryMap.put(dcClusterShard7, 7L);

        Mockito.when(redisUsedMemoryCollector.getDcClusterShardUsedMemory()).thenReturn(redisUsedMemoryMap);
        Mockito.when(config.getConsoleAddress()).thenReturn("127.0.0.1");
        Mockito.when(config.getClustersPartIndex()).thenReturn(0);

    }

    @Test
    public void testReportKeeperContainerInfo() {
        keeperContainerInfoReporter.reportKeeperContainerInfo();
        Mockito.verify(checkerConsoleService, Mockito.times(1))
                .reportKeeperContainerInfo(Mockito.anyString(), resultCaptor.capture(), Mockito.anyInt());

        Assert.assertEquals(3, resultCaptor.getValue().size());
    }
}
