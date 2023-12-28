package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.KeeperContainerService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisUsedMemoryCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.core.entity.DiskSpaceUsageInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
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

import static org.mockito.ArgumentMatchers.anyString;

/**
 * @author yu
 * <p>
 * 2023/10/18
 */
@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class KeeperUsedInfoReporterTest {

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

    @Mock
    KeeperContainerService keeperContainerService;

    @Captor
    ArgumentCaptor<List<KeeperContainerUsedInfoModel>> resultCaptor;

    @Before
    public void befor() {
        DcClusterShardActive dcClusterShard1 = new DcClusterShardActive("jq", "cluster1", "shard1", false);
        DcClusterShardActive dcClusterShard2 = new DcClusterShardActive("jq", "cluster1", "shard2", true);
        DcClusterShardActive dcClusterShard3 = new DcClusterShardActive("jq", "cluster2", "shard1", true);
        DcClusterShardActive dcClusterShard4 = new DcClusterShardActive("jq", "cluster2", "shard2", true);
        DcClusterShardActive dcClusterShard5 = new DcClusterShardActive("jq", "cluster3", "shard1", true);
        DcClusterShardActive dcClusterShard6 = new DcClusterShardActive("jq", "cluster3", "shard2", true);
        DcClusterShardActive dcClusterShard7 = new DcClusterShardActive("jq", "cluster3", "shard3", true);

        ConcurrentMap<String, Map<DcClusterShardActive, Long>> keeperFlowMap = Maps.newConcurrentMap();
        Map<DcClusterShardActive, Long> map1 = new HashMap<>();
        map1.put(dcClusterShard1, 2L);
        map1.put(dcClusterShard4, 2L);
        map1.put(dcClusterShard5, 2L);
        keeperFlowMap.put("127.0.0.1", map1);

        Map<DcClusterShardActive, Long> map2 = new HashMap<>();
        map2.put(dcClusterShard2, 2L);
        map2.put(dcClusterShard6, 2L);
        keeperFlowMap.put("127.0.0.2", map2);

        Map<DcClusterShardActive, Long> map3 = new HashMap<>();
        map3.put(dcClusterShard3, 2L);
        map3.put(dcClusterShard7, 2L);
        keeperFlowMap.put("127.0.0.3", map3);

        Mockito.when(keeperFlowCollector.getHostPort2InputFlow()).thenReturn(keeperFlowMap);

        ConcurrentMap<DcClusterShard, Long> redisUsedMemoryMap = Maps.newConcurrentMap();
        DcClusterShard dcClusterShard8 = new DcClusterShard("jq", "cluster1", "shard1");
        DcClusterShard dcClusterShard9 = new DcClusterShard("jq", "cluster1", "shard2");
        DcClusterShard dcClusterShard10 = new DcClusterShard("jq", "cluster2", "shard1");
        DcClusterShard dcClusterShard11 = new DcClusterShard("jq", "cluster2", "shard2");
        DcClusterShard dcClusterShard12 = new DcClusterShard("jq", "cluster3", "shard1");
        DcClusterShard dcClusterShard13 = new DcClusterShard("jq", "cluster3", "shard2");
        redisUsedMemoryMap.put(dcClusterShard8, 1L);
        redisUsedMemoryMap.put(dcClusterShard9, 2L);
        redisUsedMemoryMap.put(dcClusterShard10, 3L);
        redisUsedMemoryMap.put(dcClusterShard11, 4L);
        redisUsedMemoryMap.put(dcClusterShard12, 6L);
        redisUsedMemoryMap.put(dcClusterShard13, 7L);

        Mockito.when(redisUsedMemoryCollector.getDcClusterShardUsedMemory()).thenReturn(redisUsedMemoryMap);
        Mockito.when(config.getConsoleAddress()).thenReturn("127.0.0.1");
        Mockito.when(config.getClustersPartIndex()).thenReturn(0);


        KeeperDiskInfo diskInfo = new KeeperDiskInfo();
        diskInfo.available = true;
        diskInfo.spaceUsageInfo = new DiskSpaceUsageInfo();
        diskInfo.spaceUsageInfo.size = 1024*1024*1024*1024L;
        diskInfo.spaceUsageInfo.use = 1024*1024*1024*100L;
        Mockito.when(keeperContainerService.getKeeperDiskInfo(anyString())).thenReturn(diskInfo);
    }

    @Test
    public void testReportKeeperContainerInfo() {
        keeperContainerInfoReporter.
                reportKeeperContainerInfo();
        Mockito.verify(checkerConsoleService, Mockito.times(1))
                .reportKeeperContainerInfo(anyString(), resultCaptor.capture(), Mockito.anyInt());

        Assert.assertEquals(3, resultCaptor.getValue().size());
        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : resultCaptor.getValue()) {
            if ("127.0.0.1".equals(keeperContainerUsedInfoModel.getKeeperIp())) {
                long activeInputFlow = keeperContainerUsedInfoModel.getActiveInputFlow();
                long totalInputFlow = keeperContainerUsedInfoModel.getTotalInputFlow();
                Assert.assertEquals(4, activeInputFlow);
                Assert.assertEquals(6 ,totalInputFlow);
            }
        }
    }

    @Test
    public void DcClusterShardActive(){
        DcClusterShardActive active = new DcClusterShardActive();
        active.setActive(true);
        active.setDcId("dc");
        active.setClusterId("cluster");
        active.setShardId("shard");
        active.setPort(123);
        Assert.assertEquals(active.toString(), "dc:cluster:shard:true:123");
        DcClusterShardActive active1 = new DcClusterShardActive(active.toString());
        Assert.assertEquals(active.toString(), active1.toString());
        Assert.assertEquals(active, active1);
        DcClusterShardActive active2 = new DcClusterShardActive("dc","cluster","shard", true, 123);
        Assert.assertEquals(active2.hashCode(), active1.hashCode());
    }
}
