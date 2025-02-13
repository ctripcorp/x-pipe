package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisMsgCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;

import java.util.ArrayList;
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
    RedisMsgReporter redisMsgReporter;

    @Mock
    RedisMsgCollector redisMsgCollector;

    @Mock
    KeeperFlowCollector keeperFlowCollector;

    @Mock
    CheckerConsoleService checkerConsoleService;

    @Mock
    CheckerConfig config;

    @Mock
    KeeperContainerCheckerService keeperContainerService;

    @Mock
    private MetaCache metaCache;

    @Captor
    ArgumentCaptor<Map<HostPort, RedisMsg>> resultCaptor;

    @Before
    public void befor() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta();
        xpipeMeta.getDcs().put("dc",dcMeta);
        dcMeta.setId(FoundationService.DEFAULT.getDataCenter());
        List<KeeperContainerMeta> keeperContainerMetas = new ArrayList<>();
        keeperContainerMetas.add(new KeeperContainerMeta().setIp("127.0.0.1"));
        keeperContainerMetas.add(new KeeperContainerMeta().setIp("127.0.0.2"));
        keeperContainerMetas.add(new KeeperContainerMeta().setIp("127.0.0.3"));
        dcMeta.getKeeperContainers().addAll(keeperContainerMetas);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        DcClusterShardKeeper dcClusterShard1 = new DcClusterShardKeeper("jq", "cluster1", "shard1", false);
        DcClusterShardKeeper dcClusterShard2 = new DcClusterShardKeeper("jq", "cluster1", "shard2", true);
        DcClusterShardKeeper dcClusterShard3 = new DcClusterShardKeeper("jq", "cluster2", "shard1", true);
        DcClusterShardKeeper dcClusterShard4 = new DcClusterShardKeeper("jq", "cluster2", "shard2", true);
        DcClusterShardKeeper dcClusterShard5 = new DcClusterShardKeeper("jq", "cluster3", "shard1", true);
        DcClusterShardKeeper dcClusterShard6 = new DcClusterShardKeeper("jq", "cluster3", "shard2", true);
        DcClusterShardKeeper dcClusterShard7 = new DcClusterShardKeeper("jq", "cluster3", "shard3", true);

        ConcurrentMap<String, Map<DcClusterShardKeeper, Long>> keeperFlowMap = Maps.newConcurrentMap();
        Map<DcClusterShardKeeper, Long> map1 = new HashMap<>();
        map1.put(dcClusterShard1, 2L);
        map1.put(dcClusterShard4, 2L);
        map1.put(dcClusterShard5, 2L);
        keeperFlowMap.put("127.0.0.1", map1);

        Map<DcClusterShardKeeper, Long> map2 = new HashMap<>();
        map2.put(dcClusterShard2, 2L);
        map2.put(dcClusterShard6, 2L);
        keeperFlowMap.put("127.0.0.2", map2);

        Map<DcClusterShardKeeper, Long> map3 = new HashMap<>();
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

//        Mockito.when(redisMsgCollector.getRedisMsgMap()).thenReturn(redisUsedMemoryMap);
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
        redisMsgReporter.
                reportKeeperContainerInfo();
//        Mockito.verify(checkerConsoleService, Mockito.times(1))
//                .reportKeeperContainerInfo(anyString(), resultCaptor.capture(), Mockito.anyInt());

        Assert.assertEquals(3, resultCaptor.getValue().size());
//        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : resultCaptor.getValue()) {
//            if ("127.0.0.1".equals(keeperContainerUsedInfoModel.getKeeperIp())) {
//                long activeInputFlow = keeperContainerUsedInfoModel.getActiveInputFlow();
//                long totalInputFlow = keeperContainerUsedInfoModel.getTotalInputFlow();
//                Assert.assertEquals(4, activeInputFlow);
//                Assert.assertEquals(6 ,totalInputFlow);
//            }
//        }
    }

    @Test
    public void testKeeperContainerUnHealthInfo() {
        Mockito.when(keeperContainerService.getKeeperDiskInfo(anyString())).thenReturn(null);
        redisMsgReporter.
                reportKeeperContainerInfo();
//        Mockito.verify(checkerConsoleService, Mockito.times(1))
//                .reportKeeperContainerInfo(anyString(), resultCaptor.capture(), Mockito.anyInt());

        Assert.assertEquals(3, resultCaptor.getValue().size());
//        for (KeeperContainerUsedInfoModel keeperContainerUsedInfoModel : resultCaptor.getValue()) {
//            if ("127.0.0.1".equals(keeperContainerUsedInfoModel.getKeeperIp())) {
//                long activeInputFlow = keeperContainerUsedInfoModel.getActiveInputFlow();
//                long totalInputFlow = keeperContainerUsedInfoModel.getTotalInputFlow();
//                Assert.assertEquals(4, activeInputFlow);
//                Assert.assertEquals(6 ,totalInputFlow);
//            }
//        }
    }

    @Test
    public void testKeeperContainerDiskSick() {
        KeeperDiskInfo diskInfo = new KeeperDiskInfo();
        diskInfo.available = false;
        diskInfo.spaceUsageInfo = null;
        Mockito.when(keeperContainerService.getKeeperDiskInfo(anyString())).thenReturn(diskInfo);
        Mockito.when(keeperContainerService.getKeeperDiskInfo(anyString())).thenReturn(null);
        redisMsgReporter.
                reportKeeperContainerInfo();
//        Mockito.verify(checkerConsoleService, Mockito.times(1))
//                .reportKeeperContainerInfo(anyString(), resultCaptor.capture(), Mockito.anyInt());

        Assert.assertEquals(3, resultCaptor.getValue().size());
    }

    @Test
    public void DcClusterShardActive(){
        DcClusterShardKeeper active = new DcClusterShardKeeper();
        active.setActive(true);
        active.setDcId("dc");
        active.setClusterId("cluster");
        active.setShardId("shard");
        active.setPort(123);
        Assert.assertEquals(active.toString(), "dc:cluster:shard:true:123");
        DcClusterShardKeeper active1 = new DcClusterShardKeeper(active.toString());
        Assert.assertEquals(active.toString(), active1.toString());
        Assert.assertEquals(active, active1);
        DcClusterShardKeeper active2 = new DcClusterShardKeeper("dc","cluster","shard", true, 123);
        Assert.assertEquals(active2.hashCode(), active1.hashCode());
    }
}
