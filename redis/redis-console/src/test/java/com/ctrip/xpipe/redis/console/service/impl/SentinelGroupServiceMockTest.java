package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ShardEventHandler;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.ctrip.xpipe.redis.checker.controller.result.RetMessage.FAIL_STATE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SentinelGroupServiceMockTest extends AbstractRedisTest {

    @InjectMocks
    private SentinelGroupServiceImpl sentinelGroupService;

    @Mock
    private DcService dcService;

    @Mock
    private DcClusterShardService mockDcClusterShardService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ShardService mockShardService;

    @Mock
    private ShardEventHandler shardEventHandler;

    @Mock
    private MetaCache metaCache;

    @Mock
    private SentinelService sentinelService;

    @Test
    public void testGetSentinelGroups() {
        List<SentinelGroupTbl> sentinelGroupTbls = Lists.newArrayList();
        for (int i = 0; i < 3; i++) { sentinelGroupTbls.add(new SentinelGroupTbl().setSentinelGroupId(i + 1).setClusterType(ClusterType.ONE_WAY.name()).setActive(1).setKeySentinelGroupId(i + 1)); }

        List<DcClusterShardTbl> dcClusterShardTbls = new ArrayList<>();
        for (int i = 0; i < 6; i++) { dcClusterShardTbls.add(new DcClusterShardTbl().setSetinelId(i / 2 + 1).setDcClusterId(i + 1).setShardId(i + 1)); }

        MockitoAnnotations.initMocks(this);
        when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        when(metaCache.isCrossRegion(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String region1 = invocation.getArgument(0);
                    String region2 = invocation.getArgument(1);
                    if (region1.equals(region2)) return false;
                    if ("fra".equals(region1) || "fra".equals(region2)) return true;
                    return false;
                });

        when(mockDcClusterShardService.findAll()).thenReturn(dcClusterShardTbls);
        when(sentinelService.findAllWithDcName()).thenReturn(Lists.newArrayList());
        List<SentinelGroupModel> sentinelGroups = sentinelGroupService.getSentinelGroups(sentinelGroupTbls, true);
        Assert.assertEquals(3, sentinelGroups.size());
        Assert.assertEquals(2, sentinelGroups.get(0).getShardCount());
        Assert.assertEquals(2, sentinelGroups.get(1).getShardCount());
        Assert.assertEquals(2, sentinelGroups.get(2).getShardCount());

        sentinelGroups = sentinelGroupService.getSentinelGroups(sentinelGroupTbls, false);
        Assert.assertEquals(3, sentinelGroups.size());
        Assert.assertEquals(2, sentinelGroups.get(0).getShardCount());
        Assert.assertEquals(2, sentinelGroups.get(1).getShardCount());
        Assert.assertEquals(1, sentinelGroups.get(2).getShardCount());
    }


    @Test
    public void testRemoveSentinelMonitor() {
        String dc = "SHAJQ", cluster = "cluster-test";
        sentinelGroupService = new SentinelGroupServiceImpl() {
            @Override
            protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
                //do nothing
            }
        };
        sentinelGroupService.setClusterService(clusterService);
        sentinelGroupService.setShardService(mockShardService);
        sentinelGroupService.setDcService(dcService);
        sentinelGroupService.setShardEventHandler(shardEventHandler);
        sentinelGroupService.setDcClusterShardService(mockDcClusterShardService);
        when(clusterService.find(anyString())).thenReturn(new ClusterTbl().setActivedcId(1).setClusterType(ClusterType.ONE_WAY.toString()));
        when(dcService.getDcName(anyLong())).thenReturn(dc);
        when(mockDcClusterShardService.findAllByDcCluster(dc, cluster))
                .thenReturn(Lists.newArrayList(new DcClusterShardTbl().setShardId(1).setDcId(1L)));
        RetMessage retMessage = sentinelGroupService.removeSentinelMonitor(cluster);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
    }

    @Test
    public void testRemoveSentinelMonitorWithSentinelCallFail() {
        String dc = "SHAJQ", cluster = "cluster-test";
        sentinelGroupService = new SentinelGroupServiceImpl() {
            @Override
            protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
                throw new XpipeRuntimeException("fake timeout");
            }
        };
        sentinelGroupService.setClusterService(clusterService);
        sentinelGroupService.setShardService(mockShardService);
        sentinelGroupService.setDcService(dcService);
        sentinelGroupService.setShardEventHandler(shardEventHandler);
        sentinelGroupService.setDcClusterShardService(mockDcClusterShardService);
        when(clusterService.find(anyString())).thenReturn(new ClusterTbl().setActivedcId(1).setClusterType(ClusterType.ONE_WAY.toString()));
        when(dcService.getDcName(anyLong())).thenReturn(dc);
        when(mockDcClusterShardService.findAllByDcCluster(dc, cluster))
                .thenReturn(Lists.newArrayList(new DcClusterShardTbl().setShardId(1).setDcId(1L)));
        RetMessage retMessage = sentinelGroupService.removeSentinelMonitor(cluster);
        Assert.assertEquals(FAIL_STATE, retMessage.getState());
    }

    @Test
    public void testRemoveSentinelMonitorByShard() {
        String dc = "SHAJQ", cluster = "cluster-test", shard = "shard1";
        DcClusterShardTbl dcClusterShardTbl = new DcClusterShardTbl().setShardId(1L).setSetinelId(2L);
        sentinelGroupService = spy(sentinelGroupService);
        doReturn(new SentinelGroupModel().setSentinelGroupId(1).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setSentinelIp("10.0.0.1").setSentinelPort(5555),
                new SentinelInstanceModel().setSentinelIp("10.0.0.2").setSentinelPort(5555)
        ))).when(sentinelGroupService).findById(anyLong());
//
        when(mockShardService.find(anyLong())).thenReturn(new ShardTbl().setShardName(shard).setSetinelMonitorName(shard));
        doNothing().when(shardEventHandler).handleShardDelete(any(ShardEvent.class));
        sentinelGroupService.removeSentinelMonitorByShard(dc, cluster, ClusterType.ONE_WAY, dcClusterShardTbl);
        verify(shardEventHandler, times(1)).handleShardDelete(any(ShardEvent.class));
    }

    @Test
    public void testRemoveSentinelForCRDTCluster() {
        String clusterId = "test-cluster";
        when(clusterService.find(clusterId)).thenReturn(new ClusterTbl().setClusterType(ClusterType.BI_DIRECTION.toString()));

        RetMessage retMessage = sentinelGroupService.removeSentinelMonitor(clusterId);
        Assert.assertEquals(FAIL_STATE, retMessage.getState());
    }

    protected String getXpipeMetaConfigFile() {
        return "sentinel-usage-meta-test.xml";
    }
}
