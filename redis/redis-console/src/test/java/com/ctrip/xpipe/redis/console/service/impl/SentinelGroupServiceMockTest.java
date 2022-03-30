package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ShardEventHandler;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.ctrip.xpipe.redis.checker.controller.result.RetMessage.FAIL_STATE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SentinelGroupServiceMockTest {

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
}
