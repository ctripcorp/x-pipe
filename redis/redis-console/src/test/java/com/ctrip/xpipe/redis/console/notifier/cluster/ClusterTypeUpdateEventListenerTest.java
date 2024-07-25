package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ClusterTypeUpdateEventListenerTest {

    @InjectMocks
    private ClusterTypeUpdateEventListener listener;

    @Mock
    private ShardService shardService;

    @Mock
    private SentinelBalanceService sentinelBalanceService;

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Mock
    private DcClusterService dcClusterService;

    @Mock
    private ClusterService clusterService;

    @Test
    public void updateNonCrossDcClusterTest() throws Exception {

        ClusterTypeUpdateEvent event = new ClusterTypeUpdateEvent("cluster", 1, Mockito.mock(ExecutorService.class));
        event.setClusterType(ClusterType.LOCAL_DC);


        when(shardService.findAllByClusterName("cluster")).thenReturn(Lists.newArrayList(
                new ShardTbl().setShardName("shard1"), new ShardTbl().setShardName("shard2")
        ));

        when(dcClusterService.findClusterRelated("cluster")).thenReturn(Lists.newArrayList(
                new DcClusterCreateInfo().setClusterName("cluster").setDcName("dc1"),
                new DcClusterCreateInfo().setClusterName("cluster").setDcName("dc2")
        ));

        DcClusterShardTbl dc1shard1 = new DcClusterShardTbl().setSetinelId(1);
        DcClusterShardTbl dc1shard2 = new DcClusterShardTbl().setSetinelId(2);
        DcClusterShardTbl dc2shard1 = new DcClusterShardTbl().setSetinelId(3);
        DcClusterShardTbl dc2shard2 = new DcClusterShardTbl().setSetinelId(4);
        when(dcClusterShardService.find("dc1", "cluster", "shard1")).thenReturn(dc1shard1);
        when(dcClusterShardService.find("dc1", "cluster", "shard2")).thenReturn(dc1shard2);
        when(dcClusterShardService.find("dc2", "cluster", "shard1")).thenReturn(dc2shard1);
        when(dcClusterShardService.find("dc2", "cluster", "shard2")).thenReturn(dc2shard2);

        when(clusterService.findClusterTag("cluster")).thenReturn("");
        when(sentinelBalanceService.selectSentinel("dc1", ClusterType.LOCAL_DC, "")).thenReturn(new SentinelGroupModel().setSentinelGroupId(5).setClusterType(ClusterType.LOCAL_DC.name()));
        when(sentinelBalanceService.selectSentinel("dc2", ClusterType.LOCAL_DC, "")).thenReturn(new SentinelGroupModel().setSentinelGroupId(6).setClusterType(ClusterType.LOCAL_DC.name()));

        listener.update(event.getClusterEventType(), event);

        verify(dcClusterShardService, times(4)).updateDcClusterShard(any());
        Assert.assertEquals(5, dc1shard1.getSetinelId());
        Assert.assertEquals(5, dc1shard2.getSetinelId());
        Assert.assertEquals(6, dc2shard1.getSetinelId());
        Assert.assertEquals(6, dc2shard1.getSetinelId());
    }


    @Test
    public void updateCrossDcClusterTest() throws Exception {

        ClusterTypeUpdateEvent event = new ClusterTypeUpdateEvent("cluster", 1, Mockito.mock(ExecutorService.class));
        event.setClusterType(ClusterType.CROSS_DC);


        when(shardService.findAllByClusterName("cluster")).thenReturn(Lists.newArrayList(
                new ShardTbl().setShardName("shard1"), new ShardTbl().setShardName("shard2")
        ));

        when(dcClusterService.findClusterRelated("cluster")).thenReturn(Lists.newArrayList(
                new DcClusterCreateInfo().setClusterName("cluster").setDcName("dc1"),
                new DcClusterCreateInfo().setClusterName("cluster").setDcName("dc2")
        ));

        DcClusterShardTbl dc1shard1 = new DcClusterShardTbl().setSetinelId(1);
        DcClusterShardTbl dc1shard2 = new DcClusterShardTbl().setSetinelId(2);
        DcClusterShardTbl dc2shard1 = new DcClusterShardTbl().setSetinelId(3);
        DcClusterShardTbl dc2shard2 = new DcClusterShardTbl().setSetinelId(4);
        when(dcClusterShardService.find("cluster", "shard1")).thenReturn(Lists.newArrayList(dc1shard1, dc2shard1));
        when(dcClusterShardService.find("cluster", "shard2")).thenReturn(Lists.newArrayList(dc1shard2, dc2shard2));

        when(clusterService.findClusterTag("cluster")).thenReturn("");
        when(sentinelBalanceService.selectSentinel("dc1", ClusterType.CROSS_DC, "")).thenReturn(new SentinelGroupModel().setSentinelGroupId(5).setClusterType(ClusterType.CROSS_DC.name()));
//        when(sentinelBalanceService.selectSentinel("dc2", ClusterType.CROSS_DC)).thenReturn(new SentinelGroupModel().setSentinelGroupId(6).setClusterType(ClusterType.CROSS_DC.name()));

        listener.update(event.getClusterEventType(), event);

        verify(dcClusterShardService, times(4)).updateDcClusterShard(any());
        Assert.assertEquals(dc2shard1.getSetinelId(), dc1shard1.getSetinelId());
        Assert.assertEquals(dc2shard2.getSetinelId(), dc1shard2.getSetinelId());

    }
}
