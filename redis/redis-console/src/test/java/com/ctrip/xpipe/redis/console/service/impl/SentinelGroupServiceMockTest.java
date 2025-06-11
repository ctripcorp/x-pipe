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
        SentinelGroupTbl tbl1 = new SentinelGroupTbl();
        tbl1.setSentinelGroupId(1);
        tbl1.setClusterType(ClusterType.ONE_WAY.name());
        tbl1.setDeleted(0);
        tbl1.setActive(1);
        tbl1.setDatachangeLasttime(new Date());
        tbl1.setKeySentinelGroupId(1);
        tbl1.setCount(0);
        tbl1.setShardCount(0);
        sentinelGroupTbls.add(tbl1);

        SentinelGroupTbl tbl2 = new SentinelGroupTbl();
        tbl2.setSentinelGroupId(2);
        tbl2.setClusterType(ClusterType.ONE_WAY.name());
        tbl2.setDeleted(0);
        tbl2.setActive(1);
        tbl2.setDatachangeLasttime(new Date());
        tbl2.setKeySentinelGroupId(2);
        tbl2.setCount(0);
        tbl2.setShardCount(0);
        sentinelGroupTbls.add(tbl2);

        SentinelGroupTbl tbl3 = new SentinelGroupTbl();
        tbl3.setSentinelGroupId(3);
        tbl3.setClusterType(ClusterType.ONE_WAY.name());
        tbl3.setDeleted(0);
        tbl3.setActive(1);
        tbl3.setDatachangeLasttime(new Date());
        tbl3.setKeySentinelGroupId(3);
        tbl3.setCount(0);
        tbl3.setShardCount(0);
        sentinelGroupTbls.add(tbl3);

        List<DcClusterShardTbl> dcClusterShardTbls = Lists.newArrayList();
        DcClusterShardTbl tbl4 = new DcClusterShardTbl();
        tbl4.setSetinelId(1);
        tbl4.setDcClusterId(1);
        tbl4.setShardId(1);
        dcClusterShardTbls.add(tbl4);
        DcClusterShardTbl tbl5 = new DcClusterShardTbl();
        tbl5.setSetinelId(1);
        tbl5.setDcClusterId(2);
        tbl5.setShardId(2);
        dcClusterShardTbls.add(tbl5);
        DcClusterShardTbl tbl6 = new DcClusterShardTbl();
        tbl6.setSetinelId(2);
        tbl6.setDcClusterId(3);
        tbl6.setShardId(3);
        dcClusterShardTbls.add(tbl6);
        DcClusterShardTbl tbl7 = new DcClusterShardTbl();
        tbl7.setSetinelId(2);
        tbl7.setDcClusterId(4);
        tbl7.setShardId(4);
        dcClusterShardTbls.add(tbl7);
        DcClusterShardTbl tbl8 = new DcClusterShardTbl();
        tbl8.setSetinelId(3);
        tbl8.setDcClusterId(5);
        tbl8.setShardId(5);
        dcClusterShardTbls.add(tbl8);
        DcClusterShardTbl tbl9 = new DcClusterShardTbl();
        tbl9.setSetinelId(3);
        tbl9.setDcClusterId(6);
        tbl9.setShardId(6);
        dcClusterShardTbls.add(tbl9);

        MockitoAnnotations.initMocks(this);
        when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        when(metaCache.isCrossRegion("fra", "jq")).thenReturn(true);
        when(metaCache.isCrossRegion("fra", "oy")).thenReturn(true);
        when(metaCache.isCrossRegion("oy", "jq")).thenReturn(false);
        when(metaCache.isCrossRegion("oy", "oy")).thenReturn(false);
        when(metaCache.isCrossRegion("jq", "jq")).thenReturn(false);
        when(metaCache.isCrossRegion("jq", "oy")).thenReturn(false);

        when(mockDcClusterShardService.findAll()).thenReturn(dcClusterShardTbls);
        when(sentinelService.findAllWithDcName()).thenReturn(Lists.newArrayList());
        List<SentinelGroupModel> sentinelGroups = sentinelGroupService.getSentinelGroups(sentinelGroupTbls, true);
        Assert.assertEquals(3, sentinelGroups.size());
        Assert.assertEquals(2, sentinelGroups.get(0).getShardCount());
        Assert.assertEquals(2, sentinelGroups.get(1).getShardCount());
        Assert.assertEquals(2, sentinelGroups.get(2).getShardCount());
        sentinelGroups = sentinelGroupService.getSentinelGroups(sentinelGroupTbls, false);
        Assert.assertEquals(3, sentinelGroups.size());
        Assert.assertEquals(1, sentinelGroups.get(0).getShardCount());
        Assert.assertEquals(2, sentinelGroups.get(1).getShardCount());
        Assert.assertEquals(1, sentinelGroups.get(2).getShardCount());
        logger.info("[getSentinelGroups]{}", sentinelGroups);
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
