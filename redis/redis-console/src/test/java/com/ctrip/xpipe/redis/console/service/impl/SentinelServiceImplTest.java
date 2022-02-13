package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.ctrip.xpipe.redis.checker.controller.result.RetMessage.FAIL_STATE;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class SentinelServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private SentinelServiceImpl sentinelService;

    private List<SetinelTbl> sentinels = new LinkedList<>();

    @Mock
    private ClusterService clusterService;

    @Mock
    private DcService dcService;

    @Mock
    private ShardEventHandler shardEventHandler;

    @Mock
    private ShardService shardService;

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Before
    public void beforeSentinelServiceImplTest() {
        MockitoAnnotations.initMocks(this);
        int size = randomInt(0, 100);
        for (int i = 0; i < size; i++) {
            sentinels.add(new SetinelTbl().setSetinelId(i));
        }
        sentinelService.setClusterService(clusterService);
        sentinelService.setDcService(dcService);
        sentinelService.setShardEventHandler(shardEventHandler);
        sentinelService.setShardService(shardService);
        sentinelService.setDcClusterShardService(dcClusterShardService);
    }

//    @Test
//    public void testAllSentinelsByDc() {
//
//        int dcCount = 5;
//        int sentinelsEachDc = 5;
//
//        for (int i = 0; i < dcCount; i++) {
//            for (int j = 0; j < sentinelsEachDc; j++) {
//                sentinelService.insert(new SetinelTbl().setDcId(i).setSetinelAddress("desc").setSetinelDescription(getTestName()));
//            }
//        }
//
//
//        Map<Long, List<SetinelTbl>> allSentinelsByDc = sentinelService.allSentinelsByDc();
//
//        Assert.assertEquals(dcCount, allSentinelsByDc.size());
//
//        allSentinelsByDc.forEach((dcId, sentinels) -> {
//            Assert.assertTrue(sentinels.size() >= sentinelsEachDc);
//        });
//
//    }

    @Test
    public void testRandom() {

        int testCount = 1 << 10;


        Set<Long> all = new HashSet<>();

        for (int i = 0; i < testCount; i++) {

            SetinelTbl random = sentinelService.random(sentinels);
            all.add(random.getSetinelId());
        }

        Assert.assertEquals(sentinels.size(), all.size());

    }

//    @Test
//    public void testGetSentinelUsage() {
//        logger.info("{}", sentinelService.allSentinelsByDc());
//        logger.info("{}", sentinelService.getAllSentinelsUsage());
//    }

    @Test
    public void testUpdateSentinel() {
        List<SetinelTbl> sentinels = sentinelService.findAllByDcName(dcNames[0]);
        Assert.assertFalse(sentinels.isEmpty());
        SetinelTbl target = sentinels.get(0);
        Assert.assertNotNull(target);

        String prevAddr = target.getSetinelAddress();

        SentinelModel model = new SentinelModel(target);
        model.getSentinels().remove(model.getSentinels().size() - 1);
        model.getSentinels().add(HostPort.fromString(String.join(":", "192.168.0.1", "" + randomPort())));

        SentinelModel updatedModel = sentinelService.updateSentinelTblAddr(model);

        SetinelTbl updated = sentinelService.find(target.getSetinelId());

        Assert.assertNotEquals(prevAddr, updated.getSetinelAddress());

        Assert.assertEquals(updatedModel, new SentinelModel(updated));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSentinelNotExist() {
        List<SetinelTbl> sentinels = sentinelService.findAllByDcName(dcNames[0]);
        Assert.assertFalse(sentinels.isEmpty());
        SetinelTbl target = sentinels.get(0);
        Assert.assertNotNull(target);

        SentinelModel model = new SentinelModel(target);
        model.getSentinels().remove(model.getSentinels().size() - 1);
        model.getSentinels().add(HostPort.fromString(String.join(":", "192.168.0.1", "" + randomPort())));

        long unSelectedBitSentinelId = 0;
        for(SetinelTbl setinelTbl : sentinels) {
            unSelectedBitSentinelId |= setinelTbl.getSetinelId();
        }

        long targetId = Math.abs(Long.MAX_VALUE ^ unSelectedBitSentinelId);
        model.setId(targetId);

        try {
            sentinelService.updateSentinelTblAddr(model);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }

    @Test
    public void testRemoveSentinelMonitor() {
        String dc = "SHAJQ", cluster = "cluster-test";
        sentinelService = new SentinelServiceImpl() {
            @Override
            protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
                //do nothing
            }
        };
        sentinelService.setClusterService(clusterService);
        sentinelService.setShardService(shardService);
        sentinelService.setDcService(dcService);
        sentinelService.setShardEventHandler(shardEventHandler);
        sentinelService.setDcClusterShardService(dcClusterShardService);
        when(clusterService.find(anyString())).thenReturn(new ClusterTbl().setActivedcId(1).setClusterType(ClusterType.ONE_WAY.toString()));
        when(dcService.getDcName(anyLong())).thenReturn(dc);
        when(dcClusterShardService.findAllByDcCluster(dc, cluster))
                .thenReturn(Lists.newArrayList(new DcClusterShardTbl().setShardId(1).setDcId(1L)));
        RetMessage retMessage = sentinelService.removeSentinelMonitor(cluster);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
    }

    @Test
    public void testRemoveSentinelMonitorWithSentinelCallFail() {
        String dc = "SHAJQ", cluster = "cluster-test";
        sentinelService = new SentinelServiceImpl() {
            @Override
            protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
                throw new XpipeRuntimeException("fake timeout");
            }
        };
        sentinelService.setClusterService(clusterService);
        sentinelService.setShardService(shardService);
        sentinelService.setDcService(dcService);
        sentinelService.setShardEventHandler(shardEventHandler);
        sentinelService.setDcClusterShardService(dcClusterShardService);
        when(clusterService.find(anyString())).thenReturn(new ClusterTbl().setActivedcId(1).setClusterType(ClusterType.ONE_WAY.toString()));
        when(dcService.getDcName(anyLong())).thenReturn(dc);
        when(dcClusterShardService.findAllByDcCluster(dc, cluster))
                .thenReturn(Lists.newArrayList(new DcClusterShardTbl().setShardId(1).setDcId(1L)));
        RetMessage retMessage = sentinelService.removeSentinelMonitor(cluster);
        Assert.assertEquals(FAIL_STATE, retMessage.getState());
    }

    @Test
    public void testRemoveSentinelMonitorByShard() {
        String dc = "SHAJQ", cluster = "cluster-test", shard = "shard1";
        DcClusterShardTbl dcClusterShardTbl = new DcClusterShardTbl().setShardId(1L).setSetinelId(2L);
        sentinelService = spy(sentinelService);
        when(sentinelService.find(anyLong())).thenReturn(new SetinelTbl().setSetinelAddress("10.0.0.1:5555,10.0.0.2:5555"));
        when(shardService.find(anyLong())).thenReturn(new ShardTbl().setShardName(shard).setSetinelMonitorName(shard));
        doNothing().when(shardEventHandler).handleShardDelete(any(ShardEvent.class));
        sentinelService.removeSentinelMonitorByShard(dc, cluster, ClusterType.ONE_WAY, dcClusterShardTbl);
        verify(shardEventHandler, times(1)).handleShardDelete(any(ShardEvent.class));
    }

    @Test
    public void testRemoveSentinelForCRDTCluster() {
        String clusterId = "test-cluster";
        Mockito.when(clusterService.find(clusterId)).thenReturn(new ClusterTbl().setClusterType(ClusterType.BI_DIRECTION.toString()));

        RetMessage retMessage = sentinelService.removeSentinelMonitor(clusterId);
        Assert.assertEquals(FAIL_STATE, retMessage.getState());
        logger.info("[testRemoveSentinelForCRDTCluster] response {}", retMessage.getMessage());
    }
}
