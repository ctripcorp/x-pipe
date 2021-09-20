package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Random;

@RunWith(MockitoJUnitRunner.class)
public class ShardMetaServiceImplTest extends AbstractTest {

    @InjectMocks
    private ShardMetaServiceImpl shardMetaService;

    @Mock
    private DcService dcService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ShardService shardService;

    @Mock
    private RedisService redisService;

    @Mock
    private DcClusterService dcClusterService;

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Mock
    private RedisMetaService redisMetaService;

    private Random random = new Random();

    @Before
    public void beforeShardMetaServiceImplTest() {
        Mockito.doAnswer(invocationOnMock -> {
            String dcName = invocationOnMock.getArgument(0, String.class);
            sleep(random.nextInt(100));
            return mockDcInfo(dcName);
        }).when(dcService).find(Mockito.anyString());

        Mockito.doAnswer(invocationOnMock -> {
            String clusterName = invocationOnMock.getArgument(0, String.class);
            sleep(random.nextInt(100));
            return mockClusterTbl(clusterName);
        }).when(clusterService).find(Mockito.anyString());

        Mockito.doAnswer(invocationOnMock -> {
            String clusterName = invocationOnMock.getArgument(0, String.class);
            String shardName = invocationOnMock.getArgument(1, String.class);
            sleep(random.nextInt(100));
            return mockShardTbl(clusterName, shardName);
        }).when(shardService).find(Mockito.anyString(), Mockito.anyString());

        Mockito.doAnswer(invocationOnMock -> {
            String dcName = invocationOnMock.getArgument(0, String.class);
            String clusterName = invocationOnMock.getArgument(1, String.class);
            sleep(random.nextInt(100));
            return mockDcClusterTbl(dcName, clusterName);
        }).when(dcClusterService).find(Mockito.anyString(), Mockito.anyString());

        Mockito.doAnswer(invocationOnMock -> {
            String dcName = invocationOnMock.getArgument(0, String.class);
            String clusterName = invocationOnMock.getArgument(1, String.class);
            String shardName = invocationOnMock.getArgument(2, String.class);
            sleep(random.nextInt(100));
            return mockDcClusterShardTbl(dcName, clusterName, shardName);
        }).when(dcClusterShardService).find(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        Mockito.doAnswer(invocationOnMock -> {
            return Collections.emptyList();
        }).when(redisService).findAllByDcClusterShard(Mockito.anyLong());
    }

    @Test
    public void getShardMetaTest() {
        String dcName = "dc1", clusterName = "cluster1", shardName = "shard1";
        ShardMeta shardMeta = shardMetaService.getShardMeta(dcName, clusterName, shardName);
        Assert.assertEquals(shardName, shardMeta.getId());
        Assert.assertEquals(Long.valueOf(shardName.hashCode()), shardMeta.getSentinelId());
        Assert.assertEquals(SentinelUtil.getSentinelMonitorName(clusterName, shardName, dcName), shardMeta.getSentinelMonitorName());

        shardMeta = shardMetaService.getShardMeta(mockDcInfo(dcName), mockClusterTbl(clusterName), mockShardTbl(clusterName, shardName));
        Assert.assertEquals(shardName, shardMeta.getId());
        Assert.assertEquals(Long.valueOf(shardName.hashCode()), shardMeta.getSentinelId());
        Assert.assertEquals(SentinelUtil.getSentinelMonitorName(clusterName, shardName, dcName), shardMeta.getSentinelMonitorName());
    }

    private DcTbl mockDcInfo(String dcName) {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName(dcName);
        return dcTbl;
    }

    private ClusterTbl mockClusterTbl(String clusterName) {
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setClusterName(clusterName);
        return clusterTbl;
    }

    private ShardTbl mockShardTbl(String clusterName, String shardName) {
        ShardTbl shardTbl = new ShardTbl();
        shardTbl.setClusterName(clusterName);
        shardTbl.setShardName(shardName);
        shardTbl.setSetinelMonitorName(shardName);
        return shardTbl;
    }

    private DcClusterTbl mockDcClusterTbl(String dcName, String clusterName) {
        DcClusterTbl dcClusterTbl = new DcClusterTbl();
        dcClusterTbl.setDcName(dcName);
        dcClusterTbl.setClusterName(clusterName);
        return dcClusterTbl;
    }

    private DcClusterShardTbl mockDcClusterShardTbl(String dcName, String clusterName, String shardName) {
        DcClusterShardTbl dcClusterShardTbl = new DcClusterShardTbl();
        dcClusterShardTbl.setDcName(dcName);
        dcClusterShardTbl.setClusterName(clusterName);
        dcClusterShardTbl.setShardName(shardName);
        dcClusterShardTbl.setSetinelId(shardName.hashCode());
        return dcClusterShardTbl;
    }

}
