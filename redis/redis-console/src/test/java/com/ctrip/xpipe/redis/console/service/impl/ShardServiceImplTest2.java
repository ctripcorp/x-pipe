package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.entity.ShardEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.repository.DcClusterRepository;
import com.ctrip.xpipe.redis.console.repository.ShardRepository;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * @author chen.zhu
 * <p>
 * Jan 29, 2018
 */
public class ShardServiceImplTest2 {

    @Mock
    private ShardDao shardDao;

    @Mock
    private ClusterService clusterService;

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Mock
    private DcClusterService dcClusterService;

    @Mock
    private ShardRepository shardRepository;

    @Mock
    private DcClusterRepository dcClusterRepository;

    @Mock
    private AzGroupClusterRepository azGroupClusterRepository;

    @Mock
    private ConsoleConfig consoleConfig;

    @InjectMocks
    private ShardServiceImpl shardService = new ShardServiceImpl();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(clusterService.find(anyString())).thenReturn(
            new ClusterTbl().setId(1L).setClusterName("cluster-test").setClusterType(ClusterType.ONE_WAY.toString()));
    }

    /**==========================================================================
    * no monitor name is posted
     ==========================================================================* */
    // monitor name exist, no shard exist
    @Test
    public void findOrCreateShardIfNotExist1() throws Exception {
        ShardTbl proto = new ShardTbl().setShardName("shard1");
        String cluster = "cluster-test";

        when(shardDao.queryAllShardsByClusterName(anyString())).thenReturn(null);
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1"));
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

//        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());

        try {
            ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, anyList(), Maps.newHashMap());
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals("monitor name shard1 already exist", e.getMessage());
        }
    }

    // monitor name not exist
    @Test
    public void findOrCreateShardIfNotExist2() throws Exception {
        ShardTbl proto = new ShardTbl().setShardName("shard2");
        String cluster = "cluster-test";

        when(shardDao.queryAllShardsByClusterName(anyString())).thenReturn(null);
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1"));
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, anyList(), Maps.newHashMap());

        Assert.assertEquals(proto.getShardName(), shardTbl.getSetinelMonitorName());
    }

    // shard exist
    @Test
    public void findOrCreateShardIfNotExist4() throws Exception {
        ShardTbl proto = new ShardTbl().setShardName("shard1");
        String cluster = "cluster-test";

        ShardTbl expected = new ShardTbl().setShardName("shard1").setSetinelMonitorName("shard1");

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(expected));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1",
                cluster + "-" + proto.getShardName()));
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, anyList(), Maps.newHashMap());

        Assert.assertTrue(expected == shardTbl);
    }


    /**==========================================================================
     * monitor name is posted
     ==========================================================================* */
    // shard exist
    @Test
    public void findOrCreateShardIfNotExist5() throws Exception {
        String cluster = "cluster-test", shard = "shard1";
        ShardTbl proto = new ShardTbl().setShardName(shard).setSetinelMonitorName(shard);

        ShardTbl expected = new ShardTbl().setShardName("shard1").setSetinelMonitorName("shard1");

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(expected));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1",
                cluster + "-" + proto.getShardName()));
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, anyList(), Maps.newHashMap());

        Assert.assertTrue(expected == shardTbl);
    }


    // shard exist with diff monitor name
    @Test(expected = java.lang.IllegalArgumentException.class)
    public void findOrCreateShardIfNotExist6() throws Exception {
        String cluster = "cluster-test", shard = "shard1";
        ShardTbl proto = new ShardTbl().setShardName(shard).setSetinelMonitorName(shard);

        ShardTbl expected = new ShardTbl().setShardName("shard1").setSetinelMonitorName(cluster + "-shard1");

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(expected));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1",
                cluster + "-" + proto.getShardName()));
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

        try {
            ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, anyList(), Maps.newHashMap());
        } catch (Exception e) {
            Assert.assertEquals(String.format("Post shard monitor name %s diff from previous %s",
                    shard, cluster + "-shard1"), e.getMessage());
            throw e;
        }
    }

    // shard not exist, but monitor name has been occupied by other shard
    @Test(expected = java.lang.IllegalArgumentException.class)
    public void findOrCreateShardIfNotExist7() throws Exception {
        String cluster = "cluster-test", shard = "shard1";
        ShardTbl proto = new ShardTbl().setShardName(shard).setSetinelMonitorName(shard);

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList());
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1",
                cluster + "-" + proto.getShardName()));
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

        try {
            ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, anyList(), Maps.newHashMap());
        } catch (Exception e) {
            Assert.assertEquals(String.format("Shard monitor name %s already exist", shard), e.getMessage());
            throw e;
        }
    }

    @Test
    public void findOrCreateSHardIfNotExistWithCreateDcClusterShard() throws DalException {
        String cluster = "cluster-test", shard = "shard1";
        // TODO: 2022/10/10 remove hetero
//        ClusterTbl clusterTbl = new ClusterTbl().setClusterName(cluster).setClusterType(ClusterType.HETERO.toString());
        ClusterTbl clusterTbl = new ClusterTbl().setClusterName(cluster).setClusterType(ClusterType.ONE_WAY.toString());
        ShardTbl proto = new ShardTbl().setShardName(shard).setSetinelMonitorName(shard);

        when(clusterService.find(cluster)).thenReturn(clusterTbl);
        when(consoleConfig.supportSentinelHealthCheck(any(), anyString())).thenReturn(true);
        when(shardDao.insertShard(cluster, proto)).thenReturn(proto);

        shardService.findOrCreateShardIfNotExist(cluster, proto, Lists.newArrayList(new DcClusterTbl()), Maps.newHashMap());
        verify(dcClusterShardService).insertBatch(anyList());
    }

    @Test
    public void findOrCreateShardIfNotExist_nullDcClusters_bindOnlySameAzGroup() {
        String cluster = "test_hetero_probe_beacon";
        String shardName = "test_hetero_probe_beacon_2_SGP";
        long shardId = 200237L;
        long sgpAzGroupId = 2008L;

        ShardTbl existing = new ShardTbl().setId(shardId).setShardName(shardName).setSetinelMonitorName(shardName);
        ClusterTbl clusterTbl = new ClusterTbl().setId(31364L).setClusterName(cluster)
            .setClusterType(ClusterType.HETERO.toString());

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(existing));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet(shardName));
        when(clusterService.find(cluster)).thenReturn(clusterTbl);
        when(shardRepository.selectById(shardId))
            .thenReturn(new ShardEntity().setId(shardId).setAzGroupClusterId(sgpAzGroupId));
        when(dcClusterRepository.selectByAzGroupClusterId(sgpAzGroupId)).thenReturn(Lists.newArrayList(
            new DcClusterEntity().setDcClusterId(52241L).setDcId(3L).setAzGroupClusterId(sgpAzGroupId),
            new DcClusterEntity().setDcClusterId(52244L).setDcId(4L).setAzGroupClusterId(sgpAzGroupId)
        ));
        when(dcClusterShardService.find(anyLong(), eq(shardId))).thenReturn(null);
        when(consoleConfig.supportSentinelHealthCheck(any(), anyString())).thenReturn(false);

        shardService.findOrCreateShardIfNotExist(cluster, new ShardTbl().setShardName(shardName), null, Maps.newHashMap());

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(dcClusterShardService).insertBatch(captor.capture());
        List<DcClusterShardTbl> inserted = (List<DcClusterShardTbl>) captor.getValue();
        Assert.assertEquals(2, inserted.size());
        Set<Long> dcClusterIds = inserted.stream().map(DcClusterShardTbl::getDcClusterId).collect(Collectors.toSet());
        Assert.assertEquals(Sets.newHashSet(52241L, 52244L), dcClusterIds);
        verify(dcClusterService, never()).findClusterRelated(anyLong());
        verify(azGroupClusterRepository, never()).selectAzGroupTypeById(anyLong());
    }

    @Test
    public void findOrCreateShardIfNotExist_nullDcClusters_nonHeteroBindAllRelatedDcs() {
        String cluster = "one-way-cluster";
        String shardName = "one-way-shard";
        long shardId = 100L;

        ShardTbl existing = new ShardTbl().setId(shardId).setShardName(shardName).setSetinelMonitorName(shardName);
        ClusterTbl clusterTbl = new ClusterTbl().setId(7L).setClusterName(cluster)
            .setClusterType(ClusterType.ONE_WAY.toString());

        DcClusterTbl jq = new DcClusterTbl().setDcClusterId(31L).setDcId(1L);
        DcClusterTbl oy = new DcClusterTbl().setDcClusterId(32L).setDcId(2L);

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(existing));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet(shardName));
        when(clusterService.find(cluster)).thenReturn(clusterTbl);
        when(dcClusterService.findClusterRelated(7L)).thenReturn(Lists.newArrayList(jq, oy));
        when(dcClusterShardService.find(anyLong(), eq(shardId))).thenReturn(null);
        when(consoleConfig.supportSentinelHealthCheck(any(), anyString())).thenReturn(false);

        shardService.findOrCreateShardIfNotExist(cluster, new ShardTbl().setShardName(shardName), null, Maps.newHashMap());

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(dcClusterShardService).insertBatch(captor.capture());
        List<DcClusterShardTbl> inserted = (List<DcClusterShardTbl>) captor.getValue();
        Assert.assertEquals(2, inserted.size());
        Set<Long> dcClusterIds = inserted.stream().map(DcClusterShardTbl::getDcClusterId).collect(Collectors.toSet());
        Assert.assertEquals(Sets.newHashSet(31L, 32L), dcClusterIds);
        verify(dcClusterRepository, never()).selectByAzGroupClusterId(anyLong());
        verify(shardRepository, never()).selectById(anyLong());
    }

    @Test(expected = com.ctrip.xpipe.redis.console.exception.BadRequestException.class)
    public void findOrCreateShardIfNotExist_nullDcClusters_heteroWithoutAzGroupShouldFail() {
        String cluster = "hetero-cluster";
        String shardName = "hetero-shard";
        long shardId = 200L;

        ShardTbl existing = new ShardTbl().setId(shardId).setShardName(shardName).setSetinelMonitorName(shardName);
        ClusterTbl clusterTbl = new ClusterTbl().setId(8L).setClusterName(cluster)
            .setClusterType(ClusterType.HETERO.toString());

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(existing));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet(shardName));
        when(clusterService.find(cluster)).thenReturn(clusterTbl);
        when(shardRepository.selectById(shardId))
            .thenReturn(new ShardEntity().setId(shardId).setAzGroupClusterId(0L));

        shardService.findOrCreateShardIfNotExist(cluster, new ShardTbl().setShardName(shardName), null, Maps.newHashMap());
    }

    @Test(expected = com.ctrip.xpipe.redis.console.exception.BadRequestException.class)
    public void findOrCreateShardIfNotExist_nullDcClusters_heteroCreateNewShardShouldFail() {
        String cluster = "hetero-cluster";
        String shardName = "new-shard";

        ClusterTbl clusterTbl = new ClusterTbl().setId(8L).setClusterName(cluster)
            .setClusterType(ClusterType.HETERO.toString());

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList());
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet());
        when(clusterService.find(cluster)).thenReturn(clusterTbl);

        shardService.findOrCreateShardIfNotExist(cluster,
            new ShardTbl().setShardName(shardName).setSetinelMonitorName(shardName), null, Maps.newHashMap());
    }

    @Test
    public void findOrCreateShardIfNotExist_nullDcClusters_skipAlreadyBoundSameAzGroup() {
        String cluster = "test_hetero_probe_beacon";
        String shardName = "test_hetero_probe_beacon_2_SGP";
        long shardId = 200237L;
        long sgpAzGroupId = 2008L;

        ShardTbl existing = new ShardTbl().setId(shardId).setShardName(shardName).setSetinelMonitorName(shardName);
        ClusterTbl clusterTbl = new ClusterTbl().setId(31364L).setClusterName(cluster)
            .setClusterType(ClusterType.HETERO.toString());

        when(shardDao.queryAllShardsByClusterName(cluster)).thenReturn(Lists.newArrayList(existing));
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet(shardName));
        when(clusterService.find(cluster)).thenReturn(clusterTbl);
        when(shardRepository.selectById(shardId))
            .thenReturn(new ShardEntity().setId(shardId).setAzGroupClusterId(sgpAzGroupId));
        when(dcClusterRepository.selectByAzGroupClusterId(sgpAzGroupId)).thenReturn(Lists.newArrayList(
            new DcClusterEntity().setDcClusterId(52241L).setDcId(3L).setAzGroupClusterId(sgpAzGroupId),
            new DcClusterEntity().setDcClusterId(52244L).setDcId(4L).setAzGroupClusterId(sgpAzGroupId)
        ));
        when(dcClusterShardService.find(52241L, shardId)).thenReturn(new DcClusterShardTbl());
        when(dcClusterShardService.find(52244L, shardId)).thenReturn(null);
        when(consoleConfig.supportSentinelHealthCheck(any(), anyString())).thenReturn(false);

        shardService.findOrCreateShardIfNotExist(cluster, new ShardTbl().setShardName(shardName), null, Maps.newHashMap());

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(dcClusterShardService).insertBatch(captor.capture());
        List<DcClusterShardTbl> inserted = (List<DcClusterShardTbl>) captor.getValue();
        Assert.assertEquals(1, inserted.size());
        Assert.assertEquals(52244L, inserted.get(0).getDcClusterId());
    }
}
