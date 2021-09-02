package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


/**
 * @author chen.zhu
 * <p>
 * Jan 29, 2018
 */
public class ShardServiceImplTest2 {

    @Mock
    private ShardDao shardDao;

    @InjectMocks
    private ShardServiceImpl shardService = new ShardServiceImpl();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
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
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());

        Assert.assertEquals(cluster + "-" + proto.getShardName(), shardTbl.getSetinelMonitorName());
    }

    // monitor name not exist
    @Test
    public void findOrCreateShardIfNotExist2() throws Exception {
        ShardTbl proto = new ShardTbl().setShardName("shard2");
        String cluster = "cluster-test";

        when(shardDao.queryAllShardsByClusterName(anyString())).thenReturn(null);
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1"));
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());

        Assert.assertEquals(proto.getShardName(), shardTbl.getSetinelMonitorName());
    }


    // monitor name exist both for shard and cluster-shard
    @Test(expected = java.lang.IllegalStateException.class)
    public void findOrCreateShardIfNotExist3() throws Exception {
        ShardTbl proto = new ShardTbl().setShardName("shard1");
        String cluster = "cluster-test";

        when(shardDao.queryAllShardsByClusterName(anyString())).thenReturn(null);
        when(shardDao.queryAllShardMonitorNames()).thenReturn(Sets.newHashSet("shard1",
                cluster + "-" + proto.getShardName()));
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        try {
            ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());
        } catch (IllegalStateException e) {
            Assert.assertEquals(String.format("Both %s and %s is assigned as sentinel monitor name",
                    proto.getShardName(), cluster + "-" + proto.getShardName()), e.getMessage());
            throw e;
        }
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
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());

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
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());

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
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        try {
            ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());
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
        when(shardDao.insertShard(cluster, proto, Maps.newHashMap())).thenReturn(proto);

        try {
            ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(cluster, proto, Maps.newHashMap());
        } catch (Exception e) {
            Assert.assertEquals(String.format("Shard monitor name %s already exist", shard), e.getMessage());
            throw e;
        }
    }
}