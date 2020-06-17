package com.ctrip.xpipe.redis.meta.server.crdt.manage.impl;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.manage.PeerMasterStateAdjuster;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterAdjusterManagerTest extends AbstractMetaServerTest {

    @InjectMocks
    DefaultPeerMasterAdjusterManager defaultPeerMasterAdjusterManager;

    @Mock
    protected DcMetaCache dcMetaCache;

    @Mock
    protected PeerMasterStateAdjuster adjuster;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupPeerMasterStateManagerTest() throws Exception {
        defaultPeerMasterAdjusterManager.setScheduled(scheduled);
        defaultPeerMasterAdjusterManager.setExecutor(executors);
        defaultPeerMasterAdjusterManager.setClientPool(getXpipeNettyClientKeyedObjectPool());
        defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().put(Pair.of(clusterId, shardId), adjuster);
        Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(dcId);
    }

    @Test
    public void testDcMetaChange() {
        defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().put(Pair.of(clusterId, "shard2"), adjuster);
        ClusterMeta current = mockClusterMeta().addShard(new ShardMeta("shard2"));
        ClusterMeta future = mockClusterMeta().setDcs("dc1, dc2").addShard(new ShardMeta("shard3"));
        ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
        clusterMetaComparator.compare();
        Mockito.when(currentMetaManager.getPeerMaster(dcId, clusterId, shardId)).thenReturn(new RedisMeta());
        Mockito.when(currentMetaManager.getPeerMaster("dc2", clusterId, shardId)).thenReturn(new RedisMeta());
        defaultPeerMasterAdjusterManager.handleClusterModified(clusterMetaComparator);

        Assert.assertEquals(2, defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().size());
        Assert.assertTrue(defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().containsKey(Pair.of(clusterId, shardId)));
        Assert.assertTrue(defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().containsKey(Pair.of(clusterId, "shard3")));
        Mockito.verify(adjuster, Mockito.times(1)).adjust();
        Mockito.verify(currentMetaManager, Mockito.times(1)).addResource(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDcMetaDeleted() {
        defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().put(Pair.of(clusterId, "shard2"), adjuster);
        ClusterMeta clusterMeta = mockClusterMeta().addShard(new ShardMeta("shard2"));
        defaultPeerMasterAdjusterManager.handleClusterDeleted(clusterMeta);

        Assert.assertEquals(0, defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().size());
    }

    @Test
    public void testDcMetaAdded() {
        defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().clear();
        ClusterMeta clusterMeta = mockClusterMeta();
        defaultPeerMasterAdjusterManager.handleClusterAdd(clusterMeta);

        Assert.assertEquals(1, defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().size());
        Assert.assertTrue(defaultPeerMasterAdjusterManager.getPeerMasterAdjusterMap().containsKey(Pair.of(clusterId, shardId)));
    }

    private ClusterMeta mockClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        clusterMeta.addShard(shardMeta);
        clusterMeta.setDcs(dcId);

        return clusterMeta;
    }

}
