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
public class PeerMasterStateManagerTest extends AbstractMetaServerTest {

    @InjectMocks
    PeerMasterStateManager peerMasterStateManager;

    @Mock
    protected DcMetaCache dcMetaCache;

    @Mock
    protected PeerMasterStateAdjuster adjuster;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupPeerMasterStateManagerTest() throws Exception {
        peerMasterStateManager.setScheduled(scheduled);
        peerMasterStateManager.setExecutor(executors);
        peerMasterStateManager.setClientPool(getXpipeNettyClientKeyedObjectPool());
        peerMasterStateManager.getPeerMasterAdjusterMap().put(Pair.of(clusterId, shardId), adjuster);
        Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(dcId);
    }

    @Test
    public void testDcMetaChange() {
        peerMasterStateManager.getPeerMasterAdjusterMap().put(Pair.of(clusterId, "shard2"), adjuster);
        ClusterMeta current = mockClusterMeta().addShard(new ShardMeta("shard2"));
        ClusterMeta future = mockClusterMeta().setDcs("dc1, dc2").addShard(new ShardMeta("shard3"));
        ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
        clusterMetaComparator.compare();
        Mockito.when(currentMetaManager.getPeerMaster(dcId, clusterId, shardId)).thenReturn(new RedisMeta());
        Mockito.when(currentMetaManager.getPeerMaster("dc2", clusterId, shardId)).thenReturn(new RedisMeta());
        peerMasterStateManager.handleClusterModified(clusterMetaComparator);

        Assert.assertEquals(2, peerMasterStateManager.getPeerMasterAdjusterMap().size());
        Assert.assertTrue(peerMasterStateManager.getPeerMasterAdjusterMap().containsKey(Pair.of(clusterId, shardId)));
        Assert.assertTrue(peerMasterStateManager.getPeerMasterAdjusterMap().containsKey(Pair.of(clusterId, "shard3")));
        Mockito.verify(adjuster, Mockito.times(1)).adjust();
        Mockito.verify(currentMetaManager, Mockito.times(1)).addResource(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDcMetaDeleted() {
        peerMasterStateManager.getPeerMasterAdjusterMap().put(Pair.of(clusterId, "shard2"), adjuster);
        ClusterMeta clusterMeta = mockClusterMeta().addShard(new ShardMeta("shard2"));
        peerMasterStateManager.handleClusterDeleted(clusterMeta);

        Assert.assertEquals(0, peerMasterStateManager.getPeerMasterAdjusterMap().size());
    }

    @Test
    public void testDcMetaAdded() {
        peerMasterStateManager.getPeerMasterAdjusterMap().clear();
        ClusterMeta clusterMeta = mockClusterMeta();
        peerMasterStateManager.handleClusterAdd(clusterMeta);

        Assert.assertEquals(1, peerMasterStateManager.getPeerMasterAdjusterMap().size());
        Assert.assertTrue(peerMasterStateManager.getPeerMasterAdjusterMap().containsKey(Pair.of(clusterId, shardId)));
    }

    private ClusterMeta mockClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        clusterMeta.addShard(shardMeta);
        clusterMeta.setDcs(dcId);

        return clusterMeta;
    }

}
