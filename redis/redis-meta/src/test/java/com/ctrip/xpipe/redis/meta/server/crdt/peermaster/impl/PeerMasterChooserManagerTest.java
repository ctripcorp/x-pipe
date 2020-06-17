package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
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
public class PeerMasterChooserManagerTest extends AbstractMetaServerTest {

    @InjectMocks
    PeerMasterChooserManager peerMasterChooserManager;

    @Mock
    private XpipeNettyClientKeyedObjectPool clientPool;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private MultiDcService multiDcService;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private CurrentClusterServer currentClusterServer;

    @Mock
    private DefaultPeerMasterChooser chooser;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupPeerMasterChooserManagerTest() throws Exception {
        peerMasterChooserManager.initialize();
    }

    @Test
    public void testHandleClusterDcChange() {
        ClusterMeta current = mockClusterMeta();
        ClusterMeta future = mockClusterMeta();
        current.setDcs("jq, oy");
        future.setDcs("jq");
        ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
        peerMasterChooserManager.handleClusterModified(clusterMetaComparator);
        Mockito.doAnswer(invocation -> {
            String paramDc = invocation.getArgumentAt(0, String.class);
            String paramCluster = invocation.getArgumentAt(0, String.class);
            String paramShard = invocation.getArgumentAt(0, String.class);

            Assert.assertEquals("oy", paramDc);
            Assert.assertEquals(clusterId, paramCluster);
            Assert.assertEquals(shardId, paramShard);

            return null;
        }).when(currentMetaManager).removePeerMaster(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(currentMetaManager, Mockito.times(1)).removePeerMaster(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testDcMetaDeleted() {
        peerMasterChooserManager.getPeerMasterChooserMap().put(Pair.of(clusterId, shardId), chooser);
        ClusterMeta clusterMeta = mockClusterMeta();
        peerMasterChooserManager.handleClusterDeleted(clusterMeta);

        Assert.assertEquals(0, peerMasterChooserManager.getPeerMasterChooserMap().size());
    }

    @Test
    public void testDcMetaAdded() {
        peerMasterChooserManager.getPeerMasterChooserMap().clear();
        ClusterMeta clusterMeta = mockClusterMeta();
        peerMasterChooserManager.handleClusterAdd(clusterMeta);

        Assert.assertEquals(1, peerMasterChooserManager.getPeerMasterChooserMap().size());
        Assert.assertTrue(peerMasterChooserManager.getPeerMasterChooserMap().containsKey(Pair.of(clusterId, shardId)));
    }

    @Test
    public void testHandleRemotePeerMasterChange() {
        peerMasterChooserManager.getPeerMasterChooserMap().put(Pair.of(clusterId, shardId), chooser);
        PeerMasterChooseCommand command = Mockito.mock(PeerMasterChooseCommand.class);
        Mockito.when(chooser.createMasterChooserCommand(Mockito.anyString())).thenReturn(command);

        peerMasterChooserManager.handleRemotePeerMasterChange(dcId, clusterId, shardId);
        Mockito.verify(command, Mockito.times(1)).execute(Mockito.any());
    }

    private ClusterMeta mockClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta();
        ShardMeta shardMeta = new ShardMeta();
        clusterMeta.setId(clusterId);
        shardMeta.setId(shardId);
        clusterMeta.addShard(shardMeta);

        return clusterMeta;
    }

}
