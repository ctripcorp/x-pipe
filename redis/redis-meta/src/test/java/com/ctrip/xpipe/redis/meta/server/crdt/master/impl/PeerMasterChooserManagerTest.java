package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.MasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterChooserManagerTest extends AbstractMetaServerTest {

    @InjectMocks
    DefaultMasterChooserManager defaultPeerMasterChooserManager;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Before
    public void setupPeerMasterChooserManagerTest() throws Exception {
        defaultPeerMasterChooserManager.initialize();
    }

    @After
    public void afterPeerMasterChooserManagerTest() throws Exception {
        defaultPeerMasterChooserManager.dispose();
    }

    @Test
    public void testDcMetaAdded() {
        Mockito.doAnswer(invocation -> {
            String paramClusterId = invocation.getArgument(0, String.class);
            String paramShardId = invocation.getArgument(1, String.class);

            Assert.assertEquals(getClusterId(), paramClusterId);
            Assert.assertEquals(getShardId(), paramShardId);
            return null;
        }).when(currentMetaManager).addResource(Mockito.anyLong(), Mockito.anyLong(), Mockito.any());

        ClusterMeta clusterMeta = mockClusterMeta();
        defaultPeerMasterChooserManager.update(new NodeAdded<>(clusterMeta), defaultPeerMasterChooserManager);
        Mockito.verify(currentMetaManager, Mockito.times(2)).addResource(Mockito.anyLong(), Mockito.anyLong(), Mockito.any(MasterChooser.class));
    }

    private ClusterMeta mockClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta(getClusterId()).setDbId(getClusterDbId());
        ShardMeta shardMeta = new ShardMeta(getShardId()).setDbId(getShardDbId());
        clusterMeta.setType(ClusterType.BI_DIRECTION.toString());
        clusterMeta.addShard(shardMeta);

        return clusterMeta;
    }

}
