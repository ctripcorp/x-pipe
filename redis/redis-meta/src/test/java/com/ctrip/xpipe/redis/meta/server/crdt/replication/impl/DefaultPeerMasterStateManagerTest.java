package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterStateManagerTest extends AbstractMetaServerTest {

    @InjectMocks
    private DefaultPeerMasterStateManager defaultPeerMasterAdjusterManager;

    @Mock
    protected DcMetaCache dcMetaCache;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    private String clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupPeerMasterStateManagerTest() throws Exception {
        defaultPeerMasterAdjusterManager.initialize();
    }

    @Test
    public void testDcMetaAdded() {
        ClusterMeta clusterMeta = mockClusterMeta();
        defaultPeerMasterAdjusterManager.update(new NodeAdded<>(clusterMeta), defaultPeerMasterAdjusterManager);
        Mockito.verify(currentMetaManager, Mockito.times(1)).addResource(Mockito.anyString(),
                Mockito.anyString(), Mockito.any(DefaultPeerMasterStateAdjuster.class));
    }

    private ClusterMeta mockClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        clusterMeta.setType(ClusterType.BI_DIRECTION.toString());
        clusterMeta.addShard(shardMeta);

        return clusterMeta;
    }

}
