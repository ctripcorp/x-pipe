package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.PeerMasterChooseAction;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaServerRefreshPeerMasterTest extends AbstractMetaServerTest {

    @InjectMocks
    private DefaultMetaServer metaServer;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private PeerMasterChooseAction peerMasterChooseAction;

    @Before
    public void setupDefaultMetaServerRefreshPeerMasterTest() {
        Mockito.when(dcMetaCache.getClusterMeta(getClusterDbId())).thenReturn(new ClusterMeta(getClusterId()).setType(ClusterType.BI_DIRECTION.toString()));
        Mockito.when(dcMetaCache.clusterShardId2DbId(getClusterId(), getShardId())).thenReturn(Pair.from(getClusterDbId(), getShardDbId()));
    }

    @Test
    public void testUpstreamPeerChange() {
        metaServer.upstreamPeerChange("remote-dc", getClusterId(), getShardId(), null);
        Mockito.verify(peerMasterChooseAction, Mockito.times(1)).choosePeerMaster("remote-dc", getClusterDbId(), getShardDbId());
    }

}
