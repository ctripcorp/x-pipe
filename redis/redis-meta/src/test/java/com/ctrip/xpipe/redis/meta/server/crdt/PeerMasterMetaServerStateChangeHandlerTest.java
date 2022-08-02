package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustAction;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterMetaServerStateChangeHandlerTest extends AbstractMetaServerTest {

    @InjectMocks
    private PeerMasterMetaServerStateChangeHandler peerMasterMetaServerStateChangeHandler;

    @Mock
    KeyedOneThreadTaskExecutor<Pair<String, String>> peerMasterChooseExecutor;

    @Mock
    private PeerMasterAdjustAction peerMasterAdjustAction;

    private String dcId = "dc1";
    private Long clusterDbId = 1L, shardDbId = 1L;

    @Test
    public void testUpstreamPeerMasterChange() {
        peerMasterMetaServerStateChangeHandler.currentMasterChanged(clusterDbId, shardDbId);
        Mockito.verify(peerMasterAdjustAction, Mockito.times(1)).adjustPeerMaster(clusterDbId, shardDbId);
    }

    @Test
    public void testPeerMasterChanged() {
        peerMasterMetaServerStateChangeHandler.peerMasterChanged(clusterDbId, shardDbId);
        Mockito.verify(peerMasterAdjustAction, Mockito.times(1)).adjustPeerMaster(clusterDbId, shardDbId);
    }

}
