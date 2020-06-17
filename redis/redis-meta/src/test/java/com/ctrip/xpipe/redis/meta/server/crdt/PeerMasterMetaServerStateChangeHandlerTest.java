package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.manage.PeerMasterAdjusterManager;
import com.ctrip.xpipe.redis.meta.server.crdt.manage.PeerMasterStateAdjuster;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooser;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooserManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.Executor;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterMetaServerStateChangeHandlerTest extends AbstractMetaServerTest {

    @InjectMocks
    private PeerMasterMetaServerStateChangeHandler peerMasterMetaServerStateChangeHandler;

    @Mock
    private PeerMasterChooserManager peerMasterChooserManager;

    @Mock
    private PeerMasterAdjusterManager peerMasterAdjusterManager;

    @Mock
    private Executor executors;

    @Mock
    PeerMasterChooser chooser;

    @Mock
    PeerMasterChooseCommand command;

    @Mock
    PeerMasterStateAdjuster adjuster;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupPeerMasterMetaServerStateChangeHandlerTest() {
        Mockito.when(peerMasterChooserManager.getChooser(clusterId, shardId)).thenReturn(chooser);
        Mockito.when(peerMasterAdjusterManager.getAdjuster(clusterId, shardId)).thenReturn(adjuster);
        Mockito.when(chooser.createMasterChooserCommand(dcId)).thenReturn(command);
    }

    @Test
    public void testUpstreamPeerMasterChange() {
        peerMasterMetaServerStateChangeHandler.upstreamPeerMasterChange(dcId, clusterId, shardId);
        Mockito.verify(peerMasterChooserManager, Mockito.times(1)).getChooser(clusterId, shardId);
        Mockito.verify(chooser, Mockito.times(1)).createMasterChooserCommand(dcId);
        Mockito.verify(command, Mockito.times(1)).execute(Mockito.any());
    }

    @Test
    public void testPeerMasterChanged() {
        peerMasterMetaServerStateChangeHandler.peerMasterChanged(dcId, clusterId, shardId);
        Mockito.verify(peerMasterAdjusterManager, Mockito.times(1)).getAdjuster(clusterId, shardId);
        Mockito.verify(adjuster, Mockito.times(1)).adjust();
    }

}
