package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterAdjustActionTest extends AbstractMetaServerTest {

    @Mock
    private PeerMasterAdjustJobFactory adjustJobFactory;

    @Mock
    private KeyedOneThreadTaskExecutor<Pair<String, String>> peerMasterAdjustExecutors;

    @Mock
    private PeerMasterAdjustJob adjustJob;

    private DefaultPeerMasterAdjustAction action;

    @Before
    public void setupPeerMasterAdjustActionTest() {
        action = new DefaultPeerMasterAdjustAction(adjustJobFactory, peerMasterAdjustExecutors);
        Mockito.when(adjustJobFactory.buildPeerMasterAdjustJob(getClusterId(), getShardId())).thenReturn(adjustJob);
    }

    @Test
    public void testAdjustPeerMaster() {
        action.adjustPeerMaster(getClusterId(), getShardId());
        Mockito.verify(adjustJobFactory, Mockito.times(1)).buildPeerMasterAdjustJob(getClusterId(), getShardId());
        Mockito.verify(peerMasterAdjustExecutors, Mockito.times(1)).execute(Pair.of(getClusterId(), getShardId()), adjustJob);
    }

}
