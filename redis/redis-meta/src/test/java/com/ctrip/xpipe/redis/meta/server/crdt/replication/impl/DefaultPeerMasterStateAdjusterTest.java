package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterStateAdjusterTest extends AbstractMetaServerTest {

    private DefaultPeerMasterStateAdjuster adjuster;

    private String clusterId = "cluster1", shardId = "shardId";

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private PeerMasterAdjustJobFactory peerMasterAdjustJobFactory;

    @Mock
    private KeyedOneThreadTaskExecutor<com.ctrip.xpipe.tuple.Pair<String, String>> peerMasterAdjustExecutor;

    @Mock
    private PeerMasterAdjustJob job;

    @Before
    public void setupDefaultPeerMasterStateAdjusterTest() throws Exception {
        adjuster = new DefaultPeerMasterStateAdjuster(clusterId, shardId, dcMetaCache, currentMetaManager, peerMasterAdjustJobFactory,
                peerMasterAdjustExecutor, scheduled);

        Mockito.when(peerMasterAdjustJobFactory.buildPeerMasterAdjustJob(clusterId, shardId)).thenReturn(job);
    }

    @Test
    public void testForAdjust() {
        adjuster.work();
        Mockito.verify(peerMasterAdjustJobFactory, Mockito.times(1)).buildPeerMasterAdjustJob(clusterId, shardId);
        Mockito.verify(peerMasterAdjustExecutor, Mockito.times(1)).execute(Pair.of(clusterId, shardId), job);
    }

}
