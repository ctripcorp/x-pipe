package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
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
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterStateAdjusterTest extends AbstractMetaServerTest {

    private DefaultPeerMasterStateAdjuster adjuster;

    private String clusterId = "cluster1", shardId = "shardId";

    private Long clusterDbId = 1L, shardDbId = 1L;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private PeerMasterAdjustJobFactory peerMasterAdjustJobFactory;

    @Mock
    private KeyedOneThreadTaskExecutor<com.ctrip.xpipe.tuple.Pair<Long, Long>> peerMasterAdjustExecutor;

    @Mock
    private PeerMasterAdjustJob job;

    @Mock
    private MetaServerConfig metaServerConfig;

    @Before
    public void setupDefaultPeerMasterStateAdjusterTest() throws Exception {
        adjuster = new DefaultPeerMasterStateAdjuster(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, metaServerConfig,
                peerMasterAdjustJobFactory, peerMasterAdjustExecutor, scheduled);

        Mockito.when(peerMasterAdjustJobFactory.buildPeerMasterAdjustJob(clusterDbId, shardDbId)).thenReturn(job);
        Mockito.when(metaServerConfig.shouldCorrectPeerMasterPeriodically()).thenReturn(true);
    }

    @Test
    public void testForAdjust() {
        adjuster.work();
        Mockito.verify(peerMasterAdjustJobFactory, Mockito.times(1)).buildPeerMasterAdjustJob(clusterDbId, shardDbId);
        Mockito.verify(peerMasterAdjustExecutor, Mockito.times(1)).execute(Pair.of(clusterDbId, shardDbId), job);
    }

    @Test
    public void testForNotDoAdjust() {
        Mockito.when(metaServerConfig.shouldCorrectPeerMasterPeriodically()).thenReturn(false);
        adjuster.work();
        Mockito.verify(peerMasterAdjustJobFactory, Mockito.never()).buildPeerMasterAdjustJob(clusterDbId, shardDbId);
        Mockito.verify(peerMasterAdjustExecutor, Mockito.never()).execute(Pair.of(clusterDbId, shardDbId), job);
    }

}
