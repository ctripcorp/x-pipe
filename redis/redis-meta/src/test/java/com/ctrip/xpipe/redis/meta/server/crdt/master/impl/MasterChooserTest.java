package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MasterChooserTest extends AbstractMetaServerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    private Long clusterDbId = 1L, shardDbId = 1L;

    private String upstreamDcId = "dc2";

    private String dcs = "dc1, dc2";

    private int checkIntervalSeconds = 1;

    @Mock
    private MasterChooseCommandFactory factory;

    @Mock
    private MasterChooseCommand command;

    @Mock
    private KeyedOneThreadTaskExecutor<Pair<Long, Long> > keyedOneThreadTaskExecutor;

    private CurrentMasterChooser currentMasterChooser;

    private PeerMasterChooser peerMasterChooser;

    @Before
    public void setupDefaultPeerMasterChooserTest() throws Exception {
        currentMasterChooser = new CurrentMasterChooser(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, factory,
                executors, keyedOneThreadTaskExecutor,  scheduled, checkIntervalSeconds);
        peerMasterChooser = new PeerMasterChooser(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, factory,
                 executors, keyedOneThreadTaskExecutor,  scheduled, checkIntervalSeconds);

        Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(dcId);
        Mockito.when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(mockClusterMeta());
    }

    @Test
    public void testPeerMasterChooseWork() {
        Mockito.when(factory.buildPeerMasterChooserCommand(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong())).thenReturn(command);
        Mockito.doAnswer(invocation -> {
            Pair<String, String> key = invocation.getArgument(0, Pair.class);
            ParallelCommandChain commandChain = invocation.getArgument(1, ParallelCommandChain.class);
            Assert.assertEquals(Pair.of(clusterDbId, shardDbId), key);
            Assert.assertNotNull(commandChain);

            return null;
        }).when(keyedOneThreadTaskExecutor).execute(Mockito.any(), Mockito.any());

        peerMasterChooser.work();
        Mockito.verify(factory, Mockito.times(1)).buildPeerMasterChooserCommand(upstreamDcId, clusterDbId, shardDbId);
        Mockito.verify(keyedOneThreadTaskExecutor, Mockito.times(1)).execute(Mockito.any(), Mockito.any());
    }

    @Test
    public void testCurrentMasterChooseWork() {
        Mockito.when(factory.buildCurrentMasterChooserCommand(Mockito.anyLong(), Mockito.anyLong())).thenReturn(command);
        Mockito.doAnswer(invocation -> {
            Pair<String, String> key = invocation.getArgument(0, Pair.class);
            MasterChooseCommand paramCommand = invocation.getArgument(1, MasterChooseCommand.class);
            Assert.assertEquals(Pair.of(clusterDbId, shardDbId), key);
            Assert.assertEquals(command, paramCommand);

            return null;
        }).when(keyedOneThreadTaskExecutor).execute(Mockito.any(), Mockito.any());

        currentMasterChooser.work();
        Mockito.verify(factory, Mockito.times(1)).buildCurrentMasterChooserCommand(clusterDbId, shardDbId);
        Mockito.verify(keyedOneThreadTaskExecutor, Mockito.times(1)).execute(Mockito.any(), Mockito.any());
    }

    private ClusterMeta mockClusterMeta() {
        return new ClusterMeta(clusterId).addShard(new ShardMeta(shardId)).setDcs(dcs);
    }

}
