package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.MasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.*;

public class PeerMasterChooser extends CurrentMasterChooser implements MasterChooser {

    public static final int DEFAULT_PEER_MASTER_CHECK_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("PEER_MASTER_CHECK_INTERVAL_SECONDS", "10"));

    public PeerMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                             MasterChooseCommandFactory factory, Executor executors,
                             KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterChooseExecutor,
                             ScheduledExecutorService scheduled) {
        this(clusterId, shardId, dcMetaCache, currentMetaManager, factory, executors, peerMasterChooseExecutor, scheduled, DEFAULT_PEER_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public PeerMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache,
                             CurrentMetaManager currentMetaManager, MasterChooseCommandFactory factory, Executor executors,
                             KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterChooseExecutor,
                             ScheduledExecutorService scheduled, int checkIntervalSeconds) {
        super(clusterId, shardId, dcMetaCache, currentMetaManager, factory, executors, peerMasterChooseExecutor, scheduled, checkIntervalSeconds);
    }

    @Override
    protected void work() {
        ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterId);

        ParallelCommandChain parallelCommandChain = new ParallelCommandChain(executors);
        String currentDc = dcMetaCache.getCurrentDc();

        for (String dcId : clusterMeta.getDcs().split("\\s*,\\s*")) {
            if (currentDc.equalsIgnoreCase(dcId)) continue;
            parallelCommandChain.add(masterChooseCommandFactory.buildPeerMasterChooserCommand(dcId, clusterId, shardId));
        }

        peerMasterChooseExecutor.execute(Pair.of(clusterId, shardId), parallelCommandChain);
    }

}
