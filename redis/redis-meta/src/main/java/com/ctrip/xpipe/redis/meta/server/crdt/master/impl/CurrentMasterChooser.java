package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.AbstractClusterShardPeriodicTask;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class CurrentMasterChooser extends AbstractClusterShardPeriodicTask {

    protected KeyedOneThreadTaskExecutor<Pair<Long, Long>> peerMasterChooseExecutor;

    protected Executor executors;

    protected MasterChooseCommandFactory masterChooseCommandFactory;

    private int checkIntervalSeconds;

    public static final int DEFAULT_CURRENT_MASTER_CHECK_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("CURRENT_MASTER_CHECK_INTERVAL_SECONDS", "5"));

    public CurrentMasterChooser(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                MasterChooseCommandFactory factory, Executor executors,
                                KeyedOneThreadTaskExecutor<Pair<Long, Long>> peerMasterChooseExecutor,
                                ScheduledExecutorService scheduled) {
        this(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, factory, executors, peerMasterChooseExecutor, scheduled, DEFAULT_CURRENT_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public CurrentMasterChooser(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                CurrentMetaManager currentMetaManager, MasterChooseCommandFactory factory, Executor executors,
                                KeyedOneThreadTaskExecutor<Pair<Long, Long> > peerMasterChooseExecutor,
                                ScheduledExecutorService scheduled, int checkIntervalSeconds) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
        this.masterChooseCommandFactory = factory;
        this.peerMasterChooseExecutor = peerMasterChooseExecutor;
        this.executors = executors;
        this.checkIntervalSeconds = checkIntervalSeconds;
    }

    @Override
    protected void work() {
        MasterChooseCommand chooseCommand = masterChooseCommandFactory.buildCurrentMasterChooserCommand(clusterDbId, shardDbId);
        peerMasterChooseExecutor.execute(Pair.of(clusterDbId, shardDbId), chooseCommand);
    }

    @Override
    protected int getWorkIntervalSeconds() {
        return checkIntervalSeconds;
    }

}
