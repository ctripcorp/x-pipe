package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterStateAdjuster;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.AbstractClusterShardPeriodicTask;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class DefaultPeerMasterStateAdjuster extends AbstractClusterShardPeriodicTask implements PeerMasterStateAdjuster {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final int DEFAULT_PEER_MASTER_ADJUST_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("PEER_MASTER_ADJUST_INTERVAL_SECONDS", "60"));

    protected PeerMasterAdjustJobFactory peerMasterAdjustJobFactory;

    protected KeyedOneThreadTaskExecutor<Pair<Long, Long> > peerMasterAdjustExecutor;

    private MetaServerConfig config;

    private int adjustIntervalSeconds;

    public DefaultPeerMasterStateAdjuster(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                          MetaServerConfig config, PeerMasterAdjustJobFactory peerMasterAdjustJobFactory,
                                          KeyedOneThreadTaskExecutor<Pair<Long, Long> > peerMasterAdjustExecutor,
                                          ScheduledExecutorService scheduled) {
        this(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, config, peerMasterAdjustJobFactory, peerMasterAdjustExecutor,
                scheduled, DEFAULT_PEER_MASTER_ADJUST_INTERVAL_SECONDS);
    }

    public DefaultPeerMasterStateAdjuster(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                          MetaServerConfig config, PeerMasterAdjustJobFactory peerMasterAdjustJobFactory,
                                          KeyedOneThreadTaskExecutor<Pair<Long, Long> > peerMasterAdjustExecutor,
                                          ScheduledExecutorService scheduled, int adjustIntervalSeconds) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
        this.peerMasterAdjustJobFactory = peerMasterAdjustJobFactory;
        this.peerMasterAdjustExecutor = peerMasterAdjustExecutor;
        this.adjustIntervalSeconds = adjustIntervalSeconds;
        this.config = config;
    }

    @Override
    protected void work() {
        if (!config.shouldCorrectPeerMasterPeriodically()) return;

        PeerMasterAdjustJob adjustJob = peerMasterAdjustJobFactory.buildPeerMasterAdjustJob(clusterDbId, shardDbId);
        if (null != adjustJob) peerMasterAdjustExecutor.execute(Pair.of(clusterDbId, shardDbId), adjustJob);
    }

    @Override
    protected int getWorkIntervalSeconds() {
        return adjustIntervalSeconds;
    }

}
