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

    protected KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterAdjustExecutor;

    private MetaServerConfig config;

    private int adjustIntervalSeconds;

    public DefaultPeerMasterStateAdjuster(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                          MetaServerConfig config, PeerMasterAdjustJobFactory peerMasterAdjustJobFactory,
                                          KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterAdjustExecutor,
                                          ScheduledExecutorService scheduled) {
        this(clusterId, shardId, dcMetaCache, currentMetaManager, config, peerMasterAdjustJobFactory, peerMasterAdjustExecutor,
                scheduled, DEFAULT_PEER_MASTER_ADJUST_INTERVAL_SECONDS);
    }

    public DefaultPeerMasterStateAdjuster(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                          MetaServerConfig config, PeerMasterAdjustJobFactory peerMasterAdjustJobFactory,
                                          KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterAdjustExecutor,
                                          ScheduledExecutorService scheduled, int adjustIntervalSeconds) {
        super(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled);
        this.peerMasterAdjustJobFactory = peerMasterAdjustJobFactory;
        this.peerMasterAdjustExecutor = peerMasterAdjustExecutor;
        this.adjustIntervalSeconds = adjustIntervalSeconds;
        this.config = config;
    }

    @Override
    protected void work() {
        if (!config.shouldCorrectPeerMasterPeriodically()) return;

        PeerMasterAdjustJob adjustJob = peerMasterAdjustJobFactory.buildPeerMasterAdjustJob(clusterId, shardId);
        if (null != adjustJob) peerMasterAdjustExecutor.execute(Pair.of(clusterId, shardId), adjustJob);
    }

    @Override
    protected int getWorkIntervalSeconds() {
        return adjustIntervalSeconds;
    }

}
