package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.crdt.AbstractCurrentPeerMasterMetaObserver;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterStateManager;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterStateAdjuster;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.PEER_MASTER_ADJUST_EXECUTOR;

@Component
public class DefaultPeerMasterStateManager extends AbstractCurrentPeerMasterMetaObserver implements PeerMasterStateManager {

    @Autowired
    protected DcMetaCache dcMetaCache;

    @Autowired
    PeerMasterAdjustJobFactory peerMasterAdjustJobFactory;

    @Autowired
    MetaServerConfig metaServerConfig;

    @Resource(name = PEER_MASTER_ADJUST_EXECUTOR)
    KeyedOneThreadTaskExecutor<Pair<String, String>> peerMasterAdjustExecutor;

    protected ScheduledExecutorService scheduled;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 2), XpipeThreadFactory.create("PeerMasterAdjusterSchedule"));
    }

    @Override
    protected void addShard(String clusterId, String shardId) {
        try {
            PeerMasterStateAdjuster adjuster = new DefaultPeerMasterStateAdjuster(clusterId, shardId, dcMetaCache,
                    currentMetaManager, metaServerConfig, peerMasterAdjustJobFactory, peerMasterAdjustExecutor, scheduled);
            logger.info("[addShard]{}, {}, {}", clusterId, shardId, adjuster);
            adjuster.start();
            //release resources
            registerJob(clusterId, shardId, adjuster);
        } catch (Exception e) {
            logger.error("[addShard]{}, {}", clusterId, shardId, e);
        }
    }
}
