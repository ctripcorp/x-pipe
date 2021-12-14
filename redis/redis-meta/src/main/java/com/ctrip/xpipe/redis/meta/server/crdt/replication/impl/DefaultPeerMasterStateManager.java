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
    KeyedOneThreadTaskExecutor<Pair<Long, Long>> peerMasterAdjustExecutor;

    protected ScheduledExecutorService scheduled;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 2), XpipeThreadFactory.create("PeerMasterAdjusterSchedule"));
    }

    @Override
    protected void addShard(Long clusterDbId, Long shardDbId) {
        try {
            PeerMasterStateAdjuster adjuster = new DefaultPeerMasterStateAdjuster(clusterDbId, shardDbId, dcMetaCache,
                    currentMetaManager, metaServerConfig, peerMasterAdjustJobFactory, peerMasterAdjustExecutor, scheduled);
            logger.info("[addShard]{}, {}, {}", clusterDbId, shardDbId, adjuster);
            adjuster.start();
            //release resources
            registerJob(clusterDbId, shardDbId, adjuster);
        } catch (Exception e) {
            logger.error("[addShard]{}, {}", clusterDbId, shardDbId, e);
        }
    }
}
