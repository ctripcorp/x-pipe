package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustAction;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.PEER_MASTER_ADJUST_EXECUTOR;

@Component
public class DefaultPeerMasterAdjustAction implements PeerMasterAdjustAction {

    private PeerMasterAdjustJobFactory adjustJobFactory;

    private KeyedOneThreadTaskExecutor<Pair<Long, Long> > peerMasterAdjustExecutors;

    @Autowired
    public DefaultPeerMasterAdjustAction(PeerMasterAdjustJobFactory adjustJobFactory,
                                         @Qualifier(PEER_MASTER_ADJUST_EXECUTOR) KeyedOneThreadTaskExecutor<Pair<Long, Long> > peerMasterAdjustExecutors) {
        this.adjustJobFactory = adjustJobFactory;
        this.peerMasterAdjustExecutors = peerMasterAdjustExecutors;
    }

    @Override
    public void adjustPeerMaster(Long clusterDbId, Long shardDbId) {
        PeerMasterAdjustJob adjustJob = adjustJobFactory.buildPeerMasterAdjustJob(clusterDbId, shardDbId);
        if (null != adjustJob) peerMasterAdjustExecutors.execute(Pair.of(clusterDbId, shardDbId), adjustJob);
    }

}
