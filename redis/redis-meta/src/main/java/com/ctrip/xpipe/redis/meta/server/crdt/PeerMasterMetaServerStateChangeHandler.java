package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustAction;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.PEER_MASTER_CHOOSE_EXECUTOR;

@Component
public class PeerMasterMetaServerStateChangeHandler implements MetaServerStateChangeHandler {

    @Resource(name = PEER_MASTER_CHOOSE_EXECUTOR)
    KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterChooseExecutor;

    @Autowired
    private PeerMasterAdjustAction peerMasterAdjustAction;

    @Override
    public void currentMasterChanged(String clusterId, String shardId) {
        peerMasterAdjustAction.adjustPeerMaster(clusterId, shardId);
    }

    @Override
    public void peerMasterChanged(String dcId, String clusterId, String shardId) {
        peerMasterAdjustAction.adjustPeerMaster(clusterId, shardId);
    }

}
