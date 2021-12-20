package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PeerMasterMetaServerStateChangeHandler implements MetaServerStateChangeHandler {

    @Autowired
    private PeerMasterAdjustAction peerMasterAdjustAction;

    @Override
    public void currentMasterChanged(Long clusterDbId, Long shardDbId) {
        peerMasterAdjustAction.adjustPeerMaster(clusterDbId, shardDbId);
    }

    @Override
    public void peerMasterChanged(String dcId, Long clusterDbId, Long shardDbId) {
        peerMasterAdjustAction.adjustPeerMaster(clusterDbId, shardDbId);
    }

}
