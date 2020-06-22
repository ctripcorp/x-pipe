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
    public void currentMasterChanged(String clusterId, String shardId) {
        peerMasterAdjustAction.adjustPeerMaster(clusterId, shardId);
    }

    @Override
    public void peerMasterChanged(String dcId, String clusterId, String shardId) {
        peerMasterAdjustAction.adjustPeerMaster(clusterId, shardId);
    }

}
