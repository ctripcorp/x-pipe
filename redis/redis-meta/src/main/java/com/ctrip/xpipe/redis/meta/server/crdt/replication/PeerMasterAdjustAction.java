package com.ctrip.xpipe.redis.meta.server.crdt.replication;

public interface PeerMasterAdjustAction {

    void adjustPeerMaster(Long clusterDbId, Long shardDbId);

}
