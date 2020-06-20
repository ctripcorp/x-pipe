package com.ctrip.xpipe.redis.meta.server.crdt.replication;

public interface PeerMasterAdjustAction {

    void adjustPeerMaster(String clusterId, String shardId);

}
